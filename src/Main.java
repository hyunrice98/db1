import java.sql.*;

public class Main {
    static Statement stmt;
    static ResultSet rs;

    static void printDoubleLine() {
        System.out.println("\n======================================================================" +
                "======================================================================");
    }

    static void printLine(int colNum) {
        for (int i = 0; i < colNum; i++) {
            System.out.print("----------------------------------");
        }
        System.out.println();
    }

    static void printTable(String tableName) {
        System.out.println("table: " + tableName);
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int colNum = rsmd.getColumnCount();
            printLine(colNum);

            for (int i = 1; i <= colNum; i++) {
                System.out.print(String.format("%32s", rsmd.getColumnName(i)) + " |");
            }
            System.out.println();
            printLine(colNum);

            while (rs.next()) {
                for (int i = 1; i <= colNum; i++) {
                    System.out.print(String.format("%32s", rs.getString(i)) + " |");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("Where is your PostgreSQL JDBC Driver? Include in your library path!");
            e.printStackTrace();
            return;
        }

        Connection connection;
        try {
            connection = DriverManager.getConnection(
                    "jdbc:postgresql://127.0.0.1:5432/project_movie", "postgres", "cse3207"
            );
        } catch (SQLException e) {
            System.out.println("Connection Failed. Check output console");
            e.printStackTrace();
            return;
        }
        if (connection != null) {
            System.out.println(connection);
            System.out.println("You made it, take control your database now!");
        } else {
            System.out.println("Failed to make connection");
        }

        assert connection != null;
        stmt = connection.createStatement();

        // TODO: Create table
        try {
            printDoubleLine();
            System.out.println("#### CREATING TABLES ####");
            stmt.execute(
                    "CREATE TABLE \"director\" (" +
                            "\"directorID\" SERIAL PRIMARY KEY," +
                            "\"directorName\" VARCHAR(40)," +
                            "\"dateOfBirth\" DATE," +
                            "\"dateOfDeath\" DATE" +
                            ");"
            );
            System.out.println("Created table 'director'");
            stmt.execute(
                    "CREATE TABLE \"actor\" (" +
                            "\"actorID\" SERIAL PRIMARY KEY," +
                            "\"actorName\" VARCHAR(40)," +
                            "\"dateOfBirth\" DATE," +
                            "\"dateOfDeath\" DATE," +
                            "\"gender\" VARCHAR(10)" +
                            ");"
            );
            System.out.println("Created table 'actor'");
            stmt.execute(
                    "CREATE TABLE \"movie\" (" +
                            "\"movieID\" SERIAL PRIMARY KEY," +
                            "\"movieName\" VARCHAR(40)," +
                            "\"releaseYear\" INT," +
                            "\"releaseMonth\" INT," +
                            "\"releaseDate\" INT," +
                            "\"publisherName\" VARCHAR(40)," +
                            "\"avgRate\" FLOAT" +
                            ");"
            );
            System.out.println("Created table 'movie'");
            stmt.execute(
                    "CREATE TABLE \"award\" (" +
                            "\"awardID\" SERIAL PRIMARY KEY," +
                            "\"awardName\" VARCHAR(40)" +
                            ");"
            );
            System.out.println("Created table 'award'");
            stmt.execute(
                    "CREATE TABLE \"customer\" (" +
                            "\"customerID\" SERIAL PRIMARY KEY," +
                            "\"customerName\" VARCHAR(40)," +
                            "\"dateOfBirth\" DATE," +
                            "\"gender\" VARCHAR(10)" +
                            ");"
            );
            System.out.println("Created table 'customer'");
            stmt.execute("CREATE TABLE \"genre\" (\"genreName\" VARCHAR(40) UNIQUE PRIMARY KEY);");
            System.out.println("Created table 'genre'");
            stmt.execute(
                    "CREATE TABLE \"movieGenre\" (" +
                            "\"movieID\" INT," +
                            "\"genreName\" VARCHAR(40)," +
                            "PRIMARY KEY (\"movieID\", \"genreName\")," +
                            "FOREIGN KEY (\"movieID\") REFERENCES movie (\"movieID\") ON DELETE CASCADE," +
                            "FOREIGN KEY (\"genreName\") REFERENCES genre (\"genreName\") ON DELETE CASCADE" +
                            ");"
            );
            System.out.println("Created table 'movieGenre'");
            stmt.execute(
                    "CREATE TABLE \"movieObtain\" (" +
                            "\"movieID\" INT," +
                            "\"awardID\" INT," +
                            "\"year\" INT," +
                            "PRIMARY KEY (\"movieID\", \"awardID\")," +
                            "FOREIGN KEY (\"movieID\") REFERENCES movie(\"movieID\") ON DELETE CASCADE," +
                            "FOREIGN KEY (\"awardID\") REFERENCES award(\"awardID\") ON DELETE CASCADE" +
                            ");"
            );
            System.out.println("Created table 'movieObtain'");
            stmt.execute(
                    "CREATE TABLE \"actorObtain\" (" +
                            "\"actorID\" INT," +
                            "\"awardID\" INT," +
                            "year INT," +
                            "PRIMARY KEY (\"actorID\", \"awardID\")," +
                            "FOREIGN KEY (\"actorID\") REFERENCES actor(\"actorID\") ON DELETE CASCADE," +
                            "FOREIGN KEY (\"awardID\") REFERENCES award(\"awardID\") ON DELETE CASCADE" +
                            ");"
            );
            System.out.println("Created table 'actorObtain'");
            stmt.execute(
                    "CREATE TABLE \"directorObtain\" (" +
                            "\"directorID\" INT," +
                            "\"awardID\" INT," +
                            "year INT," +
                            "PRIMARY KEY (\"directorID\", \"awardID\")," +
                            "FOREIGN KEY (\"directorID\") REFERENCES director(\"directorID\") ON DELETE CASCADE," +
                            "FOREIGN KEY (\"awardID\") REFERENCES award(\"awardID\") ON DELETE CASCADE" +
                            ");"
            );
            System.out.println("Created table 'directorObtain'");
            stmt.execute(
                    "CREATE TABLE \"casting\" (" +
                            "\"movieID\" INT," +
                            "\"actorID\" INT," +
                            "\"role\" VARCHAR(30)," +
                            "PRIMARY KEY (\"movieID\", \"actorID\")," +
                            "FOREIGN KEY (\"movieID\") REFERENCES movie(\"movieID\") ON DELETE CASCADE" +
                            ");"
            );
            System.out.println("Created table 'casting'");
            stmt.execute(
                    "CREATE TABLE \"make\" (" +
                            "\"movieID\" INT," +
                            "\"directorID\" INT," +
                            "PRIMARY KEY (\"movieID\", \"directorID\")," +
                            "FOREIGN KEY (\"movieID\") REFERENCES movie(\"movieID\") ON DELETE CASCADE," +
                            "FOREIGN KEY (\"directorID\") REFERENCES director(\"directorID\") ON DELETE CASCADE" +
                            ");"
            );
            System.out.println("Created table 'make'");
            stmt.execute(
                    "CREATE TABLE \"customerRate\" (" +
                            "\"customerID\" INT," +
                            "\"movieID\" INT," +
                            "\"rate\" FLOAT," +
                            "PRIMARY KEY (\"customerID\", \"movieID\")," +
                            "FOREIGN KEY (\"customerID\") REFERENCES customer(\"customerID\") ON DELETE CASCADE," +
                            "FOREIGN KEY (\"movieID\") REFERENCES movie(\"movieID\") ON DELETE CASCADE" +
                            ");"
            );
            System.out.println("Created table 'customerRate'");
        } catch (SQLException e) {
            System.out.println("Table Creation Error: " + e);
        }

        // TODO: Initial data input
        try {
            printDoubleLine();
            System.out.println("#### INITIAL DATA INPUT ####");
            stmt.execute(
                    "INSERT INTO movie (\"movieName\", \"publisherName\", \"releaseYear\", \"releaseMonth\", \"releaseDate\") VALUES\n" +
                            "('Minari', 'A24', 2020, 12, 11),\n" +
                            "('Edward Scissorhands', '20th Century Fox Presents', 1991, 6, 29),\n" +
                            "('Alice In Wonderland', 'Korea Sony Pictures', 2010, 3, 4),\n" +
                            "('The Social Network', 'Korea Sony Pictures', 2010, 11, 18),\n" +
                            "('The Dark Knight', 'Warner Brothers Korea', 2008, 8, 6);"
            );
            System.out.println("Initialized table 'movie'");
            stmt.execute(
                    "INSERT INTO director (\"directorName\", \"dateOfBirth\", \"dateOfDeath\") VALUES\n" +
                            "('Lee Isaac Chung', '1978-10-19', NULL),\n" +
                            "('Tim Burton', '1958-8-25', NULL),\n" +
                            "('David Fincher', '1962-8-28', NULL),\n" +
                            "('Christoper Nolan', '1970-7-30', NULL);"
            );
            System.out.println("Initialized table 'director'");
            stmt.execute(
                    "INSERT INTO actor (\"actorName\", \"dateOfBirth\", \"dateOfDeath\", \"gender\") VALUES\n" +
                            "('Steven Yeun', '1983-12-21', NULL, 'Male'),\n" +
                            "('Youn Yuhjung', '1947-6-19', NULL, 'Female'),\n" +
                            "('Johnny Depp', '1963-6-9', NULL, 'Male'),\n" +
                            "('Winona Ryder', '1971-10-29', NULL, 'Female'),\n" +
                            "('Anne Hathaway', '1982-11-12', NULL, 'Female'),\n" +
                            "('Christian Bale', '1974-1-30', NULL, 'Male'),\n" +
                            "('Heath Ledger', '1979-4-4', '2008-1-22', 'Male'),\n" +
                            "('Jesse Eisenberg', '1983-10-5', NULL, 'Male'),\n" +
                            "('Andrew Garfield', '1983-8-20', NULL, 'Male');"
            );
            System.out.println("Initialized table 'actor'");
            stmt.execute(
                    "INSERT INTO customer (\"customerName\", \"dateOfBirth\", \"gender\") VALUES\n" +
                            "('Bob', '1997-11-14', 'Male'),\n" +
                            "('John', '1978-01-23', 'Male'),\n" +
                            "('Jack', '1980-05-04', 'Male'),\n" +
                            "('Jill', '1981-04-17', 'Female'),\n" +
                            "('Bell', '1990-05-14', 'Female');"
            );
            System.out.println("Initialized table 'customer'");
            stmt.execute(
                    "INSERT INTO genre (\"genreName\") VALUES" +
                            " ('Drama'), ('Fantasy'), ('Romance'), ('Adventure')," +
                            " ('Family'), ('Action'), ('Mystery'), ('Thriller');\n"
            );
            System.out.println("Initialized table 'genre'");

            stmt.execute(
                    "INSERT INTO \"movieGenre\" (\"movieID\", \"genreName\") VALUES" +
                            "(1, 'Drama'), " +
                            "(2, 'Fantasy'), " +
                            "(2, 'Romance'), " +
                            "(3, 'Fantasy')," +
                            "(3, 'Adventure')," +
                            "(3, 'Family')," +
                            "(4, 'Drama')," +
                            "(5, 'Action')," +
                            "(5, 'Drama')," +
                            "(5, 'Mystery')," +
                            "(5, 'Thriller');"
            );
            System.out.println("Initialized table 'movieGenre'");
            stmt.execute(
                    "INSERT INTO \"casting\" (\"movieID\", \"actorID\", \"role\") VALUES" +
                            "(1, 1, 'Main actor')," +
                            "(1, 2, 'Supporting Actor')," +
                            "(2, 3, 'Main actor')," +
                            "(2, 4, 'Main actor')," +
                            "(3, 3, 'Main actor')," +
                            "(3, 5, 'Main actor')," +
                            "(4, 8, 'Main actor')," +
                            "(4, 9, 'Supporting Actor')," +
                            "(5, 6, 'Main actor')," +
                            "(5, 7, 'Main actor');"
            );
            System.out.println("Initialized table 'casting'");
            stmt.execute(
                    "INSERT INTO \"make\" (\"movieID\", \"directorID\") VALUES" +
                            "(1, 1)," +
                            "(2, 2)," +
                            "(3, 2)," +
                            "(4, 3)," +
                            "(5, 4);"
            );
            System.out.println("Initialized table 'make'");
        } catch (SQLException e) {
            System.out.println("##### Initial Data Input Error #####");
            e.printStackTrace();
        }

        // TODO: Insert award data (actor/director/movie)
        try {
            printDoubleLine();
            System.out.println("#### INSERT AWARD DATA ####");
            System.out.println("\nStatement 2.1) Winona Ryder won the “Best supporting actor” award in 1994");
            System.out.println("Translated SQL: INSERT INTO award (\"awardName\") VALUES ('Best supporting actor');");
            stmt.execute("INSERT INTO award (\"awardName\") VALUES ('Best supporting actor');");
            System.out.println("                INSERT INTO \"actorObtain\" (\"actorID\", \"awardID\", \"year\") VALUES (4, 1, 1994)");
            stmt.execute("INSERT INTO \"actorObtain\" (\"actorID\", \"awardID\", \"year\") VALUES (4, 1, 1994)");
            rs = stmt.executeQuery("SELECT * FROM award");
            printTable("award");
            rs = stmt.executeQuery("SELECT * FROM \"actorObtain\"");
            printTable("actorObtain");

            System.out.println("\nStatement 2.2) Andrew Garfield won the “Best supporting actor” award in 2011");
            System.out.println("Translated SQL: INSERT INTO \"actorObtain\" (\"actorID\", \"awardID\", \"year\") VALUES (9, 1, 2011)");
            stmt.execute("INSERT INTO \"actorObtain\" (\"actorID\", \"awardID\", \"year\") VALUES (9, 1, 2011)");
            rs = stmt.executeQuery("SELECT * FROM award");
            printTable("award");
            rs = stmt.executeQuery("SELECT * FROM \"actorObtain\"");
            printTable("actorObtain");

            System.out.println("\nStatement 2.3) Jesse Eisenberg won the “Best main actor” award in 2011");
            System.out.println("Translated SQL: INSERT INTO award (\"awardName\") VALUES ('Best main actor');");
            stmt.execute("INSERT INTO award (\"awardName\") VALUES ('Best main actor');");
            System.out.println("                INSERT INTO \"actorObtain\" (\"actorID\", \"awardID\", \"year\") VALUES (8, 2, 2011)");
            stmt.execute("INSERT INTO \"actorObtain\" (\"actorID\", \"awardID\", \"year\") VALUES (8, 2, 2011)");
            rs = stmt.executeQuery("SELECT * FROM award");
            printTable("award");
            rs = stmt.executeQuery("SELECT * FROM \"actorObtain\"");
            printTable("actorObtain");

            System.out.println("\nStatement 2.4) Johnny Depp won the “Best villain actor” award in 2011");
            System.out.println("Translated SQL: INSERT INTO award (\"awardName\") VALUES ('Best villain actor')");
            stmt.execute("INSERT INTO award (\"awardName\") VALUES ('Best villain actor');");
            System.out.println("                INSERT INTO \"actorObtain\" (\"actorID\", \"awardID\", \"year\") VALUES (3, 3, 2011)");
            stmt.execute("INSERT INTO \"actorObtain\" (\"actorID\", \"awardID\", \"year\") VALUES (3, 3, 2011)");
            rs = stmt.executeQuery("SELECT * FROM award");
            printTable("award");
            rs = stmt.executeQuery("SELECT * FROM \"actorObtain\"");
            printTable("actorObtain");

            System.out.println("\nStatement 2.5) Edward Scissorhands won the “Best fantasy movie” award in 1991");
            System.out.println("Translated SQL: INSERT INTO award (\"awardName\") VALUES ('Best fantasy movie')");
            stmt.execute("INSERT INTO award (\"awardName\") VALUES ('Best fantasy movie');");
            System.out.println("                INSERT INTO \"movieObtain\" (\"movieID\", \"awardID\", \"year\") VALUES (2, 4, 1991)");
            stmt.execute("INSERT INTO \"movieObtain\" (\"movieID\", \"awardID\", \"year\") VALUES (2, 4, 1991)");
            rs = stmt.executeQuery("SELECT * FROM award");
            printTable("award");
            rs = stmt.executeQuery("SELECT * FROM \"movieObtain\"");
            printTable("actorObtain");

            System.out.println("\nStatement 2.6) Alice In Wonderland won the “Best fantasy movie” award in 2011");
            System.out.println("Translated SQL: INSERT INTO \"movieObtain\" (\"movieID\", \"awardID\", \"year\") VALUES (3, 4, 2011)");
            stmt.execute("INSERT INTO \"movieObtain\" (\"movieID\", \"awardID\", \"year\") VALUES (3, 4, 2011)");
            rs = stmt.executeQuery("SELECT * FROM award");
            printTable("award");
            rs = stmt.executeQuery("SELECT * FROM \"movieObtain\"");
            printTable("actorObtain");

            System.out.println("\nStatement 2.7) Youn Yuhjung won the “Best supporting actor” award in 2021");
            System.out.println("Translated SQL: INSERT INTO \"actorObtain\" (\"actorID\", \"awardID\", \"year\") VALUES (2, 1, 2021)");
            stmt.execute("INSERT INTO \"actorObtain\" (\"actorID\", \"awardID\", \"year\") VALUES (2, 1, 2021)");
            rs = stmt.executeQuery("SELECT * FROM award");
            printTable("award");
            rs = stmt.executeQuery("SELECT * FROM \"actorObtain\"");
            printTable("actorObtain");

            System.out.println("\nStatement 2.8) Minari won the “Best Foreign Language Film” award in 2021");
            System.out.println("Translated SQL: INSERT INTO award (\"awardName\") VALUES ('Best Foreign Language Film');");
            stmt.execute("INSERT INTO award (\"awardName\") VALUES ('Best Foreign Language Film');");
            System.out.println("INSERT INTO \"movieObtain\" (\"movieID\", \"awardID\", \"year\") VALUES (1, 5, 2021)");
            stmt.execute("INSERT INTO \"movieObtain\" (\"movieID\", \"awardID\", \"year\") VALUES (1, 5, 2021)");
            rs = stmt.executeQuery("SELECT * FROM award");
            printTable("award");
            rs = stmt.executeQuery("SELECT * FROM \"movieObtain\"");
            printTable("actorObtain");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // TODO: Insert rating data
        try {
            printDoubleLine();
            System.out.println("#### INSERT RATING DATA ####");
            System.out.println("\nStatement 3.1) Bob rates 3 to “The Dark Knight”.");
            System.out.println("Translated SQL: INSERT INTO \"customerRate\"(\"customerID\", \"movieID\", \"rate\") VALUES (1, 5, 3)");
            stmt.execute("INSERT INTO \"customerRate\"(\"customerID\", \"movieID\", \"rate\") VALUES (1, 5, 3)");
            System.out.println("                SELECT avg(rate) FROM \"customerRate\" WHERE \"customerRate\".\"movieID\" = movie.\"movieID\") WHERE \"movieID\" = 5");
            stmt.execute("UPDATE movie SET \"avgRate\" = " +
                    "(SELECT avg(rate) FROM \"customerRate\" WHERE \"customerRate\".\"movieID\" = movie.\"movieID\") " +
                    "WHERE \"movieID\" = 5");
            rs = stmt.executeQuery("SELECT * FROM \"customerRate\"");
            printTable("customerRate");
            rs = stmt.executeQuery("SELECT * FROM movie");
            printTable("movie");

            System.out.println("\nStatement 3.2) Bell rates 5 to the movies whose director is “Tim Burton”.");
            System.out.println("Translated SQL: INSERT INTO \"customerRate\" SELECT 5, \"movieID\", 4 FROM make NATURAL JOIN director WHERE \"directorID\"=2");
            stmt.execute("INSERT INTO \"customerRate\" SELECT 5, \"movieID\", 4 FROM make NATURAL JOIN director WHERE \"directorID\"=2");
            System.out.println("                UPDATE movie SET \"avgRate\" = (SELECT avg(rate) FROM \"customerRate\" WHERE \"customerRate\".\"movieID\" = movie.\"movieID\");");
            stmt.execute("UPDATE movie SET \"avgRate\" = " +
                    "(SELECT avg(rate) FROM \"customerRate\" WHERE \"customerRate\".\"movieID\" = movie.\"movieID\");");
            rs = stmt.executeQuery("SELECT * FROM \"customerRate\"");
            printTable("customerRate");
            rs = stmt.executeQuery("SELECT * FROM movie");
            printTable("movie");

            System.out.println("\nStatement 3.3) Jill rates 4 to the movies whose main actor is female.");
            System.out.println("Translated SQL: INSERT INTO \"customerRate\" SELECT 4, \"movieID\", 4 FROM casting NATURAL JOIN actor WHERE \"gender\" = 'Female'");
            stmt.execute("INSERT INTO \"customerRate\" SELECT 4, \"movieID\", 4 FROM casting NATURAL JOIN actor WHERE \"gender\" = 'Female'");
            System.out.println("                UPDATE movie SET \"avgRate\" = (SELECT avg(rate) FROM \"customerRate\" WHERE \"customerRate\".\"movieID\" = movie.\"movieID\");");
            stmt.execute("UPDATE movie SET \"avgRate\" = " +
                    "(SELECT avg(rate) FROM \"customerRate\" WHERE \"customerRate\".\"movieID\" = movie.\"movieID\");");
            rs = stmt.executeQuery("SELECT * FROM \"customerRate\"");
            printTable("customerRate");
            rs = stmt.executeQuery("SELECT * FROM movie");
            printTable("movie");

            System.out.println("\nStatement 3.4) Jack rates 4 to the fantasy movies.");
            System.out.println("Translated SQL: INSERT INTO \"customerRate\" SELECT 3, \"movieID\", 4 FROM \"movieGenre\" NATURAL JOIN genre WHERE \"genreName\" = 'Fantasy'");
            stmt.execute("INSERT INTO \"customerRate\" SELECT 3, \"movieID\", 4 FROM \"movieGenre\" NATURAL JOIN genre WHERE \"genreName\" = 'Fantasy'");
            System.out.println("                UPDATE movie SET \"avgRate\" = (SELECT avg(rate) FROM \"customerRate\" WHERE \"customerRate\".\"movieID\" = movie.\"movieID\");");
            stmt.execute("UPDATE movie SET \"avgRate\" = " +
                    "(SELECT avg(rate) FROM \"customerRate\" WHERE \"customerRate\".\"movieID\" = movie.\"movieID\");");
            rs = stmt.executeQuery("SELECT * FROM \"customerRate\"");
            printTable("customerRate");
            rs = stmt.executeQuery("SELECT * FROM movie");
            printTable("movie");

            System.out.println("\nStatement 3.5) John rates 5 to the movies whose actor won the “Best supporting actor” award");
            System.out.println("Translated SQL: INSERT INTO \"customerRate\" SELECT 2, \"movieID\", 5 FROM \"actorObtain\" NATURAL JOIN actor NATURAL JOIN casting WHERE  \"awardID\" = 1;");
            stmt.execute("INSERT INTO \"customerRate\" SELECT 2, \"movieID\", 5 FROM \"actorObtain\" NATURAL JOIN actor NATURAL JOIN casting WHERE  \"awardID\" = 1;");
            System.out.println("UPDATE movie SET \"avgRate\" = (SELECT avg(rate) FROM \"customerRate\" WHERE \"customerRate\".\"movieID\" = movie.\"movieID\");");
            stmt.execute("UPDATE movie SET \"avgRate\" = " +
                    "(SELECT avg(rate) FROM \"customerRate\" WHERE \"customerRate\".\"movieID\" = movie.\"movieID\");");
            rs = stmt.executeQuery("SELECT * FROM \"customerRate\"");
            printTable("customerRate");
            rs = stmt.executeQuery("SELECT * FROM movie");
            printTable("movie");
        } catch (SQLException e) {
            System.out.println("##### Insert Rating Error #####");
            e.printStackTrace();
        }

        // TODO: Select statements
        try {
            printDoubleLine();
            System.out.println("#### SELECT STATEMENTS ####");
            System.out.println("\nStatement 4) Select the names of the movies whose actor are dead.");
            System.out.println("Translated SQL: SELECT \"movieName\" FROM movie NATURAL JOIN casting NATURAL JOIN actor WHERE \"dateOfDeath\" IS NOT NULL;");
            rs = stmt.executeQuery("SELECT \"movieName\" FROM movie NATURAL JOIN casting NATURAL JOIN actor WHERE \"dateOfDeath\" IS NOT NULL;");
            printTable("movie");

            System.out.println("\nStatement 5) Select the names of the directors who cast the same actor more than once.");
            System.out.println("Translated SQL: SELECT \"directorName\" FROM director NATURAL JOIN make NATURAL JOIN casting");
            System.out.println("                GROUP BY (\"directorName\", \"actorID\") HAVING COUNT(\"directorName\") > 1 AND COUNT(\"actorID\") > 1;");
            rs = stmt.executeQuery("SELECT \"directorName\" FROM director NATURAL JOIN make NATURAL JOIN casting " +
                    "GROUP BY (\"directorName\", \"actorID\") HAVING COUNT(\"directorName\") > 1 AND COUNT(\"actorID\") > 1;");
            printTable("director");

            // TODO: This seems fucked up
            System.out.println("\nStatement 6) Select the names of the movies and the genres, where movies have the common genre.");
            System.out.println("Translated SQL: SELECT \"movieName\", \"genreName\" FROM movie NATURAL JOIN \"movieGenre\" WHERE \"genreName\" IN");
            System.out.println("                (SELECT \"genreName\" FROM \"movieGenre\" GROUP BY \"genreName\" HAVING COUNT(\"genreName\") > 1) ORDER BY \"genreName\"");
            rs = stmt.executeQuery("SELECT \"movieName\", \"genreName\" FROM movie NATURAL JOIN \"movieGenre\" WHERE \"genreName\" IN" +
                    "(SELECT \"genreName\" FROM \"movieGenre\" GROUP BY \"genreName\" HAVING COUNT(\"genreName\") > 1) ORDER BY \"genreName\"");
            printTable("movie");
        } catch (SQLException e) {
            System.out.println("##### Select Statement Error #####");
            e.printStackTrace();
        }

        // TODO: Delete statements
        try {
            printDoubleLine();
            System.out.println("#### DELETE STATEMENTS ####");
            System.out.println("\nStatement 7) Delete the movies whose director or actor did not get any award and delete data from related tables.");
            System.out.println("Translated SQL: DELETE FROM movie WHERE \"movieID\" NOT IN");
            System.out.println("                ((SELECT \"movieID\" FROM \"directorObtain\" NATURAL JOIN make) UNION ");
            System.out.println("                (SELECT \"movieID\" FROM \"actorObtain\" NATURAL JOIN casting))");
            stmt.executeUpdate("DELETE FROM movie WHERE \"movieID\" NOT IN" +
                    "((SELECT \"movieID\" FROM \"directorObtain\" NATURAL JOIN make) UNION " +
                    "(SELECT \"movieID\" FROM \"actorObtain\" NATURAL JOIN casting))"
            );
            rs = stmt.executeQuery("SELECT * FROM movie");
            printTable("movie");
            rs = stmt.executeQuery("SELECT * FROM \"directorObtain\"");
            printTable("directorObtain");
            rs = stmt.executeQuery("SELECT * FROM \"actorObtain\"");
            printTable("actorObtain");

            System.out.println("\nStatement 8) Delete all customers and delete data from related tables.");
            System.out.println("Translated SQL: DELETE FROM customer");
            System.out.println("                UPDATE movie SET \"avgRate\" = NULL");
            stmt.executeUpdate("DELETE FROM customer");
            stmt.executeUpdate("UPDATE movie SET \"avgRate\" = NULL");
            rs = stmt.executeQuery("SELECT * FROM customer");
            printTable("customer");
            rs = stmt.executeQuery("SELECT * FROM movie");
            printTable("movie");

            System.out.println("\nStatement 9) Delete all tables and data (make the database empty).");
            System.out.println("Translated SQL: DROP TABLE director CASCADE");
            System.out.println("                DROP TABLE director CASCADE");
            System.out.println("                DROP TABLE actor CASCADE");
            System.out.println("                DROP TABLE movie CASCADE");
            System.out.println("                DROP TABLE award CASCADE");
            System.out.println("                DROP TABLE genre CASCADE");
            System.out.println("                DROP TABLE \"movieGenre\" CASCADE");
            System.out.println("                DROP TABLE \"movieObtain\" CASCADE");
            System.out.println("                DROP TABLE \"actorObtain\" CASCADE");
            System.out.println("                DROP TABLE \"directorObtain\" CASCADE");
            System.out.println("                DROP TABLE casting CASCADE");
            System.out.println("                DROP TABLE make CASCADE");
            System.out.println("                DROP TABLE \"customerRate\" CASCADE");
            System.out.println("                DROP TABLE customer CASCADE");
            stmt.executeUpdate("DROP TABLE director CASCADE");
            stmt.executeUpdate("DROP TABLE actor CASCADE");
            stmt.executeUpdate("DROP TABLE movie CASCADE");
            stmt.executeUpdate("DROP TABLE award CASCADE");
            stmt.executeUpdate("DROP TABLE genre CASCADE");
            stmt.executeUpdate("DROP TABLE \"movieGenre\" CASCADE");
            stmt.executeUpdate("DROP TABLE \"movieObtain\" CASCADE");
            stmt.executeUpdate("DROP TABLE \"actorObtain\" CASCADE");
            stmt.executeUpdate("DROP TABLE \"directorObtain\" CASCADE");
            stmt.executeUpdate("DROP TABLE casting CASCADE");
            stmt.executeUpdate("DROP TABLE make CASCADE");
            stmt.executeUpdate("DROP TABLE \"customerRate\" CASCADE");
            stmt.executeUpdate("DROP TABLE customer CASCADE");
            rs = stmt.executeQuery("SELECT * FROM pg_tables where schemaname='public'");
            printTable("pg_tables");
        } catch (SQLException e) {
            System.out.println("##### Delete Statement Error #####");
            e.printStackTrace();
        }

        connection.close();
    }
}
