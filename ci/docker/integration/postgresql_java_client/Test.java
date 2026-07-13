import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

class JavaConnectorTest {
    private static final String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS default.test1 (`age` Int32, `name` String, `int_nullable` Nullable(Int32)) Engine = Memory";
    private static final String INSERT_SQL = "INSERT INTO default.test1(`age`, `name`) VALUES(33, 'jdbc'),(44, 'ck')";
    private static final String SELECT_SQL = "SELECT * FROM default.test1";
    private static final String SELECT_NUMBER_SQL = "SELECT * FROM system.numbers LIMIT 13";
    private static final String DROP_TABLE_SQL = "DROP TABLE default.test1";

    public static void main(String[] args) {
        int i = 0;
        String host = "127.0.0.1";
        String port = "5432";
        String user = "default";
        String password = "";
        String database = "default";
        while (i < args.length) {
            switch (args[i]) {
                case "--host":
                    host = args[++i];
                    break;
                case "--port":
                    port = args[++i];
                    break;
                case "--user":
                    user = args[++i];
                    break;
                case "--password":
                    password = args[++i];
                    break;
                case "--database":
                    database = args[++i];
                    break;
                default:
                    i++;
                    break;
            }
        }

        String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s", host, port, database);

        Connection conn = null;
        Statement stmt = null;
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("preferQueryMode", "simple");
        props.setProperty("sslmode", "disable");
        try {
            conn = DriverManager.getConnection(jdbcUrl, props);
            stmt = conn.createStatement();
            stmt.executeUpdate(CREATE_TABLE_SQL);
            stmt.executeUpdate(INSERT_SQL);

            ResultSet rs = stmt.executeQuery(SELECT_SQL);
            while (rs.next()) {
                System.out.print(rs.getString("age"));
                System.out.print(rs.getString("name"));
                System.out.print(rs.getString("int_nullable"));
                System.out.println();
            }

            stmt.executeUpdate(DROP_TABLE_SQL);

            rs = stmt.executeQuery(SELECT_NUMBER_SQL);
            while (rs.next()) {
                System.out.print(rs.getString(1));
                System.out.println();
            }

            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
