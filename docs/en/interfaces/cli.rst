Command-line client
-----------------------
Для работы из командной строки вы можете использовать clickhouse-client:
::
    $ clickhouse-client
    ClickHouse client version 0.0.26176.
    Connecting to localhost:9000.
    Connected to ClickHouse server version 0.0.26176.
    
    :) SELECT 1
    

The ``clickhouse-client`` program accepts the following parameters, which are all optional:

``--host, -h`` - server name, by defaul - localhost.
You can use either the name or the IPv4 or IPv6 address.

``--port`` - The port to connect to, by default - 9000.
Note that the HTTP interface and the native interface use different ports.

``--user, -u`` -  The username, by default - default.

``--password`` - The password, by default - empty string.

``--query, -q`` - Query to process when using non-interactive mode.

``--database, -d`` - Select the current default database, by default - the current DB from the server settings (by default, the 'default' DB).

``--multiline, -m`` - If specified, allow multiline queries (do not send request on Enter).

``--multiquery, -n`` - If specified, allow processing multiple queries separated by semicolons.
Only works in non-interactive mode.

``--format, -f`` - Use the specified default format to output the result.
``--vertical, -E`` - If specified, use the Vertical format by default to output the result. This is the same as '--format=Vertical'. In this format, each value is printed on a separate line, which is helpful when displaying wide tables.
``--time, -t`` - If specified, print the query execution time to 'stderr' in non-interactive mode.
``--stacktrace`` - If specified, also prints the stack trace if an exception occurs.
``--config-file`` - Name of the configuration file that has additional settings or changed defaults for the settings listed above.
By default, files are searched for in this order:
./clickhouse-client.xml
~/./clickhouse-client/config.xml
/etc/clickhouse-client/config.xml
Settings are only taken from the first file found.

You can also specify any settings that will be used for processing queries. For example, ``clickhouse-client --max_threads=1``. For more information, see the section "Settings".

The client can be used in interactive and non-interactive (batch) mode.
To use batch mode, specify the 'query' parameter, or send data to 'stdin' (it verifies that 'stdin' is not a terminal), or both.
Similar to the HTTP interface, when using the 'query' parameter and sending data to 'stdin', the request is a concatenation of the 'query' parameter, a line break, and the data in 'stdin'. This is convenient for large INSERT queries.

Examples for insert data via clickhouse-client:
::
    echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";

    cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
    3, 'some text', '2016-08-14 00:00:00'
    4, 'some more text', '2016-08-14 00:00:01'
    _EOF
    
    cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";


In batch mode, the default data format is TabSeparated. You can set the format in the FORMAT clause of the query.

By default, you can only process a single query in batch mode. To make multiple queries from a "script," use the 'multiquery' parameter. This works for all queries except INSERT. Query results are output consecutively without additional separators.
Similarly, to process a large number of queries, you can run 'clickhouse-client' for each query. Note that it may take tens of milliseconds to launch the 'clickhouse-client' program.

In interactive mode, you get a command line where you can enter queries.

If 'multiline' is not specified (the default):
To run a query, press Enter. The semicolon is not necessary at the end of the query. To enter a multiline query, enter a backslash ``\`` before the line break - after you press Enter, you will be asked to enter the next line of the query.

If 'multiline' is specified:
To run a query, end it with a semicolon and press Enter. If the semicolon was omitted at the end of the entered line, you will be asked to enter the next line of the query.

You can specify ``\G`` instead of or after the semicolon. This indicates using Vertical format. In this format, each value is printed on a separate line, which is convenient for wide tables. This unusual feature was added for compatibility with the MySQL CLI.

The command line is based on 'readline' (and 'history') (or 'libedit', or even nothing, depending on build). In other words, it uses the familiar keyboard shortcuts and keeps a history. The history is written to /.clickhouse-client-history.

By default, the format used is PrettyCompact. You can change the format in the FORMAT clause of the query, or by specifying '\G' at the end of the query, using the '--format' or '--vertical' argument in the command line, or using the client configuration file.

To exit the client, press Ctrl+D (or Ctrl+C), or enter one of the following :
"exit", "quit", "logout", "учше", "йгше", "дщпщге", "exit;", "quit;", "logout;", "учшеж", "йгшеж", "дщпщгеж", "q", "й", "\q", "\Q", ":q", "\й", "\Й", "Жй"

When processing a query, the client shows:
#. Progress, which is updated no more than 10 times per second (by default). For quick queries, the progress might not have time to be displayed.
#. The formatted query after parsing, for debugging.
#. The result in the specified format.
#. The number of lines in the result, the time passed, and the average speed of query processing.

To cancel a lengthy query, press Ctrl+C. However, you will still need to wait a little for the server to abort the request. It is not possible to cancel a query at certain stages. If you don't wait and press Ctrl+C a second time, the client will exit.

The command-line client allows passing external data (external temporary tables) for querying. For more information, see the section "External data for request processing".
