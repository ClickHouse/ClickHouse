# Interfaces {#interfaces}

ClickHouse provides two network interfaces (both can be optionally wrapped in TLS for additional security):

* [HTTP](http.md), which is documented and easy to use directly.
* [Native TCP](tcp.md), which has less overhead.

In most cases it is recommended to use appropriate tool or library instead of interacting with those directly. Officially supported by Yandex are the following:

* [Command-line client](cli.md)
* [JDBC driver](jdbc.md)
* [ODBC driver](odbc.md)

There are also a wide range of third-party libraries for working with ClickHouse:

* [Client libraries](third-party/client_libraries.md)
* [Integrations](third-party/integrations.md)
* [Visual interfaces](third-party/gui.md)

[Original article](https://clickhouse.yandex/docs/en/interfaces/) <!--hide-->
