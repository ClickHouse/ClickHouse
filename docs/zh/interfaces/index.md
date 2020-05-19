# 客户端 {#interfaces}

ClickHouse提供了两个网络接口（两者都可以选择包装在TLS中以提高安全性）：

-   [HTTP](http.md)，记录在案，易于使用.
-   [本地TCP](tcp.md)，这有较少的开销.

在大多数情况下，建议使用适当的工具或库，而不是直接与这些工具或库进行交互。 Yandex的官方支持如下：
\* [命令行客户端](cli.md)
\* [JDBC驱动程序](jdbc.md)
\* [ODBC驱动程序](odbc.md)
\* [C++客户端库](cpp.md)

还有许多第三方库可供使用ClickHouse：
\* [客户端库](third-party/client-libraries.md)
\* [集成](third-party/integrations.md)
\* [可视界面](third-party/gui.md)

[来源文章](https://clickhouse.tech/docs/zh/interfaces/) <!--hide-->
