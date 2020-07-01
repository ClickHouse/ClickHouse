---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: Interfaz
toc_priority: 14
toc_title: "Implantaci\xF3n"
---

# Interfaz {#interfaces}

ClickHouse proporciona dos interfaces de red (ambas se pueden ajustar opcionalmente en TLS para mayor seguridad):

-   [HTTP](http.md), que está documentado y fácil de usar directamente.
-   [TCP nativo](tcp.md), que tiene menos sobrecarga.

En la mayoría de los casos, se recomienda utilizar la herramienta o biblioteca apropiada en lugar de interactuar con ellos directamente. Oficialmente apoyados por Yandex son los siguientes:

-   [Cliente de línea de comandos](cli.md)
-   [Controlador JDBC](jdbc.md)
-   [Controlador ODBC](odbc.md)
-   [Biblioteca cliente de C++](cpp.md)

También hay una amplia gama de bibliotecas de terceros para trabajar con ClickHouse:

-   [Bibliotecas de clientes](third-party/client-libraries.md)
-   [Integración](third-party/integrations.md)
-   [Interfaces visuales](third-party/gui.md)

[Artículo Original](https://clickhouse.tech/docs/en/interfaces/) <!--hide-->
