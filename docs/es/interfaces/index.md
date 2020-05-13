---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_folder_title: Interfaces
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
