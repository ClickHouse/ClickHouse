# Interfaz nativa (TCP) {#native-interface-tcp}

El protocolo nativo se utiliza en el [cliente de línea de comandos](cli.md), para la comunicación entre servidores durante el procesamiento de consultas distribuidas, y también en otros programas de C, Desafortunadamente, el protocolo nativo de ClickHouse aún no tiene especificaciones formales, pero puede ser diseñado de manera inversa desde el código fuente de ClickHouse (comenzando [por aquí](https://github.com/ClickHouse/ClickHouse/tree/master/dbms/src/Client)) y/o mediante la interceptación y el análisis del tráfico TCP.

[Artículo Original](https://clickhouse.tech/docs/es/interfaces/tcp/) <!--hide-->
