---
machine_translated: true
---

# Dominio {#domains}

Los dominios son tipos de propósito especial, que agregan algunas características adicionales encima del tipo base existente, dejando intacto el formato en cable y en disco de la tabla subyacente. Por el momento, ClickHouse no admite dominios definidos por el usuario.

Puede usar dominios en cualquier lugar que se pueda usar el tipo base correspondiente:

-   Crear una columna de tipo de dominio
-   Leer/escribir valores desde/a la columna de dominio
-   Úselo como índice si el tipo base se puede usar como índice
-   Funciones de llamada con valores de la columna de dominio
-   sucesivamente.

### Características adicionales de los dominios {#extra-features-of-domains}

-   Nombre de tipo de columna explícito en `SHOW CREATE TABLE` o `DESCRIBE TABLE`
-   Entrada del formato humano-amistoso con `INSERT INTO domain_table(domain_column) VALUES(...)`
-   Salida al formato humano-amistoso para `SELECT domain_column FROM domain_table`
-   Carga de datos desde una fuente externa en un formato amigable para los humanos: `INSERT INTO domain_table FORMAT CSV ...`

### Limitación {#limitations}

-   No se puede convertir la columna de índice del tipo base al tipo de dominio a través de `ALTER TABLE`.
-   No se pueden convertir implícitamente valores de cadena en valores de dominio al insertar datos de otra columna o tabla.
-   Domain no agrega restricciones en los valores almacenados.

[Artículo Original](https://clickhouse.tech/docs/es/data_types/domains/overview) <!--hide-->
