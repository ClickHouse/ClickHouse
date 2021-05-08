---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 56
toc_folder_title: Dominio
toc_title: "Descripci\xF3n"
---

# Dominio {#domains}

Los dominios son tipos de propósito especial que agregan algunas características adicionales encima del tipo base existente, pero dejando intacto el formato en cable y en disco del tipo de datos subyacente. Por el momento, ClickHouse no admite dominios definidos por el usuario.

Puede usar dominios en cualquier lugar que se pueda usar el tipo base correspondiente, por ejemplo:

-   Crear una columna de un tipo de dominio
-   Leer/escribir valores desde/a la columna de dominio
-   Úselo como un índice si un tipo base se puede usar como un índice
-   Funciones de llamada con valores de la columna de dominio

### Características adicionales de los dominios {#extra-features-of-domains}

-   Nombre de tipo de columna explícito en `SHOW CREATE TABLE` o `DESCRIBE TABLE`
-   Entrada del formato humano-amistoso con `INSERT INTO domain_table(domain_column) VALUES(...)`
-   Salida al formato humano-amistoso para `SELECT domain_column FROM domain_table`
-   Carga de datos desde una fuente externa en el formato de uso humano: `INSERT INTO domain_table FORMAT CSV ...`

### Limitacion {#limitations}

-   No se puede convertir la columna de índice del tipo base al tipo de dominio a través de `ALTER TABLE`.
-   No se pueden convertir implícitamente valores de cadena en valores de dominio al insertar datos de otra columna o tabla.
-   Domain no agrega restricciones en los valores almacenados.

[Artículo Original](https://clickhouse.tech/docs/en/data_types/domains/) <!--hide-->
