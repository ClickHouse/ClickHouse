---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 31
toc_title: Perezoso
---

# Perezoso {#lazy}

Mantiene las tablas en RAM solamente `expiration_time_in_seconds` segundos después del último acceso. Solo se puede usar con tablas \*Log.

Está optimizado para almacenar muchas tablas pequeñas \* Log, para las cuales hay un largo intervalo de tiempo entre los accesos.

## Creación de una base de datos {#creating-a-database}

    CREATE DATABASE testlazy ENGINE = Lazy(expiration_time_in_seconds);

[Artículo Original](https://clickhouse.tech/docs/en/database_engines/lazy/) <!--hide-->
