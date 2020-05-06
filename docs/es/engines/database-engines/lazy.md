---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 31
toc_title: Perezoso
---

# Perezoso {#lazy}

Mantiene las tablas en RAM solamente `expiration_time_in_seconds` segundos después del último acceso. Solo se puede usar con tablas \*Log.

Está optimizado para almacenar muchas tablas pequeñas \* Log, para las cuales hay un largo intervalo de tiempo entre los accesos.

## Creación De Una Base De Datos {#creating-a-database}

    CREATE DATABASE testlazy ENGINE = Lazy(expiration_time_in_seconds);

[Artículo Original](https://clickhouse.tech/docs/en/database_engines/lazy/) <!--hide-->
