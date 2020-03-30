---
machine_translated: true
---

# Perezoso {#lazy}

Mantiene las tablas en RAM solamente `expiration_time_in_seconds` segundos después del último acceso. Solo se puede usar con tablas \*Log.

Está optimizado para almacenar muchas tablas pequeñas \* Log, para las cuales hay un intervalo de tiempo largo entre los accesos.

## Creación de una base de datos {#creating-a-database}

CREAR BASE DE DATOS testlazy ENGINE = Lazy(expiration\_time\_in\_seconds);

[Artículo Original](https://clickhouse.tech/docs/es/database_engines/lazy/) <!--hide-->
