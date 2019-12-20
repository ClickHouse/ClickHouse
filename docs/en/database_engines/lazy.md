# Lazy

Works like `Ordinary`, but keeps tables in RAM only `expiration_time_in_seconds` seconds after last access. Can be used only with \*Log tables.

It's optimized for storing many small \*Log tables, for which there is a long time interval between accesses.

## Creating a Database

```
CREATE DATABASE testlazy ENGINE = Lazy(expiration_time_in_seconds);
```

[Original article](https://clickhouse.yandex/docs/en/database_engines/lazy/) <!--hide-->
