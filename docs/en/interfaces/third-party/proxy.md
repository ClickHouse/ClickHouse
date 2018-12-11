# Proxy Servers from Third-party Developers

## chproxy

[chproxy](https://github.com/Vertamedia/chproxy), is an http proxy and load balancer for ClickHouse database. 

Features:

* Per-user routing and response caching.
* Flexible limits.
* Automatic SSL cerificate renewal.

Implemented in Go.

## KittenHouse

[KittenHouse](https://github.com/VKCOM/kittenhouse) is designed to be a local proxy between ClickHouse and application server in case it's impossible or inconvenient to buffer INSERT data on your application side.

Features:

* In-memory and on-disk data buffering.
* Per-table routing.
* Load-balancing and health checking.

Implemented in Go.
[Original article](https://clickhouse.yandex/docs/en/interfaces/third-party/proxy/) <!--hide-->
