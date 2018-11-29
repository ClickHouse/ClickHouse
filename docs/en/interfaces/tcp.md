# Native Interface (TCP)

The native protocol is used in the [command-line client](cli.md), for interserver communication during distributed query processing, and also in other C++ programs. Unfortunately, native ClickHouse protocol does not have formal specification yet, but it can be reverse engineered from ClickHouse source code (starting [around here](https://github.com/yandex/ClickHouse/tree/master/dbms/src/Client)) and/or by intercepting and analyzing TCP traffic.

[Original article](https://clickhouse.yandex/docs/en/interfaces/tcp/) <!--hide-->
