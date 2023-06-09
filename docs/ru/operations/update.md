---
sidebar_position: 47
sidebar_label: "Обновление ClickHouse"
---

# Обновление ClickHouse {#clickhouse-upgrade}

Если ClickHouse установлен с помощью deb-пакетов, выполните следующие команды на сервере:

``` bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

Если ClickHouse установлен не из рекомендуемых deb-пакетов, используйте соответствующий метод обновления.

    :::note "Примечание"
    Вы можете обновить сразу несколько серверов, кроме случая, когда все реплики одного шарда отключены.
    :::
Обновление ClickHouse до определенной версии:

**Пример**

`xx.yy.a.b` — это номер текущей стабильной версии. Последнюю стабильную версию можно узнать [здесь](https://github.com/ClickHouse/ClickHouse/releases)

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-server=xx.yy.a.b clickhouse-client=xx.yy.a.b clickhouse-common-static=xx.yy.a.b
$ sudo service clickhouse-server restart
```
