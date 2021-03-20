---
toc_priority: 47
toc_title: "Обновление ClickHouse"
---

# Обновление ClickHouse {#clickhouse-upgrade}

Если ClickHouse установлен с помощью deb-пакетов, выполните следующие команды на сервере:

``` bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

Если ClickHouse установлен не из рекомендуемых deb-пакетов, используйте соответствующий метод обновления.

!!! warning "Предупреждение"
    ClickHouse не поддерживает распределенное обновление. Операция обновления должна выполняться последовательно на каждом отдельном сервере. Не обновляйте все серверы в кластере одновременно, иначе кластер станет недоступен в течение некоторого времени.

Обновление ClickHouse до определенной версии:

Пример:

`xx.yy.a.b` — это номер текущей стабильной версии. Последнюю стабильную версию можно узнать [здесь](https://github.com/ClickHouse/ClickHouse/releases)

```bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-server=xx.yy.a.b clickhouse-client=xx.yy.a.b clickhouse-common-static=xx.yy.a.b
$ sudo service clickhouse-server restart
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/update/) <!--hide-->
