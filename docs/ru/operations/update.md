---
toc_priority: 47
toc_title: "\u041e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u0435\u0020\u0043\u006c\u0069\u0063\u006b\u0048\u006f\u0075\u0073\u0065"
---

# Обновление ClickHouse {#obnovlenie-clickhouse}

Если ClickHouse установлен с помощью deb-пакетов, выполните следующие команды на сервере:

``` bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

Если ClickHouse установлен не из рекомендуемых deb-пакетов, используйте соответствующий метод обновления.

ClickHouse не поддерживает распределенное обновление. Операция должна выполняться последовательно на каждом отдельном сервере. Не обновляйте все серверы в кластере одновременно, иначе кластер становится недоступным в течение некоторого времени.
