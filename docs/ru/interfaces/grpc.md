---
toc_priority: 18
toc_title: gRPC интерфейс
---

# Интерфейс gRPC {#grpc-interface}

## Введение {#grpc-interface-introduction}

ClickHouse поддерживает интерфейс [gRPC](https://grpc.io/). Это система удаленного вызова процедур с открытым исходным кодом, которая использует HTTP/2 и [Protocol Buffers](https://ru.wikipedia.org/wiki/Protocol_Buffers). В реализации gRPC в ClickHouse поддерживаются:

-   SSL; 
-   аутентификация; 
-   сессии; 
-   сжатие; 
-   параллельные запросы, выполняемые через один канал; 
-   отмена запросов; 
-   получение прогресса операций и логов; 
-   внешние таблицы.

Спецификация интерфейса содержится в [clickhouse_grpc.proto](https://github.com/ClickHouse/ClickHouse/blob/master/src/Server/grpc_protos/clickhouse_grpc.proto).

## Конфигурация gRPC {#grpc-interface-configuration}

Чтобы сделать доступным интерфейс gRPC, нужно задать порт с помощью настройки `grpc_port` в [конфигурации сервера](../operations/configuration-files.md). Другие настройки приведены в примере:

```xml
<grpc_port>9100</grpc_port>
    <grpc>
        <enable_ssl>false</enable_ssl>

        <!-- Пути к файлам сертификатов и ключей. Используются при включенном SSL -->
        <ssl_cert_file>/path/to/ssl_cert_file</ssl_cert_file>
        <ssl_key_file>/path/to/ssl_key_file</ssl_key_file>

        <!-- Запрашивает ли сервер сертификат клиента -->
        <ssl_require_client_auth>false</ssl_require_client_auth>

        <!-- Используется, если необходимо запрашивать сертификат -->
        <ssl_ca_cert_file>/path/to/ssl_ca_cert_file</ssl_ca_cert_file>

        <!-- Алгоритм сжатия по умолчанию (применяется, если клиент не указывает алгоритм, см. result_compression в QueryInfo).
             Поддерживаются алгоритмы: none, deflate, gzip, stream_gzip -->
        <compression>deflate</compression>

        <!-- Уровень сжатия по умолчанию (применяется, если клиент не указывает уровень сжатия, см. result_compression в QueryInfo).
             Поддерживаемые уровни: none, low, medium, high -->
        <compression_level>medium</compression_level>

        <!-- Ограничение в байтах на размер отправляемых и принимаемых сообщений. -1 означает отсутствие ограничения -->
        <max_send_message_size>-1</max_send_message_size>
        <max_receive_message_size>-1</max_receive_message_size>

        <!-- Выводить ли детализированные логи -->
        <verbose_logs>false</verbose_logs>
    </grpc>
```

## Встроенный клиент {#grpc-client}

Можно написать клиент на любом языке программирования, который поддерживается gRPC, с использованием [спецификации](https://github.com/ClickHouse/ClickHouse/blob/master/src/Server/grpc_protos/clickhouse_grpc.proto).
Также можно воспользоваться встроенным Python клиентом. Он расположен в [utils/grpc-client/clickhouse-grpc-client.py](https://github.com/ClickHouse/ClickHouse/blob/master/utils/grpc-client/clickhouse-grpc-client.py) в репозитории. Для работы встроенного клиента требуются Python модули [grpcio и grpcio-tools](https://grpc.io/docs/languages/python/quickstart). 

Клиент поддерживает аргументы:

-   `--help` – вывести справку и завершить работу.
-   `--host HOST, -h HOST` – имя сервера. Значение по умолчанию: `localhost`. Можно задать адрес IPv4 или IPv6.
-   `--port PORT` – номер порта. Этот порт должен быть задан в конфигурации сервера ClickHouse настройкой `grpc_port`. Значение по умолчанию: `9100`.
-   `--user USER_NAME, -u USER_NAME` – имя пользователя. Значение по умолчанию: `default`.
-   `--password PASSWORD` – пароль. Значение по умолчанию: пустая строка.
-   `--query QUERY, -q QUERY` – запрос, который выполнится, когда используется неинтерактивный режим работы.
-   `--database DATABASE, -d DATABASE` – база данных по умолчанию. Если не указана, то будет использована база данных, заданная в настройках сервера (по умолчанию `default`).
-   `--format OUTPUT_FORMAT, -f OUTPUT_FORMAT` – [формат](formats.md) вывода результата. Значение по умолчанию для интерактивного режима: `PrettyCompact`.
-   `--debug` – вывод отладочной информации.

Чтобы запустить клиент в интерактивном режиме, не указывайте аргумент `--query`.

В неинтерактивном режиме данные запроса можно передать через `stdin`.

**Пример использования клиента**

В примере создается таблица, и в нее загружаются данные из CSV файла. Затем выводится содержимое таблицы.

``` bash
./clickhouse-grpc-client.py -q "CREATE TABLE grpc_example_table (id UInt32, text String) ENGINE = MergeTree() ORDER BY id;"
echo "0,Input data for" > a.txt ; echo "1,gRPC protocol example" >> a.txt
cat a.txt | ./clickhouse-grpc-client.py -q "INSERT INTO grpc_example_table FORMAT CSV"

./clickhouse-grpc-client.py --format PrettyCompact -q "SELECT * FROM grpc_example_table;"
```

Результат:

``` text
┌─id─┬─text──────────────────┐
│  0 │ Input data for        │
│  1 │ gRPC protocol example │
└────┴───────────────────────┘
```
