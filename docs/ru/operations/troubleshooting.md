---
sidebar_position: 46
sidebar_label: "Устранение неисправностей"
---

# Устранение неисправностей {#ustranenie-neispravnostei}

-   [Установка дистрибутива](#troubleshooting-installation-errors)
-   [Соединение с сервером](#troubleshooting-accepts-no-connections)
-   [Обработка запросов](#troubleshooting-does-not-process-queries)
-   [Скорость обработки запросов](#troubleshooting-too-slow)

## Установка дистрибутива {#troubleshooting-installation-errors}

### Не получается скачать deb-пакеты из репозитория ClickHouse с помощью Apt-get {#ne-poluchaetsia-skachat-deb-pakety-iz-repozitoriia-clickhouse-s-pomoshchiu-apt-get}

-   Проверьте настройки брандмауэра.
-   Если по какой-либо причине вы не можете получить доступ к репозиторию, скачайте пакеты как описано в разделе [Начало работы](../getting-started/index.md) и установите их вручную командой `sudo dpkg -i <packages>`. Также, необходим пакет `tzdata`.

## Соединение с сервером {#troubleshooting-accepts-no-connections}

Возможные проблемы:

-   Сервер не запущен.
-   Неожиданные или неправильные параметры конфигурации.

### Сервер не запущен {#server-ne-zapushchen}

**Проверьте, запущен ли сервер**

Команда:

``` bash
$ sudo service clickhouse-server status
```

Если сервер не запущен, запустите его с помощью команды:

``` bash
$ sudo service clickhouse-server start
```

**Проверьте журналы**

Основной лог `clickhouse-server` по умолчанию — `/var/log/clickhouse-server/clickhouse-server.log`.

В случае успешного запуска вы должны увидеть строки, содержащие:

-   `<Information> Application: starting up.` — сервер запускается.
-   `<Information> Application: Ready for connections.` — сервер запущен и готов принимать соединения.

Если `clickhouse-server` не запустился из-за ошибки конфигурации вы увидите `<Error>` строку с описанием ошибки. Например:

``` text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

Если вы не видите ошибки в конце файла, просмотрите весь файл начиная со строки:

``` text
<Information> Application: starting up.
```

При попытке запустить второй экземпляр `clickhouse-server` журнал выглядит следующим образом:

``` text
2019.01.11 15:25:11.151730 [ 1 ] {} <Information> : Starting ClickHouse 19.1.0 with revision 54413
2019.01.11 15:25:11.154578 [ 1 ] {} <Information> Application: starting up
2019.01.11 15:25:11.156361 [ 1 ] {} <Information> StatusFile: Status file ./status already exists - unclean restart. Contents:
PID: 8510
Started at: 2019-01-11 15:24:23
Revision: 54413

2019.01.11 15:25:11.156673 [ 1 ] {} <Error> Application: DB::Exception: Cannot lock file ./status. Another server instance in same directory is already running.
2019.01.11 15:25:11.156682 [ 1 ] {} <Information> Application: shutting down
2019.01.11 15:25:11.156686 [ 1 ] {} <Debug> Application: Uninitializing subsystem: Logging Subsystem
2019.01.11 15:25:11.156716 [ 2 ] {} <Information> BaseDaemon: Stop SignalListener thread
```

**Проверьте логи system.d**

Если из логов `clickhouse-server` вы не получили необходимой информации или логов нет, то вы можете посмотреть логи `system.d` командой:

``` bash
$ sudo journalctl -u clickhouse-server
```

**Запустите clickhouse-server в интерактивном режиме**

``` bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

Эта команда запускает сервер как интерактивное приложение со стандартными параметрами скрипта автозапуска. В этом режиме `clickhouse-server` выводит сообщения в консоль.

### Параметры конфигурации {#parametry-konfiguratsii}

Проверьте:

-   Настройки Docker.

        При запуске ClickHouse в Docker в сети IPv6 убедитесь, что установлено `network=host`.

-   Параметры endpoint.

        Проверьте настройки [listen_host](./server-configuration-parameters//settings.md#server_configuration_parameters-listen_host) и [tcp_port](./server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port).

        По умолчанию, сервер ClickHouse принимает только локальные подключения.

-   Настройки протокола HTTP.

        Проверьте настройки протокола для HTTP API.

-   Параметры безопасного подключения.

        Проверьте:

        - Настройку `tcp_port_secure`.
        - Параметры для SSL-сертификатов.

         Используйте правильные параметры при подключении. Например, используйте параметр `port_secure` при использовании `clickhouse_client`.

-   Настройки пользователей.

        Возможно, вы используете неверное имя пользователя или пароль.

## Обработка запросов {#troubleshooting-does-not-process-queries}

Если ClickHouse не может обработать запрос, он отправляет клиенту описание ошибки. В `clickhouse-client` вы получаете описание ошибки в консоли. При использовании интерфейса HTTP, ClickHouse отправляет описание ошибки в теле ответа. Например:

``` bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

Если вы запускаете `clickhouse-client` c параметром `stack-trace`, то ClickHouse возвращает описание ошибки и соответствующий стек вызовов функций на сервере.

Может появиться сообщение о разрыве соединения. В этом случае необходимо повторить запрос. Если соединение прерывается каждый раз при выполнении запроса, следует проверить журналы сервера на наличие ошибок.

## Скорость обработки запросов {#troubleshooting-too-slow}

Если вы видите, что ClickHouse работает слишком медленно, необходимо профилировать загрузку ресурсов сервера и сети для ваших запросов.

Для профилирования запросов можно использовать утилиту clickhouse-benchmark. Она показывает количество запросов, обработанных за секунду, количество строк, обработанных за секунду и перцентили времени обработки запросов.
