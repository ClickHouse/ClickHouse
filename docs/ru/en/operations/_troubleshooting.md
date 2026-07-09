---
title: Устранение неполадок
---

[//]: # "Этот файл включён в FAQ > Устранение неполадок"

* [Установка](#troubleshooting-installation-errors)
* [Подключение к серверу](#troubleshooting-accepts-no-connections)
* [Обработка запросов](#troubleshooting-does-not-process-queries)
* [Эффективность обработки запросов](#troubleshooting-too-slow)

<div id="troubleshooting-installation-errors">
  ## Установка
</div>

<div id="you-cannot-get-deb-packages-from-clickhouse-repository-with-apt-get">
  ### Не удаётся получить deb-пакеты из репозитория ClickHouse с помощью apt-get
</div>

* Проверьте настройки брандмауэра.
* Если по какой-либо причине у вас нет доступа к репозиторию, скачайте пакеты, как описано в статье [руководство по установке](../getting-started/install.md), и установите их вручную командой `sudo dpkg -i <packages>`. Также потребуется пакет `tzdata`.

<div id="you-cannot-update-deb-packages-from-clickhouse-repository-with-apt-get">
  ### Не удаётся обновить deb-пакеты из репозитория ClickHouse с помощью apt-get
</div>

* Эта проблема может возникнуть, если был изменён GPG-ключ.

Чтобы обновить конфигурацию репозитория, воспользуйтесь инструкцией на странице [настройки](../getting-started/install.md#setup-the-debian-repository).

<div id="you-get-different-warnings-with-apt-get-update">
  ### При выполнении `apt-get update` появляются различные предупреждения
</div>

* Предупреждения могут выглядеть следующим образом:

```bash
N: Skipping acquire of configured file 'main/binary-i386/Packages' as repository 'https://packages.clickhouse.com/deb stable InRelease' doesn't support architecture 'i386'
```

```bash
E: Failed to fetch https://packages.clickhouse.com/deb/dists/stable/main/binary-amd64/Packages.gz  File has unexpected size (30451 != 28154). Mirror sync in progress?
```

```text
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Origin' value from 'Artifactory' to 'ClickHouse'
E: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Label' value from 'Artifactory' to 'ClickHouse'
N: Repository 'https://packages.clickhouse.com/deb stable InRelease' changed its 'Suite' value from 'stable' to ''
N: This must be accepted explicitly before updates for this repository can be applied. See apt-secure(8) manpage for details.
```

```bash
Err:11 https://packages.clickhouse.com/deb stable InRelease
  400  Bad Request [IP: 172.66.40.249 443]
```

Чтобы устранить описанную выше проблему, используйте следующий скрипт:

```bash
sudo rm /var/lib/apt/lists/packages.clickhouse.com_* /var/lib/dpkg/arch /var/lib/apt/lists/partial/packages.clickhouse.com_*
sudo apt-get clean
sudo apt-get autoclean
```

<div id="you-cant-get-packages-with-yum-because-of-wrong-signature">
  ### Не удаётся получить пакеты через yum из-за неверной подписи
</div>

Возможная причина: кэш некорректен; возможно, он был повреждён после обновления GPG-ключа в 2022-09.

Решение — очистить кэш и каталог lib для yum:

```bash
sudo find /var/lib/yum/repos/ /var/cache/yum/ -name 'clickhouse-*' -type d -exec rm -rf {} +
sudo rm -f /etc/yum.repos.d/clickhouse.repo
```

После этого воспользуйтесь [руководством по установке](../getting-started/install.md#from-rpm-packages)

<div id="you-cant-run-docker-container">
  ### Не удаётся запустить контейнер Docker
</div>

Вы выполняете простую команду `docker run clickhouse/clickhouse-server`, и она завершается сбоем с трассировкой стека, похожей на следующую:

```bash
$ docker run -it clickhouse/clickhouse-server
........
Poco::Exception. Code: 1000, e.code() = 0, System exception: cannot start thread, Stack trace (when copying this message, always include the lines below):

0. Poco::ThreadImpl::startImpl(Poco::SharedPtr<Poco::Runnable, Poco::ReferenceCounter, Poco::ReleasePolicy<Poco::Runnable>>) @ 0x00000000157c7b34
1. Poco::Thread::start(Poco::Runnable&) @ 0x00000000157c8a0e
2. BaseDaemon::initializeTerminationAndSignalProcessing() @ 0x000000000d267a14
3. BaseDaemon::initialize(Poco::Util::Application&) @ 0x000000000d2652cb
4. DB::Server::initialize(Poco::Util::Application&) @ 0x000000000d128b38
5. Poco::Util::Application::run() @ 0x000000001581cfda
6. DB::Server::run() @ 0x000000000d1288f0
7. Poco::Util::ServerApplication::run(int, char**) @ 0x0000000015825e27
8. mainEntryClickHouseServer(int, char**) @ 0x000000000d125b38
9. main @ 0x0000000007ea4eee
10. ? @ 0x00007f67ff946d90
11. ? @ 0x00007f67ff946e40
12. _start @ 0x00000000062e802e
 (version 24.10.1.2812 (official build))
```

Причина — устаревший демон Docker версии ниже `20.10.10`. Исправить это можно либо обновив его, либо запустив `docker run [--privileged | --security-opt seccomp=unconfined]`. Последний вариант связан с рисками для безопасности.

<div id="troubleshooting-accepts-no-connections">
  ## Подключение к серверу
</div>

Возможные причины:

* Сервер не запущен.
* Неожиданные или неверные параметры конфигурации.

<div id="server-is-not-running">
  ### Сервер не запущен
</div>

**Проверьте, запущен ли сервер**

Команда:

```bash
$ sudo service clickhouse-server status
```

Если сервер не запущен, запустите его командой:

```bash
$ sudo service clickhouse-server start
```

**Проверьте журналы**

Основной журнал `clickhouse-server` по умолчанию находится в файле `/var/log/clickhouse-server/clickhouse-server.log`.

Если сервер успешно запустился, вы увидите строки:

* `<Information> Application: starting up.` — Сервер запущен.
* `<Information> Application: Ready for connections.` — Сервер работает и готов к подключениям.

Если `clickhouse-server` не удалось запустить из-за ошибки конфигурации, вы увидите строку `<Error>` с описанием ошибки. Например:

```text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

Если в конце файла нет сообщения об ошибке, просмотрите весь файл, начиная со строки:

```text
<Information> Application: starting up.
```

Если вы попытаетесь запустить на сервере второй экземпляр `clickhouse-server`, вы увидите следующую запись в журнале:

```text
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

**См. логи system.d**

Если в логах `clickhouse-server` нет полезной информации или они отсутствуют, вы можете просмотреть логи `system.d` с помощью команды:

```bash
$ sudo journalctl -u clickhouse-server
```

**Запустите clickhouse-server в интерактивном режиме**

```bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

Эта команда запускает сервер в интерактивном режиме со стандартными параметрами скрипта автозапуска. В этом режиме `clickhouse-server` выводит в консоль все сообщения о событиях.

<div id="configuration-parameters">
  ### Параметры конфигурации
</div>

Проверьте:

* Настройки Docker.

  Если вы запускаете ClickHouse в Docker в сети IPv6, убедитесь, что задано `network=host`.

* Настройки конечной точки.

  Проверьте настройки [listen&#95;host](../operations/server-configuration-parameters/settings.md#listen_host) и [tcp&#95;port](../operations/server-configuration-parameters/settings.md#tcp_port).

  По умолчанию сервер ClickHouse принимает подключения только с localhost.

* Настройки HTTP-протокола.

  Проверьте настройки протокола для HTTP API.

* Настройки защищённого соединения.

  Проверьте:

  * Настройку [tcp&#95;port&#95;secure](../operations/server-configuration-parameters/settings.md#tcp_port_secure).
  * Настройки [SSL-сертификатов](../operations/server-configuration-parameters/settings.md#openssl).

    При подключении используйте правильные параметры. Например, используйте параметр `port_secure` с `clickhouse_client`.

* Настройки пользователя.

  Возможно, вы используете неверное имя пользователя или пароль.

<div id="troubleshooting-does-not-process-queries">
  ## Обработка запросов
</div>

Если ClickHouse не может обработать запрос, он отправляет клиенту описание ошибки. В `clickhouse-client` описание ошибки выводится в консоль. Если вы используете HTTP-интерфейс, ClickHouse отправляет описание ошибки в теле ответа. Например:

```bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

Если вы запускаете `clickhouse-client` с параметром `stack-trace`, ClickHouse возвращает трассировку стека сервера с описанием ошибки.

Вы можете увидеть сообщение о разрыве соединения. В этом случае можно повторить запрос. Если соединение разрывается при каждом выполнении запроса, проверьте серверные журналы на наличие ошибок.

<div id="troubleshooting-too-slow">
  ## Эффективность обработки запросов
</div>

Если вы видите, что ClickHouse работает слишком медленно, необходимо проанализировать, какую нагрузку ваши запросы создают на ресурсы сервера и сеть.

Для профилирования запросов можно использовать утилиту clickhouse-benchmark. Она показывает количество запросов, обрабатываемых в секунду, число обрабатываемых строк в секунду и процентильные значения времени выполнения запросов.