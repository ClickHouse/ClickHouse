# Начало работы

## Системные требования

ClickHouse может работать на любом Linux, FreeBSD или Mac OS X с архитектурой процессора x86\_64.

Хотя предсобранные релизы обычно компилируются с использованием набора инструкций SSE 4.2, что добавляет использование поддерживающего его процессора в список системных требований. Команда для проверки наличия поддержки инструкций SSE 4.2 на текущем процессоре:

```bash
$ grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

## Установка

### Из DEB пакетов

Яндекс рекомендует использовать официальные скомпилированные `deb` пакеты для Debian или Ubuntu.

Чтобы установить официальные пакеты, пропишите репозиторий Яндекса в `/etc/apt/sources.list` или в отдельный файл `/etc/apt/sources.list.d/clickhouse.list`:

```
deb http://repo.yandex.ru/clickhouse/deb/stable/ main/
```

Если вы хотите использовать наиболее свежую тестовую, замените `stable` на `testing` (не рекомендуется для production окружений).

Затем для самой установки пакетов выполните:

```bash
sudo apt-get install dirmngr    # optional
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4    # optional
sudo apt-get update
sudo apt-get install clickhouse-client clickhouse-server
```

Также эти пакеты можно скачать и установить вручную отсюда: <https://repo.yandex.ru/clickhouse/deb/stable/main/>.

### Из RPM пакетов

Яндекс не использует ClickHouse на поддерживающих `rpm` дистрибутивах Linux, а также `rpm` пакеты менее тщательно тестируются. Таким образом, использовать их стоит на свой страх и риск, но, тем не менее, многие другие компании успешно работают на них в production без каких-либо серьезных проблем.

Для CentOS, RHEL и Fedora возможны следующие варианты:

* Пакеты из <https://repo.yandex.ru/clickhouse/rpm/stable/x86_64/> генерируются на основе официальных `deb` пакетов от Яндекса и содержат в точности тот же исполняемый файл.
* Пакеты из <https://github.com/Altinity/clickhouse-rpm-install> собираются независимой компанией Altinity, но широко используются без каких-либо нареканий.
* Либо можно использовать Docker (см. ниже).

### Из Docker образа

Для запуска ClickHouse в Docker нужно следовать инструкции на [Docker Hub](https://hub.docker.com/r/yandex/clickhouse-server/). Внутри образов используются официальные `deb` пакеты.

### Из исходникого кода

Для компиляции ClickHouse вручную, используйте инструкцию для [Linux](../development/build.md) или [Mac OS X](../development/build_osx.md).

Можно скомпилировать пакеты и установить их, либо использовать программы без установки пакетов. Также при ручой сборке можно отключить необходимость поддержки набора инструкций SSE 4.2 или собрать под процессоры архитектуры AArch64.

```
Client: dbms/programs/clickhouse-client
Server: dbms/programs/clickhouse-server
```

Для работы собранного вручную сервера необходимо создать директории для данных и метаданных, а также сделать их `chown` для желаемого пользователя. Пути к этим директориям могут быть изменены в конфигурационном файле сервера (src/dbms/programs/server/config.xml), по умолчанию используются следующие:

```
/opt/clickhouse/data/default/
/opt/clickhouse/metadata/default/
```

На Gentoo для установки ClickHouse из исходного кода можно использовать просто `emerge clickhouse`.

## Запуск

Для запуска сервера в качестве демона, выполните:

``` bash
$ sudo service clickhouse-server start
```

Смотрите логи в директории `/var/log/clickhouse-server/`.

Если сервер не стартует, проверьте корректность конфигурации в файле `/etc/clickhouse-server/config.xml`

Также можно запустить сервер вручную из консоли:

``` bash
$ clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

При этом, лог будет выводиться в консоль, что удобно для разработки.
Если конфигурационный файл лежит в текущей директории, то указывать параметр `--config-file` не требуется, по умолчанию будет использован файл `./config.xml`.

После запуска сервера, соединиться с ним можно с помощью клиента командной строки:

``` bash
$ clickhouse-client
```

По умолчанию он соединяется с localhost:9000, от имени пользователя `default` без пароля. Также клиент может быть использован для соединения с удалённым сервером с помощью аргумента `--host`.

Терминал должен использовать кодировку UTF-8.

Более подробная информация о клиенте располагается в разделе [«Клиент командной строки»](../interfaces/cli.md).

Пример проверки работоспособности системы:

``` bash
$ ./clickhouse-client
ClickHouse client version 0.0.18749.
Connecting to localhost:9000.
Connected to ClickHouse server version 0.0.18749.

:) SELECT 1

SELECT 1

┌─1─┐
│ 1 │
└───┘

1 rows in set. Elapsed: 0.003 sec.

:)
```

**Поздравляем, система работает!**

Для дальнейших экспериментов можно попробовать загрузить один из тестовых наборов данных или пройти [пошаговое руководство для начинающих](https://clickhouse.yandex/tutorial.html).

[Оригинальная статья](https://clickhouse.yandex/docs/ru/getting_started/) <!--hide-->
