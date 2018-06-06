# Начало работы

## Системные требования

Система некроссплатформенная. Требуется ОС Linux Ubuntu не более старая, чем Precise (12.04); архитектура x86_64 с поддержкой набора инструкций SSE 4.2.
Для проверки наличия SSE 4.2, выполните:

```bash
grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

Рекомендуется использовать Ubuntu Trusty или Ubuntu Xenial или Ubuntu Precise.
Терминал должен работать в кодировке UTF-8 (как по умолчанию в Ubuntu).

## Установка

В целях тестирования и разработки, система может быть установлена на один сервер или на рабочий компьютер.

### Установка из пакетов для Debian/Ubuntu 

Пропишите в `/etc/apt/sources.list` (или в отдельный файл `/etc/apt/sources.list.d/clickhouse.list`) репозитории:

```text
deb http://repo.yandex.ru/clickhouse/deb/stable/ main/
```

Если вы хотите использовать наиболее свежую тестовую версию, замените stable на testing.

Затем выполните:

```bash
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4    # optional
sudo apt-get update
sudo apt-get install clickhouse-client clickhouse-server
```

Также можно скачать и установить пакеты вручную, отсюда: <https://repo.yandex.ru/clickhouse/deb/stable/main/>.

ClickHouse содержит настройки ограничения доступа. Они расположены в файле users.xml (рядом с config.xml).
По умолчанию, разрешён доступ отовсюду для пользователя default без пароля. См. секцию users/default/networks.
Подробнее смотрите в разделе "конфигурационные файлы".

### Установка из исходников

Для сборки воспользуйтесь инструкцией: build.md

Вы можете собрать пакеты и установить их.
Также вы можете использовать программы без установки пакетов.

```text
Client: dbms/programs/clickhouse-client
Server: dbms/programs/clickhouse-server
```

Для сервера создаёте директории с данными, например:

```text
/opt/clickhouse/data/default/
/opt/clickhouse/metadata/default/
```

(Настраивается в конфиге сервера.)
Сделайте chown под нужного пользователя.

Обратите внимание на путь к логам в конфиге сервера (src/dbms/programs/server/config.xml).

### Другие методы установки

Docker образ: <https://hub.docker.com/r/yandex/clickhouse-server/>

RPM пакеты для CentOS, RHEL: <https://github.com/Altinity/clickhouse-rpm-install>

Gentoo overlay: <https://github.com/kmeaw/clickhouse-overlay>

## Запуск

Для запуска сервера (в качестве демона), выполните:

```bash
sudo service clickhouse-server start
```

Смотрите логи в директории `/var/log/clickhouse-server/`

Если сервер не стартует - проверьте правильность конфигурации в файле `/etc/clickhouse-server/config.xml`

Также можно запустить сервер из консоли:

```bash
clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

При этом, лог будет выводиться в консоль - удобно для разработки.
Если конфигурационный файл лежит в текущей директории, то указывать параметр --config-file не требуется - по умолчанию будет использован файл ./config.xml

Соединиться с сервером можно с помощью клиента командной строки:

```bash
clickhouse-client
```

Параметры по умолчанию обозначают - соединяться с localhost:9000, от имени пользователя default без пароля.
Клиент может быть использован для соединения с удалённым сервером. Пример:

```bash
clickhouse-client --host=example.com
```

Подробнее смотри раздел "Клиент командной строки".

Проверим работоспособность системы:

```bash
milovidov@hostname:~/work/metrica/src/dbms/src/Client$ ./clickhouse-client
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

Для дальнейших экспериментов можно попробовать загрузить из тестовых наборов данных.
