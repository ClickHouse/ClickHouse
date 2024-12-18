---
slug: /ru/getting-started/install
sidebar_position: 11
sidebar_label: "Установка"
---

# Установка {#ustanovka}

## Системные требования {#sistemnye-trebovaniia}

ClickHouse может работать на любой операционной системе Linux, FreeBSD или Mac OS X с архитектурой процессора x86-64, AArch64 или PowerPC64LE.

Предварительно собранные пакеты компилируются для x86-64 и используют набор инструкций SSE 4.2, поэтому, если не указано иное, его поддержка в используемом процессоре, становится дополнительным требованием к системе. Вот команда, чтобы проверить, поддерживает ли текущий процессор SSE 4.2:

``` bash
grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```

Чтобы запустить ClickHouse на процессорах, которые не поддерживают SSE 4.2, либо имеют архитектуру AArch64 или PowerPC64LE, необходимо самостоятельно [собрать ClickHouse из исходного кода](#from-sources) с соответствующими настройками конфигурации.

## Доступные варианты установки {#dostupnye-varianty-ustanovki}

### Из deb-пакетов {#install-from-deb-packages}

Рекомендуется использовать официальные скомпилированные `deb`-пакеты для Debian или Ubuntu. Для установки пакетов выполните:

``` bash
sudo apt-get install -y apt-transport-https ca-certificates curl gnupg
curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' | sudo gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb stable main" | sudo tee \
    /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update
```

#### Установка ClickHouse server и client

```bash
sudo apt-get install -y clickhouse-server clickhouse-client
```

#### Запуск ClickHouse server

```bash
sudo service clickhouse-server start
clickhouse-client # or "clickhouse-client --password" if you've set up a password.
```

Чтобы использовать различные [версии ClickHouse](../faq/operations/production.md) в зависимости от ваших потребностей, вы можете заменить `stable` на `lts` или `testing`.

Также вы можете вручную скачать и установить пакеты из [репозитория](https://packages.clickhouse.com/deb/pool/stable).

#### Пакеты {#packages}

-   `clickhouse-common-static` — устанавливает исполняемые файлы ClickHouse.
-   `clickhouse-server` — создаёт символические ссылки для `clickhouse-server` и устанавливает конфигурационные файлы.
-   `clickhouse-client` — создаёт символические ссылки для `clickhouse-client` и других клиентских инструментов и устанавливает конфигурационные файлы `clickhouse-client`.
-   `clickhouse-common-static-dbg` — устанавливает исполняемые файлы ClickHouse собранные с отладочной информацией.

:::note Внимание
Если вам нужно установить ClickHouse определённой версии, вы должны установить все пакеты одной версии:
`sudo apt-get install clickhouse-server=21.8.5.7 clickhouse-client=21.8.5.7 clickhouse-common-static=21.8.5.7`
:::
### Из rpm-пакетов {#from-rpm-packages}

Команда ClickHouse рекомендует использовать официальные предкомпилированные `rpm`-пакеты для CentOS, RedHat и всех остальных дистрибутивов Linux, основанных на rpm.

#### Установка официального репозитория

Сначала нужно подключить официальный репозиторий:

``` bash
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://packages.clickhouse.com/rpm/clickhouse.repo
```

Для систем с пакетным менеджером `zypper` (openSUSE, SLES):

``` bash
sudo zypper addrepo -r https://packages.clickhouse.com/rpm/clickhouse.repo -g
sudo zypper --gpg-auto-import-keys refresh clickhouse-stable
```

Далее любая команда `yum install` может быть заменена на `zypper install`. Чтобы указать желаемую версию, необходимо добавить `-$VERSION` в имени пакета, например `clickhouse-client-22.2.2.22`.

#### Установка сервера и клиента

``` bash
sudo yum install -y clickhouse-server clickhouse-client
```

#### Запуск сервера

``` bash
sudo systemctl enable clickhouse-server
sudo systemctl start clickhouse-server
sudo systemctl status clickhouse-server
clickhouse-client # илм "clickhouse-client --password" если установлен пароль
```

Для использования наиболее свежих версий нужно заменить `stable` на `testing` (рекомендуется для тестовых окружений). Также иногда доступен `prestable`.

Для непосредственной установки пакетов необходимо выполнить следующие команды:

``` bash
sudo yum install clickhouse-server clickhouse-client
```

Также есть возможность установить пакеты вручную, скачав отсюда: https://packages.clickhouse.com/rpm/stable.

### Из tgz-архивов {#from-tgz-archives}

Команда ClickHouse рекомендует использовать предкомпилированные бинарники из `tgz`-архивов для всех дистрибутивов, где невозможна установка `deb`- и `rpm`- пакетов.

Интересующую версию архивов можно скачать вручную с помощью `curl` или `wget` из репозитория https://packages.clickhouse.com/tgz/.
После этого архивы нужно распаковать и воспользоваться скриптами установки. Пример установки самой свежей версии:

``` bash
LATEST_VERSION=$(curl -s https://packages.clickhouse.com/tgz/stable/ | \
    grep -Eo '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | sort -V -r | head -n 1)
export LATEST_VERSION

case $(uname -m) in
  x86_64) ARCH=amd64 ;;
  aarch64) ARCH=arm64 ;;
  *) echo "Unknown architecture $(uname -m)"; exit 1 ;;
esac

for PKG in clickhouse-common-static clickhouse-common-static-dbg clickhouse-server clickhouse-client
do
  curl -fO "https://packages.clickhouse.com/tgz/stable/$PKG-$LATEST_VERSION-${ARCH}.tgz" \
    || curl -fO "https://packages.clickhouse.com/tgz/stable/$PKG-$LATEST_VERSION.tgz"
done

tar -xzvf "clickhouse-common-static-$LATEST_VERSION-${ARCH}.tgz" \
  || tar -xzvf "clickhouse-common-static-$LATEST_VERSION.tgz"
sudo "clickhouse-common-static-$LATEST_VERSION/install/doinst.sh"

tar -xzvf "clickhouse-common-static-dbg-$LATEST_VERSION-${ARCH}.tgz" \
  || tar -xzvf "clickhouse-common-static-dbg-$LATEST_VERSION.tgz"
sudo "clickhouse-common-static-dbg-$LATEST_VERSION/install/doinst.sh"

tar -xzvf "clickhouse-server-$LATEST_VERSION-${ARCH}.tgz" \
  || tar -xzvf "clickhouse-server-$LATEST_VERSION.tgz"
sudo "clickhouse-server-$LATEST_VERSION/install/doinst.sh" configure
sudo /etc/init.d/clickhouse-server start

tar -xzvf "clickhouse-client-$LATEST_VERSION-${ARCH}.tgz" \
  || tar -xzvf "clickhouse-client-$LATEST_VERSION.tgz"
sudo "clickhouse-client-$LATEST_VERSION/install/doinst.sh"
```

Для продуктивных окружений рекомендуется использовать последнюю `stable`-версию. Её номер также можно найти на github с на вкладке https://github.com/ClickHouse/ClickHouse/tags c постфиксом `-stable`.

### Из Docker образа {#from-docker-image}

Для запуска ClickHouse в Docker нужно следовать инструкции на [Docker Hub](https://hub.docker.com/r/clickhouse/clickhouse-server/). Внутри образов используются официальные `deb`-пакеты.

### Из единого бинарного файла {#from-single-binary}

Для установки ClickHouse под Linux можно использовать единый переносимый бинарный файл из последнего коммита ветки `master`: [https://builds.clickhouse.com/master/amd64/clickhouse].

``` bash
curl -O 'https://builds.clickhouse.com/master/amd64/clickhouse' && chmod a+x clickhouse
sudo ./clickhouse install
```

### Из исполняемых файлов для нестандартных окружений {#from-binaries-non-linux}

Для других операционных систем и архитектуры AArch64 сборки ClickHouse предоставляются в виде кросс-компилированного бинарного файла из последнего коммита ветки `master` (с задержкой в несколько часов).

-   [macOS](https://builds.clickhouse.com/master/macos/clickhouse) — `curl -O 'https://builds.clickhouse.com/master/macos/clickhouse' && chmod a+x ./clickhouse`
-   [FreeBSD](https://builds.clickhouse.com/master/freebsd/clickhouse) — `curl -O 'https://builds.clickhouse.com/master/freebsd/clickhouse' && chmod a+x ./clickhouse`
-   [AArch64](https://builds.clickhouse.com/master/aarch64/clickhouse) — `curl -O 'https://builds.clickhouse.com/master/aarch64/clickhouse' && chmod a+x ./clickhouse`

После скачивания можно воспользоваться `clickhouse client` для подключения к серверу или `clickhouse local` для обработки локальных данных.

Чтобы установить ClickHouse в рамках всей системы (с необходимыми конфигурационными файлами, настройками пользователей и т. д.), выполните `sudo ./clickhouse install`. Затем выполните команды `clickhouse start` (чтобы запустить сервер) и `clickhouse-client` (чтобы подключиться к нему).

Данные сборки не рекомендуются для использования в рабочей среде, так как они недостаточно тщательно протестированы. Также в них присутствуют не все возможности ClickHouse.

### Из исходного кода {#from-sources}

Для компиляции ClickHouse вручную используйте инструкцию для [Linux](../development/build.mdx) или [Mac OS X](../development/build-osx.md).

Можно скомпилировать пакеты и установить их, либо использовать программы без установки пакетов. Также при ручой сборке можно отключить необходимость поддержки набора инструкций SSE 4.2 или собрать под процессоры архитектуры AArch64.

    Client: programs/clickhouse-client
    Server: programs/clickhouse-server

Для работы собранного вручную сервера необходимо создать каталоги для данных и метаданных, а также сделать сменить их владельца на желаемого пользователя (`chown`). Пути к этим каталогам могут быть изменены в конфигурационном файле сервера (src/programs/server/config.xml), по умолчанию используются следующие:

    /opt/clickhouse/data/default/
    /opt/clickhouse/metadata/default/

На Gentoo для установки ClickHouse из исходного кода можно использовать просто `emerge clickhouse`.

## Запуск {#zapusk}

Для запуска сервера в режиме демона, выполните:

``` bash
sudo service clickhouse-server start
```

Смотрите журналы в каталоге `/var/log/clickhouse-server/`.

Если сервер не стартует, проверьте корректность конфигурации в файле `/etc/clickhouse-server/config.xml`

Также можно запустить сервер вручную из консоли:

``` bash
clickhouse-server --config-file=/etc/clickhouse-server/config.xml
```

При этом журнал будет выводиться в консоль, что удобно для разработки.
Если конфигурационный файл лежит в текущем каталоге, то указывать параметр `--config-file` не требуется, по умолчанию будет использован файл `./config.xml`.

После запуска сервера соединиться с ним можно с помощью клиента командной строки:

``` bash
clickhouse-client
```

По умолчанию он соединяется с localhost:9000 от имени пользователя `default` без пароля. Также клиент может быть использован для соединения с удалённым сервером с помощью аргумента `--host`.

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

Для дальнейших экспериментов можно попробовать загрузить один из тестовых наборов данных или пройти [пошаговое руководство для начинающих](tutorial.md).
