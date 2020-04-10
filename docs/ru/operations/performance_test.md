---
machine_translated: true
machine_translated_rev: 1cd5f0028d917696daf71ac1c9ee849c99c1d5c8
---

# Как Протестировать Ваше Оборудование С Помощью ClickHouse {#how-to-test-your-hardware-with-clickhouse}

С помощью этой инструкции вы можете запустить базовый тест производительности ClickHouse на любом сервере без установки пакетов ClickHouse.

1.  Идти к «commits» страница: https://github.com/ClickHouse/ClickHouse/commits/master

2.  Нажмите на первую зеленую галочку или красный крест с зеленым цветом «ClickHouse Build Check» и нажмите на кнопку «Details» ссылка рядом «ClickHouse Build Check».

3.  Скопируйте ссылку на «clickhouse» двоичный код для amd64 или aarch64.

4.  ssh к серверу и скачать его с помощью wget:

<!-- -->

      # For amd64:
      wget https://clickhouse-builds.s3.yandex.net/0/00ba767f5d2a929394ea3be193b1f79074a1c4bc/1578163263_binary/clickhouse
      # For aarch64:
      wget https://clickhouse-builds.s3.yandex.net/0/00ba767f5d2a929394ea3be193b1f79074a1c4bc/1578161264_binary/clickhouse
      # Then do:
      chmod a+x clickhouse

1.  Скачать конфиги:

<!-- -->

      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.xml
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/users.xml
      mkdir config.d
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.d/path.xml -O config.d/path.xml
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.d/log_to_console.xml -O config.d/log_to_console.xml

1.  Скачать тест файлы:

<!-- -->

      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/benchmark-new.sh
      chmod a+x benchmark-new.sh
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/queries.sql

1.  Загрузите тестовые данные в соответствии с [Яндекс.Набор метрика](../getting_started/example_datasets/metrica.md) инструкция («hits» таблица, содержащая 100 миллионов строк).

<!-- -->

      wget https://clickhouse-datasets.s3.yandex.net/hits/partitions/hits_100m_obfuscated_v1.tar.xz
      tar xvf hits_100m_obfuscated_v1.tar.xz -C .
      mv hits_100m_obfuscated_v1/* .

1.  Запустите сервер:

<!-- -->

      ./clickhouse server

1.  Проверьте данные: ssh на сервер в другом терминале

<!-- -->

      ./clickhouse client --query "SELECT count() FROM hits_100m_obfuscated"
      100000000

1.  Отредактируйте текст benchmark-new.sh, изменение «clickhouse-client» к «./clickhouse client» и добавить «–max\_memory\_usage 100000000000» параметр.

<!-- -->

      mcedit benchmark-new.sh

1.  Выполнить тест:

<!-- -->

      ./benchmark-new.sh hits_100m_obfuscated

1.  Отправьте номера и информацию о конфигурации вашего оборудования по адресу clickhouse-feedback@yandex-team.com

Все результаты опубликованы здесь: https://clickhouse-да.технология / benchmark\_hardware.HTML
