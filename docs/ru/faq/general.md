# Общие вопросы {#obshchie-voprosy}

## Почему бы не использовать системы типа MapReduce? {#pochemu-by-ne-ispolzovat-sistemy-tipa-mapreduce}

Системами типа MapReduce будем называть системы распределённых вычислений, в которых операция reduce сделана на основе распределённой сортировки. Наиболее распространённым opensource решением данного класса является [Apache Hadoop](http://hadoop.apache.org). Яндекс использует собственное решение — YT.

Такие системы не подходят для онлайн запросов в силу слишком большой latency. То есть, не могут быть использованы в качестве бэкенда для веб-интерфейса.
Такие системы не подходят для обновления данных в реальном времени.
Распределённая сортировка не является оптимальным способом выполнения операции reduce, если результат выполнения операции и все промежуточные результаты, при их наличии, помещаются в оперативку на одном сервере, как обычно бывает в запросах, выполняющихся в режиме онлайн. В таком случае, оптимальным способом выполнения операции reduce является хэш-таблица. Частым способом оптимизации map-reduce задач является предагрегация (частичный reduce) с использованием хэш-таблицы в оперативной памяти. Эта оптимизация делается пользователем в ручном режиме.
Распределённая сортировка является основной причиной тормозов при выполнении несложных map-reduce задач.

Большинство реализаций MapReduce позволяют выполнять произвольный код на кластере. Но для OLAP задач лучше подходит декларативный язык запросов, который позволяет быстро проводить исследования. Для примера, для Hadoop существует Hive и Pig. Также смотрите Cloudera Impala, Shark (устаревший) для Spark, а также Spark SQL, Presto, Apache Drill. Впрочем, производительность при выполнении таких задач является сильно неоптимальной по сравнению со специализированными системами, а сравнительно высокая latency не позволяет использовать эти системы в качестве бэкенда для веб-интерфейса.

## Что делать, если у меня проблема с кодировками при использовании Oracle через ODBC? {#oracle-odbc-encodings}

Если вы используете Oracle через драйвер ODBC в качестве источника внешних словарей, необходимо задать правильное значение для переменной окружения `NLS_LANG` в `/etc/default/clickhouse`. Подробнее читайте в [Oracle NLS\_LANG FAQ](https://www.oracle.com/technetwork/products/globalization/nls-lang-099431.html).

**Пример**

``` sql
NLS_LANG=RUSSIAN_RUSSIA.UTF8
```

## Как экспортировать данные из ClickHouse в файл? {#how-to-export-to-file}

### Секция INTO OUTFILE {#sektsiia-into-outfile}

Добавьте секцию [INTO OUTFILE](../sql-reference/statements/select/into-outfile.md#into-outfile-clause) к своему запросу.

Например:

``` sql
SELECT * FROM table INTO OUTFILE 'file'
```

По умолчанию, для выдачи данных ClickHouse использует формат [TabSeparated](../interfaces/formats.md#tabseparated). Чтобы выбрать [формат данных](../interfaces/formats.md), используйте [секцию FORMAT](../sql-reference/statements/select/format.md#format-clause).

Например:

``` sql
SELECT * FROM table INTO OUTFILE 'file' FORMAT CSV
```

### Таблица с движком File {#tablitsa-s-dvizhkom-file}

Смотрите [File](../engines/table-engines/special/file.md).

### Перенаправление в командой строке {#perenapravlenie-v-komandoi-stroke}

``` sql
$ clickhouse-client --query "SELECT * from table" --format FormatName > result.txt
```

Смотрите [clickhouse-client](../interfaces/cli.md).

[Оригинальная статья](https://clickhouse.tech/docs/en/faq/general/) <!--hide-->
