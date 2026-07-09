---
description: 'Движки таблиц `Executable` и `ExecutablePool` позволяют определить
  таблицу, строки которой генерируются заданным вами скриптом (за счёт записи строк
  в **stdout**).'
sidebar_label: 'Executable/ExecutablePool'
sidebar_position: 40
slug: /engines/table-engines/special/executable
title: 'Движки таблиц `Executable` и `ExecutablePool`'
doc_type: 'reference'
---

Движки таблиц `Executable` и `ExecutablePool` позволяют определить таблицу, строки которой генерируются заданным вами скриптом (за счёт записи строк в **stdout**). Исполняемый скрипт хранится в каталоге `user_scripts` и может читать данные из любого источника.

* Таблицы `Executable`: скрипт запускается для каждого запроса
* Таблицы `ExecutablePool`: поддерживают пул постоянных процессов и используют процессы из пула для чтения

При необходимости можно указать один или несколько входных запросов, результаты которых передаются в **stdin**, откуда их читает скрипт.

<div id="creating-an-executable-table">
  ## Создание таблицы `Executable`
</div>

Для движка таблицы `Executable` требуются два параметра: имя скрипта и формат входных данных. При необходимости можно также передать один или несколько входных запросов:

```sql
Executable(script_name, format, [input_query...])
```

Вот соответствующие настройки для таблицы `Executable`:

* `send_chunk_header`
  * Описание: Перед отправкой фрагмента на обработку передавать количество строк в каждом фрагменте. Эта настройка помогает писать скрипт эффективнее, заранее выделяя необходимые ресурсы
  * Значение по умолчанию: false
* `command_termination_timeout`
  * Описание: Тайм-аут завершения команды в секундах
  * Значение по умолчанию: 10
* `command_read_timeout`
  * Описание: Тайм-аут чтения данных из stdout команды в миллисекундах
  * Значение по умолчанию: 10000
* `command_write_timeout`
  * Описание: Тайм-аут записи данных в stdin команды в миллисекундах
  * Значение по умолчанию: 10000

Рассмотрим пример. Следующий скрипт Python называется `my_script.py` и сохраняется в папке `user_scripts`. Он считывает число `i` и выводит `i` случайных строк, при этом перед каждой строкой выводится число, отделённое символом табуляции:

```python
#!/usr/bin/python3

import sys
import string
import random

def main():

    # Read input value
    for number in sys.stdin:
        i = int(number)

        # Generate some random rows
        for id in range(0, i):
            letters = string.ascii_letters
            random_string =  ''.join(random.choices(letters ,k=10))
            print(str(id) + '\t' + random_string + '\n', end='')

        # Flush results to stdout
        sys.stdout.flush()

if __name__ == "__main__":
    main()
```

Следующая таблица `my_executable_table` строится на основе вывода `my_script.py`, который генерирует 10 случайных строк каждый раз, когда вы выполняете `SELECT` из `my_executable_table`:

```sql
CREATE TABLE my_executable_table (
   x UInt32,
   y String
)
ENGINE = Executable('my_script.py', TabSeparated, (SELECT 10))
```

Создание таблицы происходит мгновенно и не запускает скрипт. При выполнении запроса к `my_executable_table` скрипт запускается:

```sql
SELECT * FROM my_executable_table
```

```response
┌─x─┬─y──────────┐
│ 0 │ BsnKBsNGNH │
│ 1 │ mgHfBCUrWM │
│ 2 │ iDQAVhlygr │
│ 3 │ uNGwDuXyCk │
│ 4 │ GcFdQWvoLB │
│ 5 │ UkciuuOTVO │
│ 6 │ HoKeCdHkbs │
│ 7 │ xRvySxqAcR │
│ 8 │ LKbXPHpyDI │
│ 9 │ zxogHTzEVV │
└───┴────────────┘
```

<div id="passing-query-results-to-a-script">
  ## Передача результатов запроса в скрипт
</div>

Пользователи сайта Hacker News оставляют комментарии. В Python есть библиотека для обработки естественного языка (`nltk`) с `SentimentIntensityAnalyzer`, который позволяет определять, являются ли комментарии положительными, отрицательными или нейтральными, а также присваивать им значение от -1 (очень отрицательный комментарий) до 1 (очень положительный комментарий). Давайте создадим таблицу `Executable`, которая будет вычислять тональность комментариев Hacker News с помощью `nltk`.

В этом примере используется таблица `hackernews`, описанная [здесь](/ru/engines/table-engines/mergetree-family/textindexes/#hacker-news-dataset). Таблица `hackernews` содержит столбец `id` типа `UInt64` и столбец `comment` типа `String`. Начнём с определения таблицы `Executable`:

```sql
CREATE TABLE sentiment (
   id UInt64,
   sentiment Float32
)
ENGINE = Executable(
    'sentiment.py',
    TabSeparated,
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```

Несколько замечаний о таблице `sentiment`:

* Файл `sentiment.py` сохраняется в папке `user_scripts` (это папка по умолчанию для настройки `user_scripts_path`)
* Формат `TabSeparated` означает, что наш скрипт Python должен генерировать строки необработанных данных со значениями, разделёнными табуляцией
* Запрос выбирает два столбца из `hackernews`. Скрипту Python потребуется извлечь значения этих столбцов из входящих строк

Вот определение `sentiment.py`:

```python
#!/usr/local/bin/python3.9

import sys
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

def main():
    sentiment_analyzer = SentimentIntensityAnalyzer()

    while True:
        try:
            row = sys.stdin.readline()
            if row == '':
                break

            split_line = row.split("\t")

            id = str(split_line[0])
            comment = split_line[1]

            score = sentiment_analyzer.polarity_scores(comment)['compound']
            print(id + '\t' + str(score) + '\n', end='')
            sys.stdout.flush()
        except BaseException as x:
            break

if __name__ == "__main__":
    main()
```

Несколько комментариев о нашем скрипте на Python:

* Чтобы это работало, нужно выполнить `nltk.downloader.download('vader_lexicon')`. Это можно было бы добавить в скрипт, но тогда загрузка выполнялась бы каждый раз при выполнении запроса к таблице `sentiment`, что неэффективно
* Каждое значение `row` будет представлять собой строку в результирующем наборе `SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20`
* Входящая строка имеет формат с разделением табуляцией, поэтому мы разбираем `id` и `comment` с помощью Python-функции `split`
* Результат `polarity_scores` — это объект JSON с несколькими значениями. Мы решили просто взять значение `compound` из этого объекта JSON
* Помните, что таблица `sentiment` в ClickHouse использует формат `TabSeparated` и содержит два столбца, поэтому наша функция `print` разделяет эти столбцы символом табуляции

Каждый раз, когда вы пишете запрос, выбирающий строки из таблицы `sentiment`, выполняется запрос `SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20`, а результат передаётся в `sentiment.py`. Давайте проверим:

```sql
SELECT *
FROM sentiment
```

Ответ будет выглядеть так:

```response
┌───────id─┬─sentiment─┐
│  7398199 │    0.4404 │
│ 21640317 │    0.1779 │
│ 21462000 │         0 │
│ 25168863 │         0 │
│ 25168978 │   -0.1531 │
│ 25169359 │         0 │
│ 25169394 │   -0.9231 │
│ 25169766 │    0.4137 │
│ 25172570 │    0.7469 │
│ 25173687 │    0.6249 │
│ 28291534 │         0 │
│ 28291669 │   -0.4767 │
│ 28291731 │         0 │
│ 28291949 │   -0.4767 │
│ 28292004 │    0.3612 │
│ 28292050 │    -0.296 │
│ 28292322 │         0 │
│ 28295172 │    0.7717 │
│ 28295288 │    0.4404 │
│ 21465723 │   -0.6956 │
└──────────┴───────────┘
```

<div id="creating-an-executablepool-table">
  ## Создание таблицы `ExecutablePool`
</div>

Синтаксис `ExecutablePool` аналогичен `Executable`, но у таблицы `ExecutablePool` есть несколько важных настроек, характерных только для неё:

* `pool_size`
  * Описание: Размер пула процессов. Если значение равно 0, ограничения на размер отсутствуют
  * Значение по умолчанию: 16
* `max_command_execution_time`
  * Описание: Максимальное время выполнения команды в секундах
  * Значение по умолчанию: 10

Мы можем легко преобразовать приведённую выше таблицу `sentiment`, чтобы использовать `ExecutablePool` вместо `Executable`:

```sql
CREATE TABLE sentiment_pooled (
   id UInt64,
   sentiment Float32
)
ENGINE = ExecutablePool(
    'sentiment.py',
    TabSeparated,
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20000)
)
SETTINGS
    pool_size = 4;
```

ClickHouse будет по мере необходимости поддерживать 4 процесса, когда ваш клиент выполняет запросы к таблице `sentiment_pooled`.