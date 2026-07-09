---
description: 'Табличная функция `executable` создает таблицу на основе вывода пользовательской функции (UDF), которую вы определяете в скрипте, выводящем строки в **stdout**.'
keywords: ['udf', 'пользовательская функция', 'ClickHouse', 'executable', 'таблица', 'функция']
sidebar_label: 'executable'
sidebar_position: 50
slug: /engines/table-functions/executable
title: 'executable'
doc_type: 'reference'
---

Табличная функция `executable` создает таблицу на основе вывода пользовательской функции (UDF), которую вы определяете в скрипте, выводящем строки в **stdout**. Исполняемый скрипт хранится в каталоге `users_scripts` и может читать данные из любого источника. Убедитесь, что на вашем ClickHouse server установлены все необходимые пакеты для запуска исполняемого скрипта. Например, если это скрипт на Python, убедитесь, что на server установлены необходимые пакеты Python.

При желании можно указать один или несколько входных запросов, которые передают свои результаты в **stdin**, чтобы скрипт мог их читать.

:::note
Ключевое преимущество табличной функции `executable` и движка таблицы `Executable` по сравнению с обычными функциями UDF заключается в том, что обычные функции UDF не могут изменять количество строк. Например, если на вход подается 100 строк, то результат также должен содержать 100 строк. При использовании табличной функции `executable` или движка таблицы `Executable` ваш скрипт может выполнять любые преобразования данных, включая сложные агрегации.
:::

<div id="syntax">
  ## Синтаксис
</div>

Табличная функция `executable` требует указания трёх параметров и принимает необязательный список входных запросов:

```sql
executable(script_name, format, structure, [input_query...] [,SETTINGS ...])
```

* `script_name`: имя файла скрипта; сохраняется в папке `user_scripts` (папка по умолчанию для настройки `user_scripts_path`)
* `format`: формат создаваемой таблицы
* `structure`: схема создаваемой таблицы
* `input_query`: необязательный запрос (или коллекция, или запросы), результаты которого передаются скрипту через **stdin**

:::note
Если вы собираетесь многократно вызывать один и тот же скрипт с одними и теми же входными запросами, рассмотрите возможность использования [движка таблицы `Executable`](../../engines/table-engines/special/executable.md).
:::

Следующий скрипт Python называется `generate_random.py` и сохранён в папке `user_scripts`. Он считывает число `i` и выводит `i` случайных строк, перед каждой из которых выводится число, отделённое символом табуляции:

```python
#!/usr/local/bin/python3.9

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

Давайте запустим скрипт, чтобы он сгенерировал 10 случайных строк:

```sql
SELECT * FROM executable('generate_random.py', TabSeparated, 'id UInt32, random String', (SELECT 10))
```

Ответ выглядит так:

```response
┌─id─┬─random─────┐
│  0 │ xheXXCiSkH │
│  1 │ AqxvHAoTrl │
│  2 │ JYvPCEbIkY │
│  3 │ sWgnqJwGRm │
│  4 │ fTZGrjcLon │
│  5 │ ZQINGktPnd │
│  6 │ YFSvGGoezb │
│  7 │ QyMJJZOOia │
│  8 │ NfiyDDhmcI │
│  9 │ REJRdJpWrg │
└────┴────────────┘
```

<div id="settings">
  ## Настройки
</div>

* `send_chunk_header` - управляет тем, отправать ли количество строк перед отправкой фрагмента данных на обработку. Значение по умолчанию — `false`.
* `pool_size` — размер пула. Если для `pool_size` указано значение 0, ограничения на размер пула отсутствуют. Значение по умолчанию — `16`.
* `max_command_execution_time` — максимальное время выполнения команды исполняемого скрипта при обработке блока данных. Указывается в секундах. Значение по умолчанию — 10.
* `command_termination_timeout` — исполняемый скрипт должен содержать основной цикл с возможностью чтения и записи. После уничтожения табличной функции пайп закрывается, и исполняемый файл получает `command_termination_timeout` секунд на завершение работы, прежде чем ClickHouse отправит дочернему процессу сигнал SIGTERM. Указывается в секундах. Значение по умолчанию — 10.
* `command_read_timeout` - тайм-аут чтения данных из stdout команды в миллисекундах. Значение по умолчанию — 10000.
* `command_write_timeout` - тайм-аут записи данных в stdin команды в миллисекундах. Значение по умолчанию — 10000.

<div id="passing-query-results-to-a-script">
  ## Передача результатов запроса в скрипт
</div>

Обязательно ознакомьтесь с примером в разделе о движке таблицы `Executable`, где показано, [как передавать результаты запроса в скрипт](../../engines/table-engines/special/executable.md#passing-query-results-to-a-script). Вот как выполнить тот же скрипт из этого примера с помощью табличной функции `executable`:

```sql
SELECT * FROM executable(
    'sentiment.py',
    TabSeparated,
    'id UInt64, sentiment Float32',
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```