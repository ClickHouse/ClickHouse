---
description: 'Документация по пользовательской функции (UDF)'
sidebar_label: 'UDF'
slug: /sql-reference/functions/udf
title: 'Пользовательская функция (UDF)'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="udfs-user-defined-functions">
  # пользовательская функция (UDF) Пользовательские функции
</div>

ClickHouse поддерживает несколько типов пользовательских функций (UDF):

* [Исполняемые UDF](#executable-user-defined-functions) запускают внешнюю программу или скрипт (Python, Bash и т. д.) и передают ей блоки данных через STDIN / STDOUT. Используйте их для интеграции существующего кода или инструментов без перекомпиляции ClickHouse. По сравнению с вариантами, выполняемыми в том же процессе, у них выше накладные расходы на каждый вызов, поэтому они лучше подходят для более тяжёлой логики или случаев, когда требуется другая среда выполнения.
* [SQL UDF](#sql-user-defined-functions) определяются с помощью `CREATE FUNCTION` исключительно на SQL. Они встраиваются/разворачиваются в план запроса (без границы между процессами), что делает их легковесными и хорошо подходящими для повторного использования логики выражений или упрощения сложных вычисляемых столбцов.
* [Экспериментальные WebAssembly UDF](#webassembly-user-defined-functions) выполняют код, скомпилированный в WebAssembly, внутри песочницы в процессе сервера. Они обеспечивают меньшие накладные расходы на каждый вызов, чем внешние исполняемые программы, и лучшую изоляцию, чем нативные расширения, поэтому подходят для пользовательских алгоритмов, написанных на языках, которые можно компилировать в WASM (например, C/C++/Rust).
* [Экспериментальные исполняемые UDF на базе драйверов](#driver-based-executable-user-defined-functions) позволяют драйверу, предоставленному оператором, преобразовать фрагмент кода, указанный в `CREATE FUNCTION ... ENGINE = DriverName(...) AS '...'`, в исполняемый UDF при создании функции (например, путём компиляции). Они основаны на исполняемых UDF и требуют серверной настройки драйвера.

<div id="executable-user-defined-functions">
  ## Исполняемые пользовательские функции
</div>

<BetaBadge />

:::note
В ClickHouse Cloud исполняемые UDF доступны в публичной бете и создаются через интерфейс консоли Cloud. См. [Пользовательские функции в Cloud](/ru/cloud/features/user-defined-functions) для Cloud-специфичного процесса.
:::

ClickHouse может вызывать любую внешнюю исполняемую программу или script для обработки данных.

Конфигурация исполняемых пользовательских функций может находиться в одном или нескольких XML-файлах.
Путь к конфигурации указывается в параметре [`user_defined_executable_functions_config`](../../operations/server-configuration-parameters/settings.md#user_defined_executable_functions_config).

Конфигурация функции содержит следующие настройки:

| Параметр                      | Описание                                                                                                                                                                                                                                                                                                                                                                                                                                  | Обязательный   | Значение по умолчанию     |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- | ------------------------- |
| `name`                        | Имя функции                                                                                                                                                                                                                                                                                                                                                                                                                               | Да             | -                         |
| `command`                     | Имя скрипта для выполнения или команда, если `execute_direct` имеет значение false                                                                                                                                                                                                                                                                                                                                                        | Да             | -                         |
| `argument`                    | Описание аргумента с `type` и необязательным `name`. Каждый аргумент описывается в отдельной настройке. Указание имени необходимо, если имена аргументов входят в сериализацию формата пользовательской функции, например [Native](/ru/interfaces/formats/Native) или [JSONEachRow](/ru/interfaces/formats/JSONEachRow)                                                                                                                         | Да             | `c` + argument&#95;number |
| `format`                      | [Формат](../../interfaces/formats.md), в котором аргументы передаются команде. Ожидается, что вывод команды также будет в этом формате                                                                                                                                                                                                                                                                                                    | Да             | -                         |
| `return_type`                 | Тип возвращаемого значения                                                                                                                                                                                                                                                                                                                                                                                                                | Да             | -                         |
| `return_name`                 | Имя возвращаемого значения. Указание этого имени необходимо, если оно входит в сериализацию формата пользовательской функции, например [Native](/ru/interfaces/formats/Native) или [JSONEachRow](/ru/interfaces/formats/JSONEachRow)                                                                                                                                                                                                            | Необязательный | `result`                  |
| `type`                        | Тип исполняемой функции. Если `type` имеет значение `executable`, запускается одна команда. Если установлено значение `executable_pool`, создаётся пул команд                                                                                                                                                                                                                                                                             | Да             | -                         |
| `max_command_execution_time`  | Максимальное время выполнения в секундах для обработки блока данных. Эта настройка применяется только к командам `executable_pool`                                                                                                                                                                                                                                                                                                        | Необязательный | `10`                      |
| `command_termination_timeout` | Время в секундах, в течение которого команда должна завершиться после закрытия её канала. По истечении этого времени процессу, выполняющему команду, отправляется `SIGTERM`                                                                                                                                                                                                                                                               | Необязательный | `10`                      |
| `command_read_timeout`        | Тайм-аут чтения данных из `stdout` команды в миллисекундах                                                                                                                                                                                                                                                                                                                                                                                | Необязательный | `10000`                   |
| `command_write_timeout`       | Тайм-аут записи данных в `stdin` команды в миллисекундах                                                                                                                                                                                                                                                                                                                                                                                  | Необязательный | `10000`                   |
| `pool_size`                   | Размер пула команд                                                                                                                                                                                                                                                                                                                                                                                                                        | Необязательный | `16`                      |
| `send_chunk_header`           | Определяет, отправлять ли количество строк перед отправкой фрагмента данных в процесс                                                                                                                                                                                                                                                                                                                                                     | Необязательный | `false`                   |
| `execute_direct`              | Если `execute_direct` = `1`, `command` будет искаться в папке user&#95;scripts, указанной в [user&#95;scripts&#95;path](../../operations/server-configuration-parameters/settings.md#user_scripts_path). Дополнительные аргументы скрипта можно указать, разделяя их пробелами. Пример: `script_name arg1 arg2`. Если `execute_direct` = `0`, `command` передаётся как аргумент в `bin/sh -c`                                             | Необязательный | `1`                       |
| `lifetime`                    | Интервал перезагрузки функции в секундах. Если установлено значение `0`, функция не перезагружается                                                                                                                                                                                                                                                                                                                                       | Необязательный | `0`                       |
| `deterministic`               | Является ли функция детерминированной (возвращает одинаковый результат для одинаковых входных данных)                                                                                                                                                                                                                                                                                                                                     | Необязательный | `false`                   |
| `stderr_reaction`             | Как обрабатывать вывод команды в stderr. Значения: `none` (игнорировать), `log` (сразу записывать весь stderr в log), `log_first` (записать первые 4 KiB после завершения), `log_last` (записать последние 4 KiB после завершения), `throw` (немедленно сгенерировать исключение при любом выводе в stderr). При использовании `log_first` или `log_last` с ненулевым кодом выхода содержимое stderr включается в сообщение об исключении | Необязательный | `log_last`                |
| `check_exit_code`             | Если true, ClickHouse будет проверять код завершения команды. Ненулевой код завершения вызывает исключение                                                                                                                                                                                                                                                                                                                                | Необязательный | `true`                    |

Команда должна читать аргументы из `STDIN` и выводить результат в `STDOUT`. Команда должна обрабатывать аргументы итеративно. То есть после обработки одного фрагмента аргументов она должна ждать следующий фрагмент.

<div id="executable-user-defined-functions">
  ## Исполняемые пользовательские функции
</div>

<div id="examples">
  ## Примеры
</div>

<div id="udf-inline">
  ### UDF из встроенного скрипта
</div>

Создайте `test_function_sum`, вручную задав `execute_direct` значение `0`, используя конфигурацию XML или YAML.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Файл `test_function.xml` (`/etc/clickhouse-server/test_function.xml` при использовании пути по умолчанию).

    ```xml title="/etc/clickhouse-server/test_function.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_function_sum</name>
            <return_type>UInt64</return_type>
            <argument>
                <type>UInt64</type>
                <name>lhs</name>
            </argument>
            <argument>
                <type>UInt64</type>
                <name>rhs</name>
            </argument>
            <format>TabSeparated</format>
            <command>cd /; clickhouse-local --input-format TabSeparated --output-format TabSeparated --structure 'x UInt64, y UInt64' --query "SELECT x + y FROM table"</command>
            <execute_direct>0</execute_direct>
            <deterministic>true</deterministic>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Файл `test_function.yaml` (`/etc/clickhouse-server/test_function.yaml` при использовании пути по умолчанию).

    ```yml title="/etc/clickhouse-server/test_function.yaml"
    functions:
      type: executable
      name: test_function_sum
      return_type: UInt64
      argument:
        - type: UInt64
          name: lhs
        - type: UInt64
          name: rhs
      format: TabSeparated
      command: 'cd /; clickhouse-local --input-format TabSeparated --output-format TabSeparated --structure ''x UInt64, y UInt64'' --query "SELECT x + y FROM table"'
      execute_direct: 0
      deterministic: true
    ```
  </TabItem>
</Tabs>

<br />

```sql title="Query"
SELECT test_function_sum(2, 2);
```

```text title="Result"
┌─test_function_sum(2, 2)─┐
│                       4 │
└─────────────────────────┘
```

<div id="udf-python">
  ### UDF из скрипта Python
</div>

В этом примере мы создаем UDF, которая считывает значение из `STDIN` и возвращает его в виде строки.

Создайте `test_function`, используя конфигурацию XML или YAML.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Файл `test_function.xml` (`/etc/clickhouse-server/test_function.xml` при настройках пути по умолчанию).

    ```xml title="/etc/clickhouse-server/test_function.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_function_python</name>
            <return_type>String</return_type>
            <argument>
                <type>UInt64</type>
                <name>value</name>
            </argument>
            <format>TabSeparated</format>
            <command>test_function.py</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Файл `test_function.yaml` (`/etc/clickhouse-server/test_function.yaml` при настройках пути по умолчанию).

    ```yml title="/etc/clickhouse-server/test_function.yaml"
    functions:
      type: executable
      name: test_function_python
      return_type: String
      argument:
        - type: UInt64
          name: value
      format: TabSeparated
      command: test_function.py
    ```
  </TabItem>
</Tabs>

<br />

Создайте файл скрипта `test_function.py` в папке `user_scripts` (`/var/lib/clickhouse/user_scripts/test_function.py` при настройках пути по умолчанию).

```python
#!/usr/bin/python3

import sys

if __name__ == '__main__':
    for line in sys.stdin:
        print("Value " + line, end='')
        sys.stdout.flush()
```

```sql title="Query"
SELECT test_function_python(toUInt64(2));
```

```text title="Result"
┌─test_function_python(2)─┐
│ Value 2                 │
└─────────────────────────┘
```

<div id="udf-stdin">
  ### Считать два значения из `STDIN` и вернуть их сумму как объект JSON
</div>

Создайте `test_function_sum_json` с именованными аргументами и форматом [JSONEachRow](/ru/interfaces/formats/JSONEachRow), используя конфигурацию XML или YAML.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Файл `test_function.xml` (`/etc/clickhouse-server/test_function.xml` при настройках путей по умолчанию).

    ```xml title="/etc/clickhouse-server/test_function.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_function_sum_json</name>
            <return_type>UInt64</return_type>
            <return_name>result_name</return_name>
            <argument>
                <type>UInt64</type>
                <name>argument_1</name>
            </argument>
            <argument>
                <type>UInt64</type>
                <name>argument_2</name>
            </argument>
            <format>JSONEachRow</format>
            <command>test_function_sum_json.py</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Файл `test_function.yaml` (`/etc/clickhouse-server/test_function.yaml` при настройках путей по умолчанию).

    ```yml title="/etc/clickhouse-server/test_function.yaml"
    functions:
      type: executable
      name: test_function_sum_json
      return_type: UInt64
      return_name: result_name
      argument:
        - type: UInt64
          name: argument_1
        - type: UInt64
          name: argument_2
      format: JSONEachRow
      command: test_function_sum_json.py
    ```
  </TabItem>
</Tabs>

<br />

Создайте файл скрипта `test_function_sum_json.py` в каталоге `user_scripts` (`/var/lib/clickhouse/user_scripts/test_function_sum_json.py` при настройках путей по умолчанию).

```python
#!/usr/bin/python3

import sys
import json

if __name__ == '__main__':
    for line in sys.stdin:
        value = json.loads(line)
        first_arg = int(value['argument_1'])
        second_arg = int(value['argument_2'])
        result = {'result_name': first_arg + second_arg}
        print(json.dumps(result), end='\n')
        sys.stdout.flush()
```

```sql title="Query"
SELECT test_function_sum_json(2, 2);
```

```text title="Result"
┌─test_function_sum_json(2, 2)─┐
│                            4 │
└──────────────────────────────┘
```

<div id="udf-parameters-in-command">
  ### Использование параметров в настройке `command`
</div>

Исполняемые пользовательские функции могут принимать константные параметры, заданные в настройке `command` (это работает только для пользовательских функций типа `executable`).
Также требуется опция `execute_direct`, чтобы избежать уязвимости, связанной с подстановкой аргументов оболочкой.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Файл `test_function_parameter_python.xml` (`/etc/clickhouse-server/test_function_parameter_python.xml` при настройках путей по умолчанию).

    ```xml title="/etc/clickhouse-server/test_function_parameter_python.xml"
    <functions>
        <function>
            <type>executable</type>
            <execute_direct>true</execute_direct>
            <name>test_function_parameter_python</name>
            <return_type>String</return_type>
            <argument>
                <type>UInt64</type>
            </argument>
            <format>TabSeparated</format>
            <command>test_function_parameter_python.py {test_parameter:UInt64}</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Файл `test_function_parameter_python.yaml` (`/etc/clickhouse-server/test_function_parameter_python.yaml` при настройках путей по умолчанию).

    ```yml title="/etc/clickhouse-server/test_function_parameter_python.yaml"
    functions:
      type: executable
      execute_direct: true
      name: test_function_parameter_python
      return_type: String
      argument:
        - type: UInt64
      format: TabSeparated
      command: test_function_parameter_python.py {test_parameter:UInt64}
    ```
  </TabItem>
</Tabs>

<br />

Создайте файл скрипта `test_function_parameter_python.py` в каталоге `user_scripts` (`/var/lib/clickhouse/user_scripts/test_function_parameter_python.py` при настройках путей по умолчанию).

```python
#!/usr/bin/python3

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        print("Parameter " + str(sys.argv[1]) + " value " + str(line), end="")
        sys.stdout.flush()
```

```sql title="Query"
SELECT test_function_parameter_python(1)(2);
```

```text title="Result"
┌─test_function_parameter_python(1)(2)─┐
│ Parameter 1 value 2                  │
└──────────────────────────────────────┘
```

<div id="udf-shell-script">
  ### UDF из shell-скрипта
</div>

В этом примере мы создаём shell-скрипт, который умножает каждое значение на 2.

<Tabs>
  <TabItem value="XML" label="XML" default>
    Файл `test_function_shell.xml` (`/etc/clickhouse-server/test_function_shell.xml` при стандартных настройках пути).

    ```xml title="/etc/clickhouse-server/test_function_shell.xml"
    <functions>
        <function>
            <type>executable</type>
            <name>test_shell</name>
            <return_type>String</return_type>
            <argument>
                <type>UInt8</type>
                <name>value</name>
            </argument>
            <format>TabSeparated</format>
            <command>test_shell.sh</command>
        </function>
    </functions>
    ```
  </TabItem>

  <TabItem value="YAML" label="YAML">
    Файл `test_function_shell.yaml` (`/etc/clickhouse-server/test_function_shell.yaml` при стандартных настройках пути).

    ```yml title="/etc/clickhouse-server/test_function_shell.yaml"
    functions:
      type: executable
      name: test_shell
      return_type: String
      argument:
        - type: UInt8
          name: value
      format: TabSeparated
      command: test_shell.sh
    ```
  </TabItem>
</Tabs>

<br />

Создайте файл скрипта `test_shell.sh` в папке `user_scripts` (`/var/lib/clickhouse/user_scripts/test_shell.sh` при стандартных настройках пути).

```bash title="/var/lib/clickhouse/user_scripts/test_shell.sh"
#!/bin/bash

while read read_data;
    do printf "$(expr $read_data \* 2)\n";
done
```

```sql title="Query"
SELECT test_shell(number) FROM numbers(10);
```

```text title="Result"
    ┌─test_shell(number)─┐
 1. │ 0                  │
 2. │ 2                  │
 3. │ 4                  │
 4. │ 6                  │
 5. │ 8                  │
 6. │ 10                 │
 7. │ 12                 │
 8. │ 14                 │
 9. │ 16                 │
10. │ 18                 │
    └────────────────────┘
```

<div id="error-handling">
  ## Обработка ошибок
</div>

Некоторые функции могут сгенерировать исключение, если данные некорректны.
В этом случае запрос отменяется, а клиенту возвращается текст ошибки.
При распределённой обработке, если на одном из серверов возникает исключение, остальные серверы также пытаются прервать запрос.

<div id="evaluation-of-argument-expressions">
  ## Оценка выражений аргументов
</div>

Почти во всех языках программирования для некоторых операторов один из аргументов может не вычисляться.
Как правило, это операторы `&&`, `||` и `?:`.
В ClickHouse аргументы функций (операторов) вычисляются всегда.
Это связано с тем, что вычисление выполняется сразу для целых частей столбцов, а не для каждой строки по отдельности.

<div id="performing-functions-for-distributed-query-processing">
  ## Выполнение функций при распределённой обработке запросов
</div>

При распределённой обработке запросов максимально возможное число этапов обработки запроса выполняется на удалённых серверах, а остальные этапы (слияние промежуточных результатов и всё, что следует за ним) — на сервере-инициаторе запроса.

Это означает, что функции могут выполняться на разных серверах.
Например, в запросе `SELECT f(sum(g(x))) FROM distributed_table GROUP BY h(y),`

* если `distributed_table` имеет как минимум два сегмента, функции &#39;g&#39; и &#39;h&#39; выполняются на удалённых серверах, а функция &#39;f&#39; — на сервере-инициаторе запроса.
* если `distributed_table` имеет только один сегмент, все функции &#39;f&#39;, &#39;g&#39; и &#39;h&#39; выполняются на сервере этого сегмента.

Результат функции обычно не зависит от того, на каком сервере она выполняется. Однако иногда это важно.
Например, функции, работающие со словарями, используют словарь, доступный на том сервере, где они выполняются.
Другой пример — функция `hostName`, которая возвращает имя сервера, на котором выполняется, чтобы можно было делать `GROUP BY` по серверам в запросе `SELECT`.

Если функция в запросе выполняется на сервере-инициаторе запроса, но вам нужно выполнить её на удалённых серверах, вы можете обернуть её в агрегатную функцию &#39;any&#39; или добавить в ключ `GROUP BY`.

<div id="sql-user-defined-functions">
  ## Пользовательские функции SQL
</div>

Пользовательские функции на основе lambda-выражений можно создавать с помощью оператора [CREATE FUNCTION](../statements/create/function.md). Чтобы удалить эти функции, используйте оператор [DROP FUNCTION](../statements/drop.md#drop-function).

<div id="webassembly-user-defined-functions">
  ## Пользовательские функции WebAssembly
</div>

<CloudNotSupportedBadge />

<ExperimentalBadge />

Пользовательские функции WebAssembly (WASM UDF) позволяют выполнять пользовательский код, скомпилированный в WebAssembly, внутри процесса сервера ClickHouse.

<div id="quick-start">
  ### Быстрый старт
</div>

Включите экспериментальную поддержку WebAssembly в конфигурации ClickHouse:

```xml
<clickhouse>
    <allow_experimental_webassembly_udf>true</allow_experimental_webassembly_udf>
</clickhouse>
```

Вставьте скомпилированный модуль WASM в системную таблицу:

```sql
INSERT INTO system.webassembly_modules (name, code)
SELECT 'my_module', base64Decode('AGFzbQEAAAA...');
```

Создайте функцию с помощью вашего WASM-модуля:

```sql
CREATE FUNCTION my_function
LANGUAGE WASM
ABI ROW_DIRECT
FROM 'my_module'
ARGUMENTS (x UInt32, y UInt32)
RETURNS UInt32;
```

Используйте функцию в запросах:

```sql
SELECT my_function(10, 20);
```

<div id="more-information">
  ### Дополнительная информация
</div>

Подробную информацию см. в документации о [пользовательских функциях WebAssembly](wasm_udf.md).

<div id="driver-based-executable-user-defined-functions">
  ## Исполняемые пользовательские функции на основе драйверов
</div>

<CloudNotSupportedBadge />

<ExperimentalBadge />

:::note
Это экспериментальная возможность, которая в будущих релизах может измениться с нарушением обратной совместимости. Включите её с помощью настройки сервера [`allow_experimental_executable_udf_drivers`](../../operations/server-configuration-parameters/settings.md#allow_experimental_executable_udf_drivers).
:::

*Драйвер* — это предоставляемый оператором адаптер, который превращает фрагмент пользовательского кода в запускаемый [исполняемый UDF](#executable-user-defined-functions). Когда функция создаётся с `ENGINE = DriverName(...)`, ClickHouse запускает `create_command` драйвера, передавая ему сигнатуру функции и тело кода; драйвер компилирует или иным образом обрабатывает это тело и выводит конфигурацию исполняемого UDF, которую ClickHouse затем сохраняет и загружает.

Это позволяет администраторам предоставить пользователям безопасный и ограниченный способ определять функции на произвольном языке (например, на C, компилируемом внутри изолированного контейнера), не предоставляя им доступа к файлам конфигурации сервера или файловой системе. Набор доступных драйверов полностью контролируется оператором.

<div id="enabling-drivers">
  ### Включение драйверов
</div>

Исполняемые UDF на базе драйверов по умолчанию отключены. Чтобы включить их:

1. Установите экспериментальный флаг в конфигурации сервера:

   ```xml
   <clickhouse>
       <allow_experimental_executable_udf_drivers>true</allow_experimental_executable_udf_drivers>
   </clickhouse>
   ```

2. Укажите в [`user_defined_executable_function_drivers_config`](../../operations/server-configuration-parameters/settings.md#user_defined_executable_function_drivers_config) путь к одному или нескольким файлам конфигурации драйверов (поддерживается glob-шаблон) и при необходимости задайте [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path) — каталог, в котором хранятся сгенерированные конфигурации исполняемых UDF:

   ```xml
   <clickhouse>
       <user_defined_executable_function_drivers_config>user_defined_executable_function_drivers_config.d/*_driver.xml</user_defined_executable_function_drivers_config>
       <dynamic_user_defined_executable_functions_path>/var/lib/clickhouse/dynamic_user_defined_executable_functions/</dynamic_user_defined_executable_functions_path>
   </clickhouse>
   ```

Реестр драйверов загружается при запуске сервера и обновляется по команде `SYSTEM RELOAD CONFIG`, поэтому драйверы можно добавлять, изменять и удалять без перезапуска сервера.

<div id="driver-configuration">
  ### Конфигурация драйвера
</div>

Драйвер описывается XML- или YAML-файлом с корневым элементом `<driver>`. Поддерживаются следующие поля:

| Поле               | Описание                                                                                                                                                                                                  | Обязательное |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| `name`             | Имя драйвера, используемое в `CREATE FUNCTION ... ENGINE = <name>(...)`.                                                                                                                                  | Да           |
| `create_command`   | Путь к программе, которая вызывается для создания UDF из фрагмента кода. Относительные пути разрешаются относительно файла конфигурации драйвера.                                                         | Да           |
| `drop_command`     | Путь к программе, которая вызывается при удалении функции, основанной на этом драйвере.                                                                                                                   | Нет          |
| `engine_arguments` | Определяет аргументы, допустимые внутри `ENGINE = DriverName(...)`. Каждый дочерний элемент представляет собой имя аргумента; дочерний элемент `<required>true</required>` помечает его как обязательный. | Нет          |
| `env`              | Переменные окружения, экспортируемые при вызове команд драйвера.                                                                                                                                          | Нет          |

Пример конфигурации драйвера:

```xml
<clickhouse>
    <driver>
        <name>DockerC</name>
        <create_command>../user_defined_executable_function_drivers/docker_c_create.sh</create_command>
        <drop_command>../user_defined_executable_function_drivers/docker_c_drop.sh</drop_command>
        <engine_arguments>
            <opt_level><required>false</required></opt_level>
        </engine_arguments>
        <env>
            <CLICKHOUSE_C_DRIVER_MEMORY>256m</CLICKHOUSE_C_DRIVER_MEMORY>
            <CLICKHOUSE_C_DRIVER_CPUS>1.0</CLICKHOUSE_C_DRIVER_CPUS>
        </env>
    </driver>
</clickhouse>
```

<div id="driver-invocation-contract">
  #### Контракт вызова драйвера
</div>

При выполнении `CREATE FUNCTION` вызывается `create_command` с заданными переменными `env` и следующими аргументами:

* `--name <function_name>`
* `--return <return_type>` (если указано предложение `RETURNS`)
* `--args <signature>` (если указано предложение `ARGUMENTS`), где сигнатура — это объявленный список аргументов, например `x UInt8, y DateTime`
* `--<key> <value>` для каждого объявленного аргумента движка, переданного в `ENGINE = DriverName(key = value)`

Тело пользовательского кода (текст после `AS`) передаётся на стандартный ввод команды. Команда должна вывести в стандартный вывод конфигурацию исполняемого UDF. format определяется автоматически: вывод, начинающийся с `<`, интерпретируется как XML, иначе — как YAML. Имя функции, определённое в сгенерированной configuration, должно совпадать с создаваемым именем. Если `create_command` завершается с ненулевым status, оператор завершается ошибкой с исключением, включающим код завершения и стандартный поток ошибок драйвера.

`drop_command`, если он задан, вызывается таким же образом (без тела кода в stdin) при удалении функции.

<div id="creating-a-function-with-a-driver">
  ### Создание FUNCTION
</div>

```sql
CREATE [OR REPLACE] FUNCTION [IF NOT EXISTS] name [ON CLUSTER cluster]
    ARGUMENTS (a UInt8, b String) RETURNS UInt64
    ENGINE = DriverName(key1 = 'value1', key2 = 42)
    AS '...code body...'
```

ClickHouse запускает `create_command` драйвера, записывает сгенерированную конфигурацию в [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path), и имеющийся загрузчик исполняемый UDF подхватывает её. После этого функцию можно вызывать как любую другую.

<div id="dropping-a-function-with-a-driver">
  ### Удаление функции
</div>

```sql
DROP FUNCTION [IF EXISTS] name [ON CLUSTER cluster]
```

`DROP FUNCTION` вызывает `drop_command` драйвера (если он задан), удаляет сгенерированную динамическую конфигурацию и отдельный рабочий каталог каждой функции, перезагружает загрузчик исполняемых UDF и удаляет сохранённый запрос.

<div id="driver-persistence-and-restart">
  ### Сохранение и перезапуск
</div>

Исходный запрос сохраняется в виде оператора `ATTACH FUNCTION ...` в каталоге пользовательских SQL-объектов, поэтому функция сохраняется после перезапуска сервера. При запуске сгенерированные конфигурации из [`dynamic_user_defined_executable_functions_path`](../../operations/server-configuration-parameters/settings.md#dynamic_user_defined_executable_functions_path) загружаются напрямую, без повторного запуска драйвера. Если для сохранённого `ATTACH FUNCTION` нет соответствующей сгенерированной конфигурации (например, если динамический каталог был утерян), драйвер запускается повторно, чтобы создать её заново.

<div id="driver-limitations">
  ### Ограничения
</div>

* Эта возможность экспериментальная и доступна только при включении `allow_experimental_executable_udf_drivers`.
* Функции на основе драйверов не поддерживаются в реплицируемом хранилище пользовательских функций (`ON CLUSTER` и `<user_defined_zookeeper_path>`), поскольку реплицируется только исходный запрос, а не созданные артефакты.
* При `RESTORE` резервной копии функции на основе драйвера запрос сохраняется, но драйвер не запускается повторно; сгенерированная конфигурация материализуется позже в ходе восстановления после перезапуска.

<div id="example-c-drivers">
  ### Пример драйверов C
</div>

В дереве исходного кода доступны демонстрационные драйверы в каталоге `programs/server/user_defined_executable_function_drivers_config.d/`, которые компилируют и выполняют тело функции на C. Это лишь примеры, и **в составе пакетов они не устанавливаются**:

* `DockerC` - компилирует и выполняет код внутри изолированных контейнеров Docker (`--network=none --read-only --cap-drop=ALL --security-opt=no-new-privileges`, а также ограничения по памяти/CPU/PID), создавая UDF `executable_pool`.
* `GVisorC` - вариант, который запускает скомпилированный бинарный файл в среде выполнения [gVisor](https://gvisor.dev/) `runsc`.
* `UnsafeC` - компилирует и выполняет код напрямую на хосте без песочницы. Как следует из названия, никакой изоляции он не обеспечивает и предназначен только для доверенных сред и тестирования.

Эти демонстрационные драйверы предназначены только как отправная точка; прежде чем предоставлять к ним доступ недоверенным пользователям, проверьте и дополнительно усильте изоляцию с учетом вашей среды.

<div id="related-content">
  ## Связанные материалы
</div>

* [Пользовательские функции в ClickHouse Cloud](https://clickhouse.com/blog/user-defined-functions-clickhouse-udfs)