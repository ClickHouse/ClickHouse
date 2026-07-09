---
description: 'Документация по интерфейсу командной строки клиента ClickHouse'
sidebar_label: 'Клиент ClickHouse'
sidebar_position: 18
slug: /interfaces/client
title: 'Клиент ClickHouse'
doc_type: 'справочник'
---

import Image from '@theme/IdealImage';
import cloud_connect_button from '@site/static/images/_snippets/cloud-connect-button.png';
import connection_details_native from '@site/static/images/_snippets/connection-details-native.png';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

ClickHouse предоставляет собственный клиент командной строки для выполнения SQL-запросов напрямую к серверу ClickHouse.
Он поддерживает как интерактивный режим (для выполнения запросов в реальном времени), так и пакетный режим (для сценариев и автоматизации).
Результаты запроса можно выводить в терминал или экспортировать в файл; поддерживаются все [форматы](formats.md) вывода ClickHouse, такие как Pretty, CSV, JSON и другие.

Клиент показывает информацию о выполнении запроса в реальном времени: индикатор прогресса, количество прочитанных строк, обработанных байтов и время выполнения запроса.
Он поддерживает как [параметры командной строки](#command-line-options), так и [файлы конфигурации](#configuration_files).

<div id="install">
  ## Установка
</div>

Чтобы загрузить ClickHouse, выполните:

```bash
curl https://clickhouse.com/ | sh
```

Чтобы установить и его, выполните:

```bash
sudo ./clickhouse install
```

См. [Установка ClickHouse](../getting-started/install/install.mdx), чтобы ознакомиться с другими вариантами установки.

Разные версии клиента и сервера совместимы между собой, но некоторые возможности могут быть недоступны в более старых версиях клиента. Мы рекомендуем использовать одну и ту же версию клиента и сервера.

<div id="run">
  ## Запуск
</div>

:::note
Если вы скачали ClickHouse, но не установили его, используйте `./clickhouse client` вместо `clickhouse-client`.
:::

Чтобы подключиться к серверу ClickHouse, выполните:

```bash
$ clickhouse-client --host server

ClickHouse client version 24.12.2.29 (official build).
Connecting to server:9000 as user default.
Connected to ClickHouse server version 24.12.2.

:)
```

При необходимости укажите дополнительные сведения о подключении:

| Option                           | Description                                                                                                                                                                                    |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--port <port>`                  | Порт, на котором ClickHouse server принимает подключения. Порты по умолчанию: 9440 (TLS) и 9000 (без TLS). Обратите внимание: клиент ClickHouse использует собственный протокол, а не HTTP(S). |
| `-s [ --secure ]`                | Использовать ли TLS (обычно определяется автоматически).                                                                                                                                       |
| `-u [ --user ] <username>`       | Пользователь базы данных, от имени которого выполняется подключение. По умолчанию используется пользователь `default`.                                                                         |
| `--password <password>`          | Пароль пользователя базы данных. Пароль для подключения также можно указать в конфигурационном файле. Если пароль не указан, клиент запросит его.                                              |
| `-c [ --config ] <path-to-file>` | Расположение конфигурационного файла для клиента ClickHouse, если он находится не в одном из стандартных расположений. См. [Конфигурационные файлы](#configuration_files).                        |
| `--connection <name>`            | Имя заранее настроенных сведений о подключении из [конфигурационного файла](#connection-credentials).                                                                                               |

Полный список параметров командной строки см. в разделе [Command Line Options](#command-line-options).

<div id="connecting-cloud">
  ### Подключение к ClickHouse Cloud
</div>

Сведения о вашем сервисе ClickHouse Cloud доступны в консоли ClickHouse Cloud. Выберите сервис, к которому нужно подключиться, и нажмите **Connect**:

<Image img={cloud_connect_button} size="md" alt="Кнопка подключения сервиса ClickHouse Cloud" />

<br />

<br />

Выберите **Native** — отобразятся сведения о подключении и пример команды `clickhouse-client`:

<Image img={connection_details_native} size="md" alt="Сведения о нативном TCP-соединении ClickHouse Cloud" />

<div id="connection-credentials">
  ### Хранение подключений в конфигурационном файле
</div>

Вы можете хранить сведения о подключении для одного или нескольких серверов ClickHouse в [конфигурационном файле](#configuration_files).

Формат выглядит так:

```xml
<config>
    <connections_credentials>
        <connection>
            <name>default</name>
            <hostname>hostname</hostname>
            <port>9440</port>
            <secure>1</secure>
            <user>default</user>
            <password>password</password>
            <!-- <history_file></history_file> -->
            <!-- <history_max_entries></history_max_entries> -->
            <!-- <accept-invalid-certificate>false</accept-invalid-certificate> -->
            <!-- <prompt></prompt> -->
        </connection>
    </connections_credentials>
</config>
```

См. [раздел о файлах конфигурации](#configuration_files), чтобы узнать подробности.

:::note
Чтобы не отвлекаться от синтаксиса запроса, в остальных примерах опущены сведения о подключении (`--host`, `--port` и т. д.). Не забудьте добавить их при использовании этих команд.
:::

<div id="interactive-mode">
  ## Интерактивный режим
</div>

<div id="using-interactive-mode">
  ### Работа в интерактивном режиме
</div>

Чтобы запустить ClickHouse в интерактивном режиме, просто выполните:

```bash
clickhouse-client
```

Откроется интерактивный цикл Read-Eval-Print Loop (REPL), где можно вводить SQL-запросы.
После подключения появится промпт, в котором можно вводить запросы:

```bash
ClickHouse client version 25.x.x.x
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 25.x.x.x

hostname :)
```

В интерактивном режиме формат вывода по умолчанию — `PrettyCompact`.
Вы можете изменить формат в предложении `FORMAT` запроса или указав параметр командной строки `--format`.
Чтобы использовать формат Vertical, можно указать `--vertical` или добавить `\G` в конце запроса.
В этом формате каждое значение выводится на отдельной строке, что удобно для широких таблиц.

В интерактивном режиме по умолчанию при нажатии `Enter` выполняется всё введённое.
Точка с запятой в конце запроса не обязательна.

Вы можете запустить клиент с параметром `-m, --multiline`.
Чтобы ввести многострочный запрос, поставьте обратную косую черту `\` перед переводом строки.
После нажатия `Enter` вам будет предложено ввести следующую строку запроса.
Чтобы выполнить запрос, завершите его точкой с запятой и нажмите `Enter`.

Клиент ClickHouse основан на `replxx` (аналог `readline`), поэтому поддерживает привычные сочетания клавиш и сохраняет историю.
По умолчанию история записывается в `~/.clickhouse-client-history`.

Чтобы выйти из клиента, нажмите `Ctrl+D` или введите вместо запроса одно из следующего:

* `exit` или `exit;`
* `quit` или `quit;`
* `q`, `Q` или `:q`
* `logout` или `logout;`

<div id="getting-help">
  ### Получение справки
</div>

Вы можете просматривать документацию по любой функции, движку таблицы, типу данных, формату, настройке и другим компонентам системы, не выходя из клиента. Введите `help`, а затем имя (также работают эквивалентные формы `/help`, `man` и `/man`):

```text
help domainWithoutWWW
```

Поиск регистронезависимый и выполняет запрос к таблице [`system.documentation`](../operations/system-tables/documentation.md). Соответствующая документация отображается в терминале в виде Markdown: с полужирным/курсивным текстом, таблицами и блоками кода с подсветкой синтаксиса. Когда одно имя используется несколькими сущностями (например, `file`, который является и функцией, и движком таблицы), показываются все варианты.

Если точных совпадений нет, клиент выводит список похожих имён (с учётом возможных опечаток) и сущностей, в документации которых упоминается это слово:

```text
help maxx_threads
```

Ввод `help` без аргументов выводит краткую справку по использованию.

<div id="processing-info">
  ### Информация об обработке запроса
</div>

При обработке запроса клиент показывает:

1. Прогресс, который по умолчанию обновляется не чаще 10 раз в секунду.
   Для быстрых запросов прогресс может не успеть отобразиться.
2. Отформатированный запрос после разбора — для отладки.
3. Результат в указанном формате.
4. Количество строк в результате, затраченное время и среднюю скорость обработки запроса.
   Все объемы данных относятся к несжатым данным.

Вы можете отменить долгий запрос, нажав `Ctrl+C`.
Однако после этого все равно придется немного подождать, пока сервер прервет выполнение запроса.
На некоторых этапах отменить запрос невозможно.
Если не ждать и нажать `Ctrl+C` второй раз, клиент завершит работу.

Клиент ClickHouse позволяет передавать внешние данные (внешние временные таблицы) для выполнения запросов.
Дополнительные сведения см. в разделе [Внешние данные для обработки запросов](../engines/table-engines/special/external-data.md).

<div id="cli_aliases">
  ### Псевдонимы
</div>

В REPL можно использовать следующие псевдонимы:

* `\l` - SHOW DATABASES
* `\d` - SHOW TABLES
* `\c <DATABASE>` - USE DATABASE
* `.` - повторить последний запрос

<div id="keyboard_shortcuts">
  ### Сочетания клавиш
</div>

* `Alt (Option) + Shift + e` — открыть редактор с текущим запросом. Редактор можно указать с помощью переменной окружения `EDITOR`. По умолчанию используется `vim`.
* `Alt (Option) + #` — закомментировать строку.
* `Ctrl + r` — нечёткий поиск по истории.

Полный список всех доступных сочетаний клавиш приведён в [replxx](https://github.com/AmokHuginnsson/replxx/blob/1f149bf/src/replxx_impl.cxx#L262).

:::tip
Чтобы клавиша meta (Option) в macOS работала корректно, настройте её следующим образом:

iTerm2: перейдите в Preferences -&gt; Profile -&gt; Keys -&gt; Left Option key и нажмите Esc+
:::

<div id="batch-mode">
  ## Режим батча
</div>

<div id="using-batch-mode">
  ### Использование режима батча
</div>

Вместо интерактивной работы с клиентом ClickHouse его можно запускать в режиме батча.
В режиме батча ClickHouse выполняет один запрос и сразу завершает работу — без интерактивного приглашения и без цикла.

Один запрос можно задать так:

```bash
$ clickhouse-client "SELECT sum(number) FROM numbers(10)"
45
```

Вы также можете использовать опцию командной строки `--query`:

```bash
$ clickhouse-client --query "SELECT uniq(number) FROM numbers(10)"
10
```

Вы можете передать запрос через `stdin`:

```bash
$ echo "SELECT avg(number) FROM numbers(10)" | clickhouse-client
4.5
```

Если таблица `messages` уже существует, вы также можете вставить данные из командной строки:

```bash
$ echo "Hello\nGoodbye" | clickhouse-client --query "INSERT INTO messages FORMAT CSV"
```

Когда указан `--query`, весь ввод добавляется к запросу после символа перевода строки.

<div id="cloud-example">
  ### Вставка CSV-файла в удалённый сервис ClickHouse
</div>

В этом примере CSV-файл с тестовым набором данных `cell_towers.csv` вставляется в существующую таблицу `cell_towers` в базе данных `default`:

```bash
clickhouse-client --host HOSTNAME.clickhouse.cloud \
  --port 9440 \
  --user default \
  --password PASSWORD \
  --query "INSERT INTO cell_towers FORMAT CSVWithNames" \
  < cell_towers.csv
```

<div id="more-examples">
  ### Примеры вставки данных из командной строки
</div>

Есть несколько способов вставки данных из командной строки.
В примере ниже в таблицу ClickHouse вставляются две строки данных в формате CSV в режиме батча:

```bash
echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | \
  clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

В примере ниже `cat <<_EOF` начинает heredoc, который считывает всё до следующего появления `_EOF`, а затем выводит результат:

```bash
cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF
```

В примере ниже содержимое файла file.csv выводится в stdout с помощью `cat`, а затем через конвейер передаётся на вход `clickhouse-client`:

```bash
cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

В режиме батча формат данных по умолчанию — `TabSeparated`.
Вы можете задать формат в предложении `FORMAT` запроса, как показано в примере выше. Ссылка на формат: [формат](formats.md)

<div id="cli-queries-with-parameters">
  ## Запросы с параметрами
</div>

Вы можете задавать параметры в запросе и передавать им значения через параметры командной строки.
Это позволяет избежать форматирования запроса с конкретными динамическими значениями на стороне клиента.
Например:

```bash
$ clickhouse-client --param_parName="[1, 2]" --query "SELECT {parName: Array(UInt16)}"
[1,2]
```

Параметры также можно задавать в [интерактивном сеансе](#interactive-mode):

```text
$ clickhouse-client
ClickHouse client version 25.X.X.XXX (official build).

#highlight-next-line
:) SET param_parName='[1, 2]';

SET param_parName = '[1, 2]'

Query id: 7ac1f84e-e89a-4eeb-a4bb-d24b8f9fd977

Ok.

0 rows in set. Elapsed: 0.000 sec.

#highlight-next-line
:) SELECT {parName:Array(UInt16)}

SELECT {parName:Array(UInt16)}

Query id: 0358a729-7bbe-4191-bb48-29b063c548a7

   ┌─_CAST([1, 2]⋯y(UInt16)')─┐
1. │ [1,2]                    │
   └──────────────────────────┘

1 row in set. Elapsed: 0.006 sec.
```

<div id="cli-queries-with-parameters-syntax">
  ### Синтаксис запроса
</div>

В запросе заключите в фигурные скобки значения, которые хотите подставить с помощью параметров командной строки, в следующем формате:

```sql
{<name>:<data type>}
```

| Параметр    | Описание                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`      | Идентификатор-заполнитель. Соответствующая опция командной строки — `--param_<name> = value`.                                                                                                                                                                                                                                                                                                                                                                              |
| `data type` | [Тип данных](../sql-reference/data-types/index.md) параметра. <br /><br />Например, структура данных вида `(integer, ('string', integer))` может иметь тип `Tuple(UInt8, Tuple(String, UInt8))` (также можно использовать другие типы [integer](../sql-reference/data-types/int-uint.md)). <br /><br />В качестве параметров также можно передавать имя таблицы, имя базы данных и имена столбцов; в этом случае в качестве типа данных следует использовать `Identifier`. |

<div id="cli-queries-with-parameters-examples">
  ### Примеры
</div>

```bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" \
    --query "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"

$ clickhouse-client --param_tbl="numbers" --param_db="system" --param_col="number" --param_alias="top_ten" \
    --query "SELECT {col:Identifier} as {alias:Identifier} FROM {db:Identifier}.{tbl:Identifier} LIMIT 10"
```

<div id="ai-sql-generation">
  ## Генерация SQL с помощью ИИ
</div>

Клиент ClickHouse включает встроенную поддержку ИИ для генерации SQL-запросов по описаниям на естественном языке. Эта возможность помогает пользователям писать сложные запросы без глубокого знания SQL.

Поддержка ИИ работает сразу после настройки переменной окружения `OPENAI_API_KEY` или `ANTHROPIC_API_KEY`. Для более тонкой настройки см. раздел [Конфигурация](#ai-sql-generation-configuration).

<div id="ai-sql-generation-usage">
  ### Использование
</div>

Чтобы воспользоваться генерацией SQL с помощью ИИ, добавьте префикс `??` перед запросом на естественном языке:

```bash
:) ?? show all users who made purchases in the last 30 days
```

ИИ будет:

1. Автоматически анализировать схему вашей базы данных
2. Генерировать подходящий SQL на основе обнаруженных таблиц и столбцов
3. Сразу выполнять сгенерированный запрос

<div id="ai-sql-generation-example">
  ### Пример
</div>

```bash
:) ?? count orders by product category

Starting AI SQL generation with schema discovery...
──────────────────────────────────────────────────

🔍 list_databases
   ➜ system, default, sales_db

🔍 list_tables_in_database
   database: sales_db
   ➜ orders, products, categories

🔍 get_schema_for_table
   database: sales_db
   table: orders
   ➜ CREATE TABLE orders (order_id UInt64, product_id UInt64, quantity UInt32, ...)

✨ SQL query generated successfully!
──────────────────────────────────────────────────

SELECT
    c.name AS category,
    COUNT(DISTINCT o.order_id) AS order_count
FROM sales_db.orders o
JOIN sales_db.products p ON o.product_id = p.product_id
JOIN sales_db.categories c ON p.category_id = c.category_id
GROUP BY c.name
ORDER BY order_count DESC
```

<div id="ai-sql-generation-configuration">
  ### Настройка
</div>

Для генерации SQL с помощью ИИ необходимо настроить провайдера ИИ в файле конфигурации клиента ClickHouse. Можно использовать OpenAI, Anthropic или любой API-сервис, совместимый с OpenAI.

<div id="ai-sql-generation-fallback">
  #### Использование переменных окружения в качестве резервного варианта
</div>

Если в файле конфигурации не задана конфигурация ИИ, клиент ClickHouse автоматически попытается использовать переменные окружения:

1. Сначала проверяется переменная окружения `OPENAI_API_KEY`
2. Если она не найдена, проверяется переменная окружения `ANTHROPIC_API_KEY`
3. Если не найдена ни одна из них, возможности ИИ будут отключены

Это позволяет быстро выполнить настройку без файлов конфигурации:

```bash
# Using OpenAI
export OPENAI_API_KEY=your-openai-key
clickhouse-client

# Using Anthropic
export ANTHROPIC_API_KEY=your-anthropic-key
clickhouse-client
```

<div id="ai-sql-generation-configuration-file">
  #### Файл конфигурации
</div>

Чтобы точнее управлять настройками AI, задайте их в файле конфигурации клиента ClickHouse, расположенном по одному из следующих путей:

* `$XDG_CONFIG_HOME/clickhouse/config.xml` (или `~/.config/clickhouse/config.xml`, если `XDG_CONFIG_HOME` не задан) (формат XML)
* `$XDG_CONFIG_HOME/clickhouse/config.yaml` (или `~/.config/clickhouse/config.yaml`, если `XDG_CONFIG_HOME` не задан) (формат YAML)
* `~/.clickhouse-client/config.xml` (формат XML, устаревший путь)
* `~/.clickhouse-client/config.yaml` (формат YAML, устаревший путь)
* Или укажите пользовательский путь с помощью `--config-file`

<Tabs>
  <TabItem value="xml" label="XML" default>
    ```xml
    <config>
        <ai>
            <!-- Обязательно: ваш API-ключ (или задайте его через переменную окружения) -->
            <api_key>your-api-key-here</api_key>

            <!-- Обязательно: тип провайдера (openai, anthropic) -->
            <provider>openai</provider>

            <!-- Используемая модель (значение по умолчанию зависит от провайдера) -->
            <model>gpt-4o</model>

            <!-- Необязательно: пользовательская конечная точка API для сервисов, совместимых с OpenAI -->
            <!-- <base_url>https://openrouter.ai/api</base_url> -->

            <!-- Настройки доступа к схеме -->
            <enable_schema_access>true</enable_schema_access>

            <!-- Параметры генерации -->
            <!-- Необязательно: temperature передаётся модели, только если задан здесь.
                 По умолчанию он не передаётся, потому что некоторые модели не принимают этот параметр. -->
            <!-- <temperature>0.0</temperature> -->
            <max_tokens>1000</max_tokens>
            <timeout_seconds>30</timeout_seconds>
            <max_steps>10</max_steps>

            <!-- Необязательно: пользовательский системный промпт -->
            <!-- <system_prompt>You are an expert ClickHouse SQL assistant...</system_prompt> -->
        </ai>
    </config>
    ```
  </TabItem>

  <TabItem value="yaml" label="YAML">
    ```yaml
    ai:
      # Обязательно: ваш API-ключ (или задайте его через переменную окружения)
      api_key: your-api-key-here

      # Обязательно: тип провайдера (openai, anthropic)
      provider: openai

      # Используемая модель
      model: gpt-4o

      # Необязательно: пользовательская конечная точка API для сервисов, совместимых с OpenAI
      # base_url: https://openrouter.ai/api

      # Включает доступ к схеме — позволяет AI запрашивать информацию о базе данных и таблицах
      enable_schema_access: true

      # Параметры генерации
      # temperature передаётся модели, только если задан здесь; по умолчанию не передаётся,
      # потому что некоторые модели не принимают этот параметр.
      # temperature: 0.0    # Управляет случайностью (0.0 = deterministic)
      max_tokens: 1000      # Максимальная длина ответа
      timeout_seconds: 30   # Тайм-аут запроса
      max_steps: 10         # Максимальное число шагов исследования схемы

      # Необязательно: пользовательский системный промпт
      # system_prompt: |
      #   Вы — экспертный помощник по ClickHouse SQL. Преобразуйте естественный язык в SQL.
      #   Уделяйте внимание производительности и используйте оптимизации, специфичные для ClickHouse.
      #   Всегда возвращайте исполняемый SQL без объяснений.
    ```
  </TabItem>
</Tabs>

<br />

**Использование API, совместимых с OpenAI (например, OpenRouter):**

```yaml
ai:
  provider: openai  # Use 'openai' for compatibility
  api_key: your-openrouter-api-key
  base_url: https://openrouter.ai/api/v1
  model: anthropic/claude-3.5-sonnet  # Use OpenRouter model naming
```

**Примеры минимальной конфигурации:**

```yaml
# Minimal config - uses environment variable for API key
ai:
  provider: openai  # Will use OPENAI_API_KEY env var

# No config at all - automatic fallback
# (Empty or no ai section - will try OPENAI_API_KEY then ANTHROPIC_API_KEY)

# Only override model - uses env var for API key
ai:
  provider: openai
  model: gpt-3.5-turbo
```

<div id="ai-sql-generation-parameters">
  ### Параметры
</div>

<details>
  <summary>Обязательные параметры</summary>

  * `api_key` - Ваш API-ключ для сервиса ИИ. Его можно не указывать, если он задан через переменную окружения:
    * OpenAI: `OPENAI_API_KEY`
    * Anthropic: `ANTHROPIC_API_KEY`
    * Note: API-ключ в файле конфигурации имеет приоритет над переменной окружения
  * `provider` - Провайдер ИИ: `openai` или `anthropic`
    * Если не указан, выполняется автоматический выбор на основе доступных переменных окружения
</details>

<details>
  <summary>Конфигурация модели</summary>

  * `model` - Модель, которую нужно использовать (по умолчанию: зависит от провайдера)
    * OpenAI: `gpt-4o`, `gpt-4`, `gpt-3.5-turbo`, и т. д.
    * Anthropic: `claude-3-5-sonnet-20241022`, `claude-3-opus-20240229`, и т. д.
    * OpenRouter: Используйте их формат именования моделей, например `anthropic/claude-3.5-sonnet`
</details>

<details>
  <summary>Настройки подключения</summary>

  * `base_url` - Пользовательская конечная точка API для сервисов, совместимых с OpenAI (необязательно)
  * `timeout_seconds` - Тайм-аут запроса в секундах (по умолчанию: `30`)
</details>

<details>
  <summary>Изучение схем</summary>

  * `enable_schema_access` - Разрешить ИИ изучать схемы базы данных (по умолчанию: `true`)
  * `max_steps` - Максимальное число шагов вызова инструментов для изучения схем (по умолчанию: `10`)
</details>

<details>
  <summary>Параметры генерации</summary>

  * `temperature` - Управляет случайностью: 0.0 = детерминированно, 1.0 = креативно. По умолчанию параметр не задаётся и отправляется модели только при явной установке, поскольку некоторые модели его не принимают.
  * `max_tokens` - Максимальная длина ответа в токенах (по умолчанию: `1000`)
  * `system_prompt` - Пользовательские инструкции для ИИ (необязательно)
</details>

<div id="ai-sql-generation-how-it-works">
  ### Как это работает
</div>

Генератор SQL на базе ИИ использует многоэтапный процесс:

<VerticalStepper headerLevel="list">
  1. **Обнаружение схемы**

  ИИ использует встроенные инструменты для анализа вашей базы данных

  * Выводит список доступных баз данных
  * Обнаруживает таблицы в соответствующих базах данных
  * Анализирует структуры таблиц с помощью операторов `CREATE TABLE`

  2. **Генерация запросов**

  На основе обнаруженной схемы ИИ генерирует SQL, который:

  * Соответствует вашему запросу на естественном языке
  * Использует правильные имена таблиц и столбцов
  * Применяет подходящие JOIN и агрегации

  3. **Выполнение**

  Сгенерированный SQL выполняется автоматически, а результаты отображаются
</VerticalStepper>

<div id="ai-sql-generation-limitations">
  ### Ограничения
</div>

* Требуется подключение к интернету
* На использование API распространяются ограничения по частоте запросов, а также тарифы поставщика ИИ
* Для сложных запросов может потребоваться несколько уточнений
* ИИ имеет доступ только для чтения к информации о схеме, но не к самим данным

<div id="ai-sql-generation-security">
  ### Безопасность
</div>

* Ключи API никогда не отправляются на серверы ClickHouse
* ИИ видит только информацию о схеме (имена таблиц/столбцов и типы), а не сами данные
* Все сгенерированные запросы учитывают существующие разрешения вашей базы данных

<div id="connection_string">
  ## Строка подключения
</div>

<div id="ai-sql-generation-usage">
  ### Использование
</div>

Клиент ClickHouse также поддерживает подключение к серверу ClickHouse с помощью строки подключения, аналогичной тем, что используются в [MongoDB](https://www.mongodb.com/docs/manual/reference/connection-string/), [PostgreSQL](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) и [MySQL](https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html#connecting-using-uri). Используется следующий синтаксис:

```text
clickhouse:[//[user[:password]@][hosts_and_ports]][/database][?query_parameters]
```

| Компонент (все необязательны) | Описание                                                                                                                                                              | По умолчанию     |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| `user`                        | Имя пользователя базы данных.                                                                                                                                         | `default`        |
| `password`                    | Пароль пользователя базы данных. Если указан `:`, а пароль пуст, клиент предложит ввести пароль пользователя.                                                         | -                |
| `hosts_and_ports`             | Список хостов и необязательных портов `host[:port] [, host:[port]], ...`.                                                                                             | `localhost:9000` |
| `database`                    | Имя базы данных.                                                                                                                                                      | `default`        |
| `query_parameters`            | Список пар ключ-значение `param1=value1[,&param2=value2], ...`. Для некоторых параметров значение не требуется. Имена параметров и значения чувствительны к регистру. | -                |

<div id="connection-string-notes">
  ### Примечания
</div>

Если username, password или database указаны в строке подключения, их нельзя указывать через `--user`, `--password` или `--database` (и наоборот).

Компонент хоста может быть либо именем хоста, либо адресом IPv4 или IPv6.
IPv6-адреса должны быть заключены в `[]`:

```text
clickhouse://[2001:db8::1234]
```

Строки подключения могут содержать несколько хостов.
Клиент ClickHouse будет пытаться подключиться к этим хостам по порядку (слева направо).
После установления соединения попытки подключения к оставшимся хостам не выполняются.

Строка подключения должна быть указана в качестве первого аргумента для `clickHouse-client`.
Строку подключения можно использовать вместе с произвольным количеством других [параметров командной строки](#command-line-options), кроме `--host` и `--port`.

Для `query_parameters` допустимы следующие ключи:

| Key               | Описание                                                                                                                                                    |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `secure` (or `s`) | Если указано, клиент подключится к серверу через защищённое соединение (TLS). См. `--secure` в разделе [параметры командной строки](#command-line-options). |

**Процентное кодирование**

Символы вне US-ASCII, пробелы и специальные символы в следующих параметрах должны быть [процентно закодированы](https://en.wikipedia.org/wiki/URL_encoding):

* `user`
* `password`
* `hosts`
* `database`
* `query parameters`

<div id="cli-queries-with-parameters-examples">
  ### Примеры
</div>

Подключитесь к `localhost` через порт 9000 и выполните запрос `SELECT 1`.

```bash
clickhouse-client clickhouse://localhost:9000 --query "SELECT 1"
```

Подключитесь к `localhost` от имени пользователя `john` с паролем `secret`, используя хост `127.0.0.1` и порт `9000`

```bash
clickhouse-client clickhouse://john:secret@127.0.0.1:9000
```

Подключитесь к `localhost` от имени пользователя `default`, указав хост с IPv6-адресом `[::1]` и порт `9000`.

```bash
clickhouse-client clickhouse://[::1]:9000
```

Подключитесь к `localhost` на порту 9000 в многострочном режиме.

```bash
clickhouse-client clickhouse://localhost:9000 '-m'
```

Подключитесь к `localhost` на порту 9000 от имени пользователя `default`.

```bash
clickhouse-client clickhouse://default@localhost:9000

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --user default
```

Подключитесь к `localhost` на порту 9000 и по умолчанию используйте базу данных `my_database`.

```bash
clickhouse-client clickhouse://localhost:9000/my_database

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --database my_database
```

Подключитесь к `localhost` через порт 9000, используйте по умолчанию базу данных `my_database`, указанную в строке подключения, и задайте защищённое соединение с помощью сокращённого параметра `s`.

```bash
clickhouse-client clickhouse://localhost/my_database?s

# equivalent to:
clickhouse-client clickhouse://localhost/my_database -s
```

Подключитесь к хосту по умолчанию, используя порт по умолчанию, пользователя default и базу данных по умолчанию.

```bash
clickhouse-client clickhouse:
```

Подключитесь к хосту по умолчанию через порт по умолчанию под пользователем `my_user`, без пароля.

```bash
clickhouse-client clickhouse://my_user@

# Using a blank password between : and @ means to asking the user to enter the password before starting the connection.
clickhouse-client clickhouse://my_user:@
```

Подключитесь к `localhost`, используя адрес электронной почты в качестве имени пользователя. Символ `@` кодируется как `%40`.

```bash
clickhouse-client clickhouse://some_user%40some_mail.com@localhost:9000
```

Подключитесь к одному из двух хостов: `192.168.1.15`, `192.168.1.25`.

```bash
clickhouse-client clickhouse://192.168.1.15,192.168.1.25
```

<div id="query-id-format">
  ## Формат идентификатора запроса
</div>

В интерактивном режиме клиент ClickHouse показывает идентификатор для каждого запроса. По умолчанию он имеет такой формат:

```sql
Query id: 927f137d-00f1-4175-8914-0dd066365e96
```

Пользовательский формат можно указать в файле конфигурации внутри тега `query_id_formats`. Заполнитель `{query_id}` в строке формата заменяется идентификатором запроса. Внутри тега можно указать несколько строк формата.
Эту возможность можно использовать для генерации URL-адресов, чтобы упростить анализ данных профилирования запросов.

**Пример**

```xml
<config>
  <query_id_formats>
    <speedscope>http://speedscope-host/#profileURL=qp%3Fid%3D{query_id}</speedscope>
  </query_id_formats>
</config>
```

При указанной выше конфигурации идентификатор запроса отображается в следующем формате:

```response
speedscope:http://speedscope-host/#profileURL=qp%3Fid%3Dc8ecc783-e753-4b38-97f1-42cddfb98b7d
```

<div id="configuration_files">
  ## Конфигурационные файлы
</div>

Клиент ClickHouse использует первый из следующих существующих файлов:

* Файл, указанный с помощью параметра `-c [ -C, --config, --config-file ]`.
* `./clickhouse-client.[xml|yaml|yml]`
* `$XDG_CONFIG_HOME/clickhouse/config.[xml|yaml|yml]` (или `~/.config/clickhouse/config.[xml|yaml|yml]`, если `XDG_CONFIG_HOME` не задан)
* `~/.clickhouse-client/config.[xml|yaml|yml]`
* `/etc/clickhouse-client/config.[xml|yaml|yml]`

См. пример файла конфигурации в репозитории ClickHouse: [`clickhouse-client.xml`](https://github.com/ClickHouse/ClickHouse/blob/master/programs/client/clickhouse-client.xml)

<Tabs>
  <TabItem value="xml" label="XML" default>
    ```xml
    <config>
        <user>username</user>
        <password>password</password>
        <secure>true</secure>
        <openSSL>
          <client>
            <caConfig>/etc/ssl/cert.pem</caConfig>
          </client>
        </openSSL>
    </config>
    ```
  </TabItem>

  <TabItem value="yaml" label="YAML">
    ```yaml
    user: username
    password: 'password'
    secure: true
    openSSL:
      client:
        caConfig: '/etc/ssl/cert.pem'
    ```
  </TabItem>
</Tabs>

<div id="environment-variable-options">
  ## Параметры переменных окружения
</div>

Имя пользователя, пароль и хост можно задать с помощью переменных окружения `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` и `CLICKHOUSE_HOST`.
Аргументы командной строки `--user`, `--password` или `--host`, а также [строка подключения](#connection_string) (если она указана) имеют приоритет над переменными окружения.

<div id="command-line-options">
  ## Параметры командной строки
</div>

Все параметры командной строки можно указывать непосредственно в командной строке или задавать значения по умолчанию в [конфигурационном файле](#configuration_files).

<div id="command-line-options-general">
  ### Общие параметры
</div>

| Параметр                                            | Описание                                                                                                                                         | По умолчанию           |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------- |
| `-c [ -C, --config, --config-file ] <path-to-file>` | Расположение конфигурационного файла клиента, если он находится не в одном из стандартных мест. См. [Файлы конфигурации](#configuration_files).  | -                      |
| `--help`                                            | Вывести краткую справку и завершить работу. Используйте вместе с `--verbose`, чтобы показать все возможные параметры, включая настройки запроса. | -                      |
| `--history_file <path-to-file>`                     | Путь к файлу с историей команд.                                                                                                                  | -                      |
| `--history_max_entries`                             | Максимальное число записей в файле истории.                                                                                                      | `1000000` (1 миллион)  |
| `--prompt <prompt>`                                 | Указать пользовательский промпт.                                                                                                                 | `display_name` сервера |
| `--verbose`                                         | Увеличить подробность вывода.                                                                                                                    | -                      |
| `-V [ --version ]`                                  | Вывести версию и завершить работу.                                                                                                               | -                      |

<div id="command-line-options-connection">
  ### Параметры подключения
</div>

| Option                               | Description                                                                                                                                                                                                                                                                                                                                                                                                        | Default                                                                                                                                    |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `--connection <name>`                | Имя предварительно настроенных сведений о подключении из файла конфигурации. См. [Учетные данные подключения](#connection-credentials).                                                                                                                                                                                                                                                                            | -                                                                                                                                          |
| `-d [ --database ] <database>`       | Выберите базу данных, которая будет использоваться по умолчанию для этого подключения.                                                                                                                                                                                                                                                                                                                             | Текущая база данных из настройки сервера (`default` по умолчанию)                                                                          |
| `-h [ --host ] <host>`               | Имя хоста сервера ClickHouse, к которому нужно подключиться. Это может быть как имя хоста, так и IPv4- или IPv6-адрес. Можно передать несколько хостов, указав несколько аргументов.                                                                                                                                                                                                                               | `localhost`                                                                                                                                |
| `--jwt <value>`                      | Использовать JSON Web Token (JWT) для аутентификации. <br /><br />JWT-авторизация на сервере доступна только в ClickHouse Cloud.                                                                                                                                                                                                                                                                                   | -                                                                                                                                          |
| `login`                              | Запускает OAuth-поток Device Grant для аутентификации через IdP. <br /><br />Для хостов ClickHouse Cloud переменные OAuth определяются автоматически; в противном случае их нужно указать с помощью `--oauth-url`, `--oauth-client-id` и `--oauth-audience`.                                                                                                                                                       | -                                                                                                                                          |
| `--no-warnings`                      | Отключить показ предупреждений из `system.warnings` при подключении клиента к серверу.                                                                                                                                                                                                                                                                                                                             | -                                                                                                                                          |
| `--no-server-client-version-message` | Подавить сообщение о несовпадении версий сервера и клиента при подключении клиента к серверу.                                                                                                                                                                                                                                                                                                                      | -                                                                                                                                          |
| `--password <password>`              | Пароль пользователя базы данных. Пароль для подключения также можно указать в файле конфигурации. Если пароль не указан, клиент запросит его.                                                                                                                                                                                                                                                                      | -                                                                                                                                          |
| `--port <port>`                      | Порт, на котором сервер принимает подключения. Порты по умолчанию: 9440 (TLS) и 9000 (без TLS). <br /><br />Примечание: клиент использует собственный протокол, а не HTTP(S).                                                                                                                                                                                                                                      | `9440`, если указан `--secure`, иначе `9000`. Если имя хоста оканчивается на `.clickhouse.cloud`, по умолчанию всегда используется `9440`. |
| `-s [ --secure ]`                    | Использовать ли TLS. <br /><br />Автоматически включается при подключении к порту 9440 (защищенный порт по умолчанию) или к ClickHouse Cloud. <br /><br />Вам может потребоваться настроить CA‑сертификаты в [файле конфигурации](#configuration_files). Доступные параметры конфигурации такие же, как для [настройки TLS на стороне сервера](../operations/server-configuration-parameters/settings.md#openssl). | Автоматически включается при подключении к порту 9440 или к ClickHouse Cloud                                                               |
| `--ssh-key-file <path-to-file>`      | Файл, содержащий закрытый SSH-ключ для аутентификации при подключении к серверу.                                                                                                                                                                                                                                                                                                                                   | -                                                                                                                                          |
| `--ssh-key-passphrase <value>`       | Кодовая фраза для закрытого SSH-ключа, указанного в `--ssh-key-file`.                                                                                                                                                                                                                                                                                                                                              | -                                                                                                                                          |
| `--tls-sni-override <server name>`   | Если используется TLS, имя сервера (SNI), передаваемое при рукопожатии.                                                                                                                                                                                                                                                                                                                                            | Хост, указанный через `-h` или `--host`.                                                                                                   |
| `-u [ --user ] <username>`           | Пользователь базы данных, от имени которого выполняется подключение.                                                                                                                                                                                                                                                                                                                                               | `default`                                                                                                                                  |

:::note
Вместо параметров `--host`, `--port`, `--user` и `--password` клиент также поддерживает [строки подключения](#connection_string).
:::

<div id="command-line-options-query">
  ### Параметры запроса
</div>

| Option                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--param_<name>=<value>`        | Значение подстановки для параметра [запроса с параметрами](#cli-queries-with-parameters).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `-q [ --query ] <query>`        | Запрос для выполнения в batch mode. Можно указать несколько раз (`--query "SELECT 1" --query "SELECT 2"`) или один раз, перечислив несколько запросов через точку с запятой (`--query "SELECT 1; SELECT 2;"`). В последнем случае запросы `INSERT` с форматами, отличными от `VALUES`, должны разделяться пустыми строками. <br /><br />Один запрос также можно указать без параметра: `clickhouse-client "SELECT 1"` <br /><br />Нельзя использовать вместе с `--queries-file`.                                                                                                                                                                                                    |
| `--queries-file <path-to-file>` | Путь к файлу с запросами. `--queries-file` можно указать несколько раз, например: `--queries-file queries1.sql --queries-file queries2.sql`. <br /><br />Нельзя использовать вместе с `--query`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `-m [ --multiline ]`            | Если указано, разрешает многострочные запросы (запрос не отправляется по нажатию Enter). Запросы отправляются только после завершения точкой с запятой.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `--inline-insert-data`          | Отправлять `INSERT ... VALUES` (и другие inline-форматы) как есть в тексте запроса вместо преобразования данных в блоки в собственном формате. В этом случае сервер сам разбирает inline-данные, что позволяет избежать round-trip с передачей клиенту структуры таблицы и значений столбцов по умолчанию. Это может повысить производительность при большом количестве небольших вставок через собственный протокол. Автоматически устанавливает [`send_table_structure_on_insert_with_inline_data`](/ru/operations/settings/settings#send_table_structure_on_insert_with_inline_data) в `0`. Нельзя использовать вместе с inline-данными и внешними данными (из stdin или `INFILE`). |

<div id="command-line-options-query-settings">
  ### Настройки запроса
</div>

Настройки запроса можно указать в клиенте через параметры командной строки, например:

```bash
$ clickhouse-client --max_threads 1
```

Список настроек см. в разделе [Настройки](../operations/settings/settings.md).

<div id="command-line-options-formatting">
  ### Параметры форматирования
</div>

| Option                            | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | Default                                                                |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------- |
| `-f [ --format ] <format>`        | Использовать указанный формат для вывода результата. <br /><br />Список поддерживаемых форматов см. в разделе [Форматы для входных и выходных данных](formats.md).                                                                                                                                                                                                                                                                                                                                                                                                                               | `TabSeparated`                                                         |
| `--pager <command>`               | Передавать весь вывод в эту команду. Обычно это `less` (например, `less -S` для отображения широких результирующих наборов) или аналогичная команда.                                                                                                                                                                                                                                                                                                                                                                                                                                             | -                                                                      |
| `-E [ --vertical ]`               | Использовать [вертикальный формат](/ru/interfaces/formats/Vertical) для вывода результата. Это то же самое, что `--format Vertical`. В этом формате каждое значение выводится на отдельной строке, что удобно при отображении широких таблиц.                                                                                                                                                                                                                                                                                                                                                       | -                                                                      |
| `--echo [ <bool> ]`               | Выводить каждый запрос перед выполнением. Принимает необязательное булевое значение.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `true` в интерактивном режиме, `false` в неинтерактивном (батч) режиме |
| `--echo-formatted [ <bool> ]`     | Форматировать выводимые запросы. Принимает необязательное булевое значение.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | `true` в интерактивном режиме, `false` в неинтерактивном (батч) режиме |
| `--echo-query-id [ <bool> ]`      | Выводить Query id перед выполнением. Принимает необязательное булевое значение.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | `true` в интерактивном режиме, `false` в неинтерактивном (батч) режиме |
| `--echo-query-separator <string>` | Выводить этот разделитель перед отформатированным запросом (требуется `--echo-formatted`), чтобы было проще отличить введённый запрос от его переформатированного вывода.                                                                                                                                                                                                                                                                                                                                                                                                                        | Пусто (отключено)                                                      |
| `--highlight [ --hilite ] <bool>` | Включать или отключать подсветку синтаксиса в командной строке и для выводимых запросов.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | `true`                                                                 |
| `--hints <bool>`                  | Показывать подсказки автодополнения по мере ввода (в виде встроенного «призрачного» текста) с наиболее подходящим вариантом, когда курсор находится в конце строки ввода. Переключаться между подсказками можно клавишами Up/Down (или Ctrl-Up/Ctrl-Down); встроенную подсказку можно принять клавишами Tab или Right; `Enter` принимает подсказку только после её явного выбора, а иначе выполняет запрос; `Tab` также открывает классический список автодополнения. Требуется `--highlight` (подсказкам нужен цвет) и механизм предложений (поэтому `--disable_suggestion` тоже их отключает). | `true`                                                                 |

<div id="command-line-options-execution-details">
  ### Параметры выполнения
</div>

| Option                           | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                           | Default                                                              |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| `--chime [N]`                    | Записывает управляющий символ `BEL` в `stderr`, когда запрос завершается (успешно или с ошибкой), если его выполнение заняло не менее `N` секунд. Выводится только если `stderr` подключен к терминалу (TTY); перенаправление `stderr` (например, `2>err.log`) подавляет этот вывод, а перенаправление `stdout` (например, `> result.tsv`) — нет. Если указать `--chime` без значения, будет использован порог по умолчанию. Установите `--chime 0`, чтобы отключить. | `5` секунд                                                           |
| `--enable-progress-table-toggle` | Включает переключение таблицы прогресса нажатием управляющей клавиши (Space). Применяется только в интерактивном режиме, когда включен вывод таблицы прогресса.                                                                                                                                                                                                                                                                                                       | `включено`                                                           |
| `--hardware-utilization`         | Выводит информацию об использовании аппаратных ресурсов в индикаторе прогресса.                                                                                                                                                                                                                                                                                                                                                                                       | -                                                                    |
| `--memory-usage`                 | Если указан, выводит использование памяти в `stderr` в неинтерактивном режиме. <br /><br />Возможные значения: <br />• `none` - не выводить использование памяти <br />• `default` - выводить число байт <br />• `readable` - выводить использование памяти в человекочитаемом формате                                                                                                                                                                                | -                                                                    |
| `--print-profile-events`         | Выводит packets `ProfileEvents`.                                                                                                                                                                                                                                                                                                                                                                                                                                      | -                                                                    |
| `--progress`                     | Выводит прогресс выполнения запроса. <br /><br />Возможные значения: <br />• `tty\|on\|1\|true\|yes` - вывод в терминал в интерактивном режиме <br />• `err` - вывод в `stderr` в неинтерактивном режиме <br />• `off\|0\|false\|no` - отключает вывод прогресса                                                                                                                                                                                                      | `tty` в интерактивном режиме, `off` в неинтерактивном (batch) режиме |
| `--progress-table`               | Выводит таблицу прогресса с изменяющимися метриками во время выполнения запроса. <br /><br />Возможные значения: <br />• `tty\|on\|1\|true\|yes` - вывод в терминал в интерактивном режиме <br />• `err` - вывод в `stderr` в неинтерактивном режиме <br />• `off\|0\|false\|no` - отключает таблицу прогресса                                                                                                                                                        | `tty` в интерактивном режиме, `off` в неинтерактивном (batch) режиме |
| `--stacktrace`                   | Выводит трассировки стека исключений.                                                                                                                                                                                                                                                                                                                                                                                                                                 | -                                                                    |
| `-t [ --time ]`                  | Выводит время выполнения запроса в `stderr` в неинтерактивном режиме (для бенчмарков).                                                                                                                                                                                                                                                                                                                                                                                | -                                                                    |