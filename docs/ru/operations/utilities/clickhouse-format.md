---
toc_priority: 65
toc_title: clickhouse-format
---

# clickhouse-format {#clickhouse-format}

Утилита `clickhouse-format` позволяет форматировать входящие запросы.

Ключи:

- `--help` или`-h` — выводит описание ключей
- `--hilite` — добавляет подсветку синтаксиса с экранированием символов
- `--oneline` — форматирование в одну строку
- `--quiet` или `-q` — проверяет синтаксис без вывода результата
- `--multiquery` or `-n` — поддерживает несколько запросов в одной строке
- `--obfuscate` — обфускирует вместо форматирования
- `--seed <строка>` — задает строку, которая определяет результат обфускации
- `--backslash` — добавляет обратный слеш в конце каждой строки отформатированного запроса. Удобно использовать, если вы скопировали многострочный запрос из интернета или другого источника и хотите выполнить его из командной строки.

## Примеры {#examples} 

1. Пример с подсветкой синтаксиса и форматированием в одну строку:

    ```bash
    $clickhouse-format --oneline --hilite <<< "SELECT sum(number) FROM numbers(5);"
    ```

    Результат:

    ```sql
    SELECT sum(number) FROM numbers(5)
    ```

2. Пример с несколькими запросами в одной строке: 

    ```bash
    $clickhouse-format -n <<< "SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION DISTINCT SELECT 3);"
    ```
    
    Результат:

    ```text
    SELECT 1
    ;

    SELECT 1
    UNION ALL
    (
        SELECT 1
        UNION DISTINCT
        SELECT 3
    )
    ;
    ```
3. Пример с обфуксацией:

    ```bash
    $clickhouse-format --seed Hello --obfuscate <<< "SELECT cost_first_screen BETWEEN a AND b, CASE WHEN x >= 123 THEN y ELSE NULL END;"
    ```
    Результат:

    ```text
    SELECT treasury_mammoth_hazelnut BETWEEN nutmeg AND span, CASE WHEN chive >= 116 THEN switching ELSE ANYTHING END;
    ```
   
    Другая строка для обфускации:

    ```bash
    $ clickhouse-format --seed World --obfuscate <<< "SELECT cost_first_screen BETWEEN a AND b, CASE WHEN x >= 123 THEN y ELSE NULL END;"
    ```
    
    Результат:

    ```text
    SELECT horse_tape_summer BETWEEN folklore AND moccasins, CASE WHEN intestine >= 116 THEN nonconformist ELSE FORESTRY END;
    ```

4. Пример с обратным слешем:

    ```bash
    $clickhouse-format --backslash <<< "SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION DISTINCT SELECT 3);"
    ```

    Результат:

    ```text
    SELECT * \
    FROM  \
    ( \
       SELECT 1 AS x \
       UNION ALL \
       SELECT 1 \
       UNION DISTINCT \
       SELECT 3 \
    )
    ``` 