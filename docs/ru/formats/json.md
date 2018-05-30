<a name="json"></a>

# JSON

Выводит данные в формате JSON. Кроме таблицы с данными, также выводятся имена и типы столбцов, и некоторая дополнительная информация - общее количество выведенных строк, а также количество строк, которое могло бы быть выведено, если бы не было LIMIT-а. Пример:

```sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

```json
{
        "meta":
        [
                {
                        "name": "SearchPhrase",
                        "type": "String"
                },
                {
                        "name": "c",
                        "type": "UInt64"
                }
        ],

        "data":
        [
                {
                        "SearchPhrase": "",
                        "c": "8267016"
                },
                {
                        "SearchPhrase": "bathroom interior design",
                        "c": "2166"
                },
                {
                        "SearchPhrase": "yandex",
                        "c": "1655"
                },
                {
                        "SearchPhrase": "spring 2014 fashion",
                        "c": "1549"
                },
                {
                        "SearchPhrase": "freeform photos",
                        "c": "1480"
                }
        ],

        "totals":
        {
                "SearchPhrase": "",
                "c": "8873898"
        },

        "extremes":
        {
                "min":
                {
                        "SearchPhrase": "",
                        "c": "1480"
                },
                "max":
                {
                        "SearchPhrase": "",
                        "c": "8267016"
                }
        },

        "rows": 5,

        "rows_before_limit_at_least": 141137
}
```

JSON совместим с JavaScript. Для этого, дополнительно экранируются некоторые символы: символ прямого слеша `/` экранируется в виде `\/`; альтернативные переводы строк `U+2028`, `U+2029`, на которых ломаются некоторые браузеры, экранируются в виде `\uXXXX`-последовательностей. Экранируются ASCII control characters: backspace, form feed, line feed, carriage return, horizontal tab в виде `\b`, `\f`, `\n`, `\r`, `\t` соответственно, а также остальные байты из диапазона 00-1F с помощью `\uXXXX`-последовательностей. Невалидные UTF-8 последовательности заменяются на replacement character � и, таким образом, выводимый текст будет состоять из валидных UTF-8 последовательностей. Числа типа UInt64 и Int64, для совместимости с JavaScript, по умолчанию выводятся в двойных кавычках, чтобы они выводились без кавычек можно установить конфигурационный параметр output_format_json_quote_64bit_integers равным 0.

`rows` - общее количество выведенных строчек.

`rows_before_limit_at_least` - не менее скольких строчек получилось бы, если бы не было LIMIT-а. Выводится только если запрос содержит LIMIT.
В случае, если запрос содержит GROUP BY, rows_before_limit_at_least - точное число строк, которое получилось бы, если бы не было LIMIT-а.

`totals` - тотальные значения (при использовании WITH TOTALS).

`extremes` - экстремальные значения (при настройке extremes, выставленной в 1).

Этот формат подходит только для вывода результата выполнения запроса, но не для парсинга (приёма данных для вставки в таблицу).
Смотрите также формат JSONEachRow.
