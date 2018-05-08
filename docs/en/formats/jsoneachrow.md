# JSONEachRow

Outputs data as separate JSON objects for each row (newline delimited JSON).

```json
{"SearchPhrase":"","count()":"8267016"}
{"SearchPhrase":"bathroom interior design","count()":"2166"}
{"SearchPhrase":"yandex","count()":"1655"}
{"SearchPhrase":"spring 2014 fashion","count()":"1549"}
{"SearchPhrase":"freeform photo","count()":"1480"}
{"SearchPhrase":"angelina jolie","count()":"1245"}
{"SearchPhrase":"omsk","count()":"1112"}
{"SearchPhrase":"photos of dog breeds","count()":"1091"}
{"SearchPhrase":"curtain design","count()":"1064"}
{"SearchPhrase":"baku","count()":"1000"}
```

Unlike the JSON format, there is no substitution of invalid UTF-8 sequences. Any set of bytes can be output in the rows. This is necessary so that data can be formatted without losing any information. Values are escaped in the same way as for JSON.

For parsing, any order is supported for the values of different columns. It is acceptable for some values to be omitted – they are treated as equal to their default values. In this case, zeros and blank rows are used as default values. Complex values that could be specified in the table are not supported as defaults. Whitespace between elements is ignored. If a comma is placed after the objects, it is ignored. Objects don't necessarily have to be separated by new lines.

