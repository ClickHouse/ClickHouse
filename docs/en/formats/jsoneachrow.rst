JSONEachRow
-----------

If put in SELECT query, displays data in newline delimited JSON (JSON objects separated by \\n character) format.
If put in INSERT query, expects this kind of data as input.

.. code-block:: text

  {"SearchPhrase":"","count()":"8267016"}
  {"SearchPhrase":"bathroom interior","count()":"2166"}
  {"SearchPhrase":"yandex","count()":"1655"}
  {"SearchPhrase":"spring 2014 fashion","count()":"1549"}
  {"SearchPhrase":"free-form photo","count()":"1480"}
  {"SearchPhrase":"Angelina Jolie","count()":"1245"}
  {"SearchPhrase":"omsk","count()":"1112"}
  {"SearchPhrase":"photos of dog breeds","count()":"1091"}
  {"SearchPhrase":"curtain design","count()":"1064"}
  {"SearchPhrase":"baku","count()":"1000"}

Unlike JSON format, there are no replacements of invalid UTF-8 sequences. There can be arbitrary amount of bytes in a line.
This is done in order to avoid data loss during formatting. Values are displayed analogous to JSON format.

In INSERT queries JSON data can be supplied with arbitrary order of columns (JSON key-value pairs). It is also possible to omit values in which case the default value of the column is inserted. N.B. when using JSONEachRow format, complex default values are not supported, so when omitting a column its value will be zeros or empty string depending on its type.

Space characters between JSON objects are skipped. Between objects there can be a comma which is ignored. Newline character is not a mandatory separator for objects.
