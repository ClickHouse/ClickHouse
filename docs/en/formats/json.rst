JSON
----

Outputs data in JSON format. Besides data tables, it also outputs column names and types, along with some additional information - the total number of output rows, and the number of rows that could have been output if there weren't a LIMIT. Example:

.. code-block:: sql

  SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
  
.. code-block:: json

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
                          "SearchPhrase": "интерьер ванной комнаты",
                          "c": "2166"
                  },
                  {
                          "SearchPhrase": "яндекс",
                          "c": "1655"
                  },
                  {
                          "SearchPhrase": "весна 2014 мода",
                          "c": "1549"
                  },
                  {
                          "SearchPhrase": "фриформ фото",
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

JSON is compatible with JavaScript. For this purpose, certain symbols are additionally escaped: the forward slash ``/`` is escaped as ``\/``; alternative line breaks ``U+2028`` and ``U+2029``, which don't work in some browsers, are escaped as \uXXXX-sequences. ASCII control characters are escaped: backspace, form feed, line feed, carriage return, and horizontal tab as ``\b``, ``\f``, ``\n``, ``\r``, and ``\t`` respectively, along with the rest of the bytes from the range 00-1F using \uXXXX-sequences. Invalid UTF-8 sequences are changed to the replacement character ``�`` and, thus, the output text will consist of valid UTF-8 sequences. UInt64 and Int64 numbers are output in double quotes for compatibility with JavaScript.

``rows`` - The total number of output rows.

``rows_before_limit_at_least`` - The minimal number of rows there would have been without a LIMIT. Output only if the query contains LIMIT.

If the query contains GROUP BY, ``rows_before_limit_at_least`` is the exact number of rows there would have been without a LIMIT.

``totals`` - Total values (when using WITH TOTALS).

``extremes`` - Extreme values (when extremes is set to 1).

When used in WATCH query, ``hash`` attribute is added to the result:

.. code-block:: json

  {
          "hash": "687d4e26eeadf6dfe009b66f0c22edf",

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
          ...
  }

also in WATCH query, heartbeat objects are supported:

.. code-block:: json

  {
          "heartbeat":
          {
                  "timestamp": "1517023310853606",

                  "hash":
                  {
                          "blocks": "687d4e26eeadf6dfe009b66f0c22edf"
                  }
          }
  }

This format is only appropriate for outputting a query result, not for parsing.
See JSONEachRow format for INSERT queries.
