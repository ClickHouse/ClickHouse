JSONCompact
-----------

Differs from ``JSON`` only in that data rows are output in arrays, not in objects. 

Example: 
::
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
                  ["", "8267016"],
                  ["bath interiors", "2166"],
                  ["yandex", "1655"],
                  ["spring 2014 fashion", "1549"],
                  ["freeform photo", "1480"]
          ],

          "totals": ["","8873898"],

          "extremes":
          {
                  "min": ["","1480"],
                  "max": ["","8267016"]
          },

          "rows": 5,
  
          "rows_before_limit_at_least": 141137
  }

This format is only appropriate for outputting a query result, not for parsing.
See ``JSONEachRow`` format for INSERT queries.
