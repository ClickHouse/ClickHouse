JSONOneLine
-----------

Differs from ``JSON`` only in that object is output as one line and without empty spaces.

This format is convenient to use when multiple objects are sent during the query execution such as
in case of WATCH query. In this case, a new line character can be used as a delimeter between different
query results.

Example:

.. code-block:: text

  {"meta":[{"name":"1","type":"UInt8"}],"data":[{"1":1}],"rows":1,"statistics":{"elapsed":0.000272909,"rows_read":0,"bytes_read":0}}

When used in WATCH query, ``hash`` attribute is added to the result:

.. code-block:: json

  {"hash":"687d4e26eeadf6dfe009b66f0c22edf","meta":[{"name":"1","type":"UInt8"}],...}

also in WATCH query, heartbeat objects are supported:

.. code-block:: json

  {"heartbeat":{"timestamp":"1517023830793441","hash":{"blocks":"687d4e26eeadf6dfe009b66f0c22edf"}}}

This format is only appropriate for outputting a query result, not for parsing.
See ``JSONEachRow`` format for INSERT queries.