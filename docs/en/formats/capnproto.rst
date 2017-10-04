CapnProto
---------

Cap'n Proto is a binary message format. Like Protocol Buffers and Thrift (but unlike JSON or MessagePack), Cap'n Proto messages are strongly-typed and not self-describing. Due to this, it requires a ``schema`` setting to specify schema file and the root object. The schema is parsed on runtime and cached for each SQL statement.

.. code-block:: sql

  SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase FORMAT CapnProto SETTINGS schema = 'schema.capnp:Message'

When the schema file looks like:

.. code-block:: text

  struct Message {
    SearchPhrase @0 :Text;
    c @1 :Uint64;
  }

Deserialisation is almost as efficient as the binary rows format, with typically zero allocation overhead per message.

You can use this format as an efficient exchange message format in your data processing pipeline.