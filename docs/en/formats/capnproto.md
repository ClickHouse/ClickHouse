<a name="format_capnproto"></a>

# CapnProto

Cap'n Proto is a binary message format similar to Protocol Buffers and Thrift, but not like JSON or MessagePack.

Cap'n Proto messages are strictly typed and not self-describing, meaning they need an external schema description. The schema is applied on the fly and cached for each query.

```sql
SELECT SearchPhrase, count() AS c FROM test.hits
       GROUP BY SearchPhrase FORMAT CapnProto SETTINGS schema = 'schema:Message'
```

Where `schema.capnp` looks like this:

```
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

Schema files are in the file that is located in the directory specified in [ format_schema_path](../operations/server_settings/settings.md#server_settings-format_schema_path) in the server configuration.

Deserialization is effective and usually doesn't increase the system load.

