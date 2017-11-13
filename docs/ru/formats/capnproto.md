CapnProto
=========

Cap'n Proto - формат бинарных сообщений, похож на Protocol Buffers и Thrift, но не похож на JSON или MessagePack.

Сообщения Cap'n Proto строго типизированы и не самоописывающиеся, т.е. нуждаются во внешнем описании схемы. Схема применяется "на лету" и кешируется для каждого запроса.

```sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase FORMAT CapnProto SETTINGS schema = 'schema.capnp:Message'
```

When the schema file looks like:

```
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

Десериализация эффективна и обычно не повышает нагрузку на систему.
