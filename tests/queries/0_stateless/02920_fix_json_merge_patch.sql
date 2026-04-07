-- Tags: no-fasttest

select '{"id":1,"foo":["bar"]}' as a, jsonMergePatch(a,toJSONString(map('foo',arrayPushBack(arrayMap(x->JSONExtractString(x),JSONExtractArrayRaw(a, 'foo')),'baz')))) as b;
