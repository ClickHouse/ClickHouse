set enable_analyzer=1;

select '{"a" : [{"b" : 42}]}'::JSON(a Array(JSON)) as json, json.a.b, json::JSON as json2, dynamicType(json2.a), json2.a[].b;

select '{"transaction": {"date": "2025-03-13 22:20:29.751999"}}'::JSON::JSON(transaction Nullable(JSON(date DateTime64(6, 'UTC'))));

