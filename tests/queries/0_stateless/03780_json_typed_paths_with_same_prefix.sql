set enable_analyzer=1;
select '{"a" : 42, "a.b" : 43}'::JSON(a UInt32, `a.b` UInt32) as json, json.a, json.a.b;

