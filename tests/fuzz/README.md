The list of functions generated via the following query

```
    clickhouse client -q "SELECT * FROM (SELECT DISTINCT concat('\"', name, '\"') as res FROM system.functions ORDER BY name UNION ALL SELECT concat('\"', a.name, b.name, '\"') as res FROM system.functions as a CROSS JOIN system.aggregate_function_combinators as b WHERE a.is_aggregate = 1) ORDER BY res" > functions.dict
```

The list of datatypes generated via the following query:

```
    clickhouse client -q "SELECT DISTINCT concat('\"', name, '\"') as res FROM system.data_type_families ORDER BY name" > datatypes.dict
```

The list of keywords generated via the following query:

```
    clickhouse client -q "SELECT DISTINCT concat('\"', keyword, '\"') as res FROM system.keywords ORDER BY keyword" > key_words.dict
```

Then merge all dictionaries into one (all.dict)

```
    cat ./dictionaries/* | sort | uniq > all.dict
```