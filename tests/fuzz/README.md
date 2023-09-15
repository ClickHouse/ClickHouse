The list of funtions generated via following query

```
    clickhouse-client -q "select concat('\"', name, '\"') from system.functions union all select concat('\"', alias_to, '\"') from system.functions where alias_to != '' " > functions.dict
```

The list of datatypes generated via following query:

```
    clickhouse-client -q "select concat('\"', name, '\"') from system.data_type_families union all select concat('\"', alias_to, '\"') from system.data_type_families where alias_to != '' " > datatypes.dict
```


Then merge all dictionaries into one (all.dict)
