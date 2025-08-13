---
slug: /en/sql-reference/table-functions/fuzzJSON
sidebar_position: 75
sidebar_label: fuzzJSON
---

# fuzzJSON

Perturbs a JSON string with random variations.

``` sql
fuzzJSON({ named_collection [, option=value [,..]] | json_str[, random_seed] })
```

**Arguments**

- `named_collection`- A [NAMED COLLECTION](/docs/en/sql-reference/statements/create/named-collection.md).
- `option=value` - Named collection optional parameters and their values.
 - `json_str` (String) - The source string representing structured data in JSON format.
 - `random_seed` (UInt64) - Manual random seed for producing stable results.
 - `reuse_output` (boolean) - Reuse the output from a fuzzing process as input for the next fuzzer.
 - `malform_output` (boolean) - Generate a string that cannot be parsed as a JSON object.
 - `max_output_length` (UInt64) - Maximum allowable length of the generated or perturbed JSON string.
 - `probability` (Float64) - The probability to fuzz a JSON field (a key-value pair). Must be within [0, 1] range.
 - `max_nesting_level` (UInt64) - The maximum allowed depth of nested structures within the JSON data.
 - `max_array_size` (UInt64) - The maximum allowed size of a JSON array.
 - `max_object_size` (UInt64) - The maximum allowed number of fields on a single level of a JSON object.
 - `max_string_value_length` (UInt64) - The maximum length of a String value.
 - `min_key_length` (UInt64) - The minimum key length. Should be at least 1.
 - `max_key_length` (UInt64) - The maximum key length. Should be greater or equal than the `min_key_length`, if specified.

**Returned Value**

A table object with a a single column containing perturbed JSON strings.

## Usage Example

``` sql
CREATE NAMED COLLECTION json_fuzzer AS json_str='{}';
SELECT * FROM fuzzJSON(json_fuzzer) LIMIT 3;
```

``` text
{"52Xz2Zd4vKNcuP2":true}
{"UPbOhOQAdPKIg91":3405264103600403024}
{"X0QUWu8yT":[]}
```

``` sql
SELECT * FROM fuzzJSON(json_fuzzer, json_str='{"name" : "value"}', random_seed=1234) LIMIT 3;
```

``` text
{"key":"value", "mxPG0h1R5":"L-YQLv@9hcZbOIGrAn10%GA"}
{"BRE3":true}
{"key":"value", "SWzJdEJZ04nrpSfy":[{"3Q23y":[]}]}
```

``` sql
SELECT * FROM fuzzJSON(json_fuzzer, json_str='{"students" : ["Alice", "Bob"]}', reuse_output=true) LIMIT 3;
```

``` text
{"students":["Alice", "Bob"], "nwALnRMc4pyKD9Krv":[]}
{"students":["1rNY5ZNs0wU&82t_P", "Bob"], "wLNRGzwDiMKdw":[{}]}
{"xeEk":["1rNY5ZNs0wU&82t_P", "Bob"], "wLNRGzwDiMKdw":[{}, {}]}
```

``` sql
SELECT * FROM fuzzJSON(json_fuzzer, json_str='{"students" : ["Alice", "Bob"]}', max_output_length=512) LIMIT 3;
```

``` text
{"students":["Alice", "Bob"], "BREhhXj5":true}
{"NyEsSWzJdeJZ04s":["Alice", 5737924650575683711, 5346334167565345826], "BjVO2X9L":true}
{"NyEsSWzJdeJZ04s":["Alice", 5737924650575683711, 5346334167565345826], "BjVO2X9L":true, "k1SXzbSIz":[{}]}
```

``` sql
SELECT * FROM fuzzJSON('{"id":1}', 1234) LIMIT 3;
```

``` text
{"id":1, "mxPG0h1R5":"L-YQLv@9hcZbOIGrAn10%GA"}
{"BRjE":16137826149911306846}
{"XjKE":15076727133550123563}
```

``` sql
SELECT * FROM fuzzJSON(json_nc, json_str='{"name" : "FuzzJSON"}', random_seed=1337, malform_output=true) LIMIT 3;
```

``` text
U"name":"FuzzJSON*"SpByjZKtr2VAyHCO"falseh
{"name"keFuzzJSON, "g6vVO7TCIk":jTt^
{"DBhz":YFuzzJSON5}
```
