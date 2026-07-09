---
description: 'JSON文字列にランダムな変化を加えます。'
sidebar_label: 'fuzzJSON'
sidebar_position: 75
slug: /sql-reference/table-functions/fuzzJSON
title: 'fuzzJSON'
doc_type: 'reference'
---

JSON文字列にランダムな変化を加えます。

<div id="syntax">
  ## 構文
</div>

```sql
fuzzJSON({ named_collection [, option=value [,..]] | json_str[, random_seed] })
```

<div id="arguments">
  ## 引数
</div>

| Argument                           | Description                                                                  |
| ---------------------------------- | ---------------------------------------------------------------------------- |
| `named_collection`                 | [NAMED COLLECTION](/ja/sql-reference/statements/create/named-collection.md) です。 |
| `option=value`                     | named collection のオプションパラメータとその値です。                                          |
| `json_str` (String)                | JSON 形式の構造化データを表す入力文字列。                                                      |
| `random_seed` (UInt64)             | 安定した結果を生成するための手動のランダムシード。                                                    |
| `reuse_output` (boolean)           | fuzzing プロセスの出力を、次の fuzzer の入力として再利用します。                                     |
| `malform_output` (boolean)         | JSON object として解析できない文字列を生成します。                                              |
| `max_output_length` (UInt64)       | 生成または変更された JSON 文字列の最大許容長。                                                   |
| `probability` (Float64)            | JSON field (キー・バリューのペア) を fuzzing する確率。[0, 1] の範囲内である必要があります。                |
| `max_nesting_level` (UInt64)       | JSON データ内でネストされた構造の最大深さ。                                                     |
| `max_array_size` (UInt64)          | JSON array の最大許容サイズ。                                                         |
| `max_object_size` (UInt64)         | JSON object の 1 レベルあたりの最大フィールド数。                                             |
| `max_string_value_length` (UInt64) | String 値の最大長。                                                                |
| `min_key_length` (UInt64)          | キーの最小長。1 以上である必要があります。                                                       |
| `max_key_length` (UInt64)          | キーの最大長。指定する場合、`min_key_length` 以上である必要があります。                                 |

<div id="returned_value">
  ## 戻り値
</div>

変更が加えられたJSON文字列を含む1つのカラムを持つテーブルオブジェクト。

<div id="usage-example">
  ## 使用例
</div>

```sql
CREATE NAMED COLLECTION json_fuzzer AS json_str='{}';
SELECT * FROM fuzzJSON(json_fuzzer) LIMIT 3;
```

```text
{"52Xz2Zd4vKNcuP2":true}
{"UPbOhOQAdPKIg91":3405264103600403024}
{"X0QUWu8yT":[]}
```

```sql
SELECT * FROM fuzzJSON(json_fuzzer, json_str='{"name" : "value"}', random_seed=1234) LIMIT 3;
```

```text
{"key":"value", "mxPG0h1R5":"L-YQLv@9hcZbOIGrAn10%GA"}
{"BRE3":true}
{"key":"value", "SWzJdEJZ04nrpSfy":[{"3Q23y":[]}]}
```

```sql
SELECT * FROM fuzzJSON(json_fuzzer, json_str='{"students" : ["Alice", "Bob"]}', reuse_output=true) LIMIT 3;
```

```text
{"students":["Alice", "Bob"], "nwALnRMc4pyKD9Krv":[]}
{"students":["1rNY5ZNs0wU&82t_P", "Bob"], "wLNRGzwDiMKdw":[{}]}
{"xeEk":["1rNY5ZNs0wU&82t_P", "Bob"], "wLNRGzwDiMKdw":[{}, {}]}
```

```sql
SELECT * FROM fuzzJSON(json_fuzzer, json_str='{"students" : ["Alice", "Bob"]}', max_output_length=512) LIMIT 3;
```

```text
{"students":["Alice", "Bob"], "BREhhXj5":true}
{"NyEsSWzJdeJZ04s":["Alice", 5737924650575683711, 5346334167565345826], "BjVO2X9L":true}
{"NyEsSWzJdeJZ04s":["Alice", 5737924650575683711, 5346334167565345826], "BjVO2X9L":true, "k1SXzbSIz":[{}]}
```

```sql
SELECT * FROM fuzzJSON('{"id":1}', 1234) LIMIT 3;
```

```text
{"id":1, "mxPG0h1R5":"L-YQLv@9hcZbOIGrAn10%GA"}
{"BRjE":16137826149911306846}
{"XjKE":15076727133550123563}
```

```sql
SELECT * FROM fuzzJSON(json_nc, json_str='{"name" : "FuzzJSON"}', random_seed=1337, malform_output=true) LIMIT 3;
```

```text
U"name":"FuzzJSON*"SpByjZKtr2VAyHCO"falseh
{"name"keFuzzJSON, "g6vVO7TCIk":jTt^
{"DBhz":YFuzzJSON5}
```