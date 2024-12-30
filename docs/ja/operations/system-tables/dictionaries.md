---
slug: /ja/operations/system-tables/dictionaries
---
# dictionaries

[Dictionary](../../sql-reference/dictionaries/index.md)に関する情報を含んでいます。

カラム:

- `database` ([String](../../sql-reference/data-types/string.md)) — DDLクエリによって作成されたDictionaryを含むデータベースの名前。他のDictionaryの場合は空文字列。
- `name` ([String](../../sql-reference/data-types/string.md)) — [Dictionary名](../../sql-reference/dictionaries/index.md)。
- `uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — DictionaryのUUID。
- `status` ([Enum8](../../sql-reference/data-types/enum.md)) — Dictionaryのステータス。可能な値は以下の通りです：
    - `NOT_LOADED` — 使用されなかったためDictionaryはロードされませんでした。
    - `LOADED` — Dictionaryが正常にロードされました。
    - `FAILED` — エラーが原因でDictionaryをロードできませんでした。
    - `LOADING` — 現在、Dictionaryがロード中です。
    - `LOADED_AND_RELOADING` — Dictionaryが正常にロードされ、現在リロード中です（頻繁な理由: [SYSTEM RELOAD DICTIONARY](../../sql-reference/statements/system.md#query_language-system-reload-dictionary)クエリ、タイムアウト、Dictionary設定の変更）。
    - `FAILED_AND_RELOADING` — エラーが原因でDictionaryをロードできず、現在ロード中です。
- `origin` ([String](../../sql-reference/data-types/string.md)) — Dictionaryを記述する設定ファイルへのパス。
- `type` ([String](../../sql-reference/data-types/string.md)) — Dictionaryの割り当てタイプ。[メモリにDictionaryを保存](../../sql-reference/dictionaries/index.md#storig-dictionaries-in-memory)。
- `key.names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Dictionaryによって提供される[キーの名前](../../sql-reference/dictionaries/index.md#dictionary-key-and-fields#ext_dict_structure-key)の配列。
- `key.types` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Dictionaryによって提供される[キーのタイプ](../../sql-reference/dictionaries/index.md#dictionary-key-and-fields#ext_dict_structure-key)の対応する配列。
- `attribute.names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Dictionaryによって提供される[属性の名前](../../sql-reference/dictionaries/index.md#dictionary-key-and-fields#ext_dict_structure-attributes)の配列。
- `attribute.types` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Dictionaryによって提供される[属性のタイプ](../../sql-reference/dictionaries/index.md#dictionary-key-and-fields#ext_dict_structure-attributes)の対応する配列。
- `bytes_allocated` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Dictionaryに割り当てられたRAMの量。
- `query_count` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Dictionaryのロードまたは最後の正常な再起動以来のクエリの数。
- `hit_rate` ([Float64](../../sql-reference/data-types/float.md)) — キャッシュDictionaryの場合、キャッシュ内に値があった使用回数の割合。
- `found_rate` ([Float64](../../sql-reference/data-types/float.md)) — 値が見つかった使用回数の割合。
- `element_count` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Dictionaryに格納されているアイテムの数。
- `load_factor` ([Float64](../../sql-reference/data-types/float.md)) — Dictionary （ハッシュベースのDictionaryではハッシュテーブル）の充填率。
- `source` ([String](../../sql-reference/data-types/string.md)) — Dictionaryの[データソース](../../sql-reference/dictionaries/index.md#dictionary-sources)を説明するテキスト。
- `lifetime_min` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — メモリ内のDictionaryの最小[有効期限](../../sql-reference/dictionaries/index.md#dictionary-updates)、この後ClickHouseはDictionaryをリロードしようとします（`invalidate_query`が設定されている場合、変わっていればのみ）。秒単位で設定。
- `lifetime_max` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — メモリ内のDictionaryの最大[有効期限](../../sql-reference/dictionaries/index.md#dictionary-updates)、この後ClickHouseはDictionaryをリロードしようとします（`invalidate_query`が設定されている場合、変わっていればのみ）。秒単位で設定。
- `loading_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Dictionaryのロード開始時間。
- `last_successful_update_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Dictionaryのロードまたは更新の終了時間。Dictionaryソースに関連する問題の監視と原因調査に役立ちます。
- `loading_duration` ([Float32](../../sql-reference/data-types/float.md)) — Dictionaryロードの期間。
- `last_exception` ([String](../../sql-reference/data-types/string.md)) — Dictionaryを作成またはリロードする際に発生するエラーのテキスト、Dictionaryが作成できなかった場合。
- `comment` ([String](../../sql-reference/data-types/string.md)) — Dictionaryへのコメントのテキスト。

**例**

Dictionaryを構成します：

``` sql
CREATE DICTIONARY dictionary_with_comment
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'source_table'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000)
COMMENT 'The temporary dictionary';
```

Dictionaryがロードされていることを確認します。

``` sql
SELECT * FROM system.dictionaries LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
database:                    default
name:                        dictionary_with_comment
uuid:                        4654d460-0d03-433a-8654-d4600d03d33a
status:                      NOT_LOADED
origin:                      4654d460-0d03-433a-8654-d4600d03d33a
type:
key.names:                   ['id']
key.types:                   ['UInt64']
attribute.names:             ['value']
attribute.types:             ['String']
bytes_allocated:             0
query_count:                 0
hit_rate:                    0
found_rate:                  0
element_count:               0
load_factor:                 0
source:
lifetime_min:                0
lifetime_max:                0
loading_start_time:          1970-01-01 00:00:00
last_successful_update_time: 1970-01-01 00:00:00
loading_duration:            0
last_exception:
comment:                     The temporary dictionary
```
