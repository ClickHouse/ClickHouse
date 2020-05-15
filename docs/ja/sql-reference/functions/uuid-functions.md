---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 53
toc_title: "UUID\u306E\u64CD\u4F5C"
---

# UUIDを操作するための関数 {#functions-for-working-with-uuid}

UUIDを操作するための関数を以下に示します。

## generateUUIDv4 {#uuid-function-generate}

を生成する。 [UUID](../../sql-reference/data-types/uuid.md) の [バージョン4](https://tools.ietf.org/html/rfc4122#section-4.4).

``` sql
generateUUIDv4()
```

**戻り値**

UUID型の値。

**使用例**

この例では、uuid型の列を使用してテーブルを作成し、テーブルに値を挿入する方法を示します。

``` sql
CREATE TABLE t_uuid (x UUID) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4()

SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┐
│ f4bf890f-f9dc-4332-ad5c-0c18e73f28e9 │
└──────────────────────────────────────┘
```

## toUUID(x) {#touuid-x}

文字列型の値をuuid型に変換します。

``` sql
toUUID(String)
```

**戻り値**

UUID型の値。

**使用例**

``` sql
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS uuid
```

``` text
┌─────────────────────────────────uuid─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0 │
└──────────────────────────────────────┘
```

## UUIDStringToNum {#uuidstringtonum}

次の形式の36文字を含む文字列を受け取ります `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` そして、それをaのバイトのセットとして返します [FixedString(16)](../../sql-reference/data-types/fixedstring.md).

``` sql
UUIDStringToNum(String)
```

**戻り値**

FixedString(16)

**使用例**

``` sql
SELECT
    '612f3c40-5d3b-217e-707b-6a546a3d7b29' AS uuid,
    UUIDStringToNum(uuid) AS bytes
```

``` text
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ a/<@];!~p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

## UUIDNumToString {#uuidnumtostring}

を受け入れる [FixedString(16)](../../sql-reference/data-types/fixedstring.md) 値、およびテキスト形式で36文字を含む文字列を返します。

``` sql
UUIDNumToString(FixedString(16))
```

**戻り値**

文字列。

**使用例**

``` sql
SELECT
    'a/<@];!~p{jTj={)' AS bytes,
    UUIDNumToString(toFixedString(bytes, 16)) AS uuid
```

``` text
┌─bytes────────────┬─uuid─────────────────────────────────┐
│ a/<@];!~p{jTj={) │ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │
└──────────────────┴──────────────────────────────────────┘
```

## また見なさい {#see-also}

-   [dictGetUUID](ext-dict-functions.md#ext_dict_functions-other)

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/uuid_function/) <!--hide-->
