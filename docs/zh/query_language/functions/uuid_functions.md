# UUID函数

下面列出了所有UUID的相关函数

## generateUUIDv4 {#uuid_function-generate}

生成一个UUID（[版本4](https://tools.ietf.org/html/rfc4122#section-4.4)）。

```sql
generateUUIDv4()
```

**返回值**

UUID类型的值。

**使用示例**

此示例演示如何在表中创建UUID类型的列，并对其写入数据。

``` sql
:) CREATE TABLE t_uuid (x UUID) ENGINE=TinyLog

:) INSERT INTO t_uuid SELECT generateUUIDv4()

:) SELECT * FROM t_uuid

┌────────────────────────────────────x─┐
│ f4bf890f-f9dc-4332-ad5c-0c18e73f28e9 │
└──────────────────────────────────────┘
```

## toUUID (x)

将String类型的值转换为UUID类型的值。

```sql
toUUID(String)
```

**返回值**

UUID类型的值

**使用示例**

``` sql
:) SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS uuid

┌─────────────────────────────────uuid─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0 │
└──────────────────────────────────────┘
```

## UUIDStringToNum

接受一个String类型的值，其中包含36个字符且格式为`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`，将其转换为UUID的数值并以[FixedString(16)](../../data_types/fixedstring.md)将其返回。

``` sql
UUIDStringToNum(String)
```

**返回值**

FixedString(16)

**使用示例**

``` sql
:) SELECT
    '612f3c40-5d3b-217e-707b-6a546a3d7b29' AS uuid,
    UUIDStringToNum(uuid) AS bytes

┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ a/<@];!~p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

## UUIDNumToString

接受一个[FixedString(16)](../../data_types/fixedstring.md)类型的值，返回其对应的String表现形式。

``` sql
UUIDNumToString(FixedString(16))
```

**返回值**

String.

**使用示例**

``` sql
SELECT
    'a/<@];!~p{jTj={)' AS bytes,
    UUIDNumToString(toFixedString(bytes, 16)) AS uuid

┌─bytes────────────┬─uuid─────────────────────────────────┐
│ a/<@];!~p{jTj={) │ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │
└──────────────────┴──────────────────────────────────────┘
```

## 另请参阅

- [dictGetUUID](ext_dict_functions.md)

[来源文章](https://clickhouse.yandex/docs/en/query_language/functions/uuid_function/) <!--hide-->
