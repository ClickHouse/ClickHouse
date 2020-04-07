---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 59
toc_title: IPv4
---

## IPv4 {#ipv4}

`IPv4` ドメインは以下に基づきます `UInt32` 入力し、IPv4値を格納するための型指定された置換として機能します。 それは点検で人間に適する入出力形式およびコラムのタイプ情報を密集した貯蔵に与える。

### 基本的な使用法 {#basic-usage}

``` sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY url;

DESCRIBE TABLE hits;
```

``` text
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
│ url  │ String │              │                    │         │                  │
│ from │ IPv4   │              │                    │         │                  │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┘
```

または、ipv4ドメインをキーとして使用できます:

``` sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY from;
```

`IPv4` ドメインはIPv4文字列としてカスタム入力形式をサポート:

``` sql
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '116.253.40.133')('https://clickhouse.tech', '183.247.232.58')('https://clickhouse.yandex/docs/en/', '116.106.34.242');

SELECT * FROM hits;
```

``` text
┌─url────────────────────────────────┬───────────from─┐
│ https://clickhouse.tech/docs/en/ │ 116.106.34.242 │
│ https://wikipedia.org              │ 116.253.40.133 │
│ https://clickhouse.tech          │ 183.247.232.58 │
└────────────────────────────────────┴────────────────┘
```

値が格納されコンパクトにバイナリ形式:

``` sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

``` text
┌─toTypeName(from)─┬─hex(from)─┐
│ IPv4             │ B7F7E83A  │
└──────────────────┴───────────┘
```

ドメイン値は、以下の型以外の型に暗黙的に変換できません `UInt32`.
変換したい場合 `IPv4` 値を文字列に変換するには、それを明示的に行う必要があります `IPv4NumToString()` 機能:

``` sql
SELECT toTypeName(s), IPv4NumToString(from) as s FROM hits LIMIT 1;
```

    ┌─toTypeName(IPv4NumToString(from))─┬─s──────────────┐
    │ String                            │ 183.247.232.58 │
    └───────────────────────────────────┴────────────────┘

または `UInt32` 値:

``` sql
SELECT toTypeName(i), CAST(from as UInt32) as i FROM hits LIMIT 1;
```

``` text
┌─toTypeName(CAST(from, 'UInt32'))─┬──────────i─┐
│ UInt32                           │ 3086477370 │
└──────────────────────────────────┴────────────┘
```

[元の記事](https://clickhouse.tech/docs/en/data_types/domains/ipv4) <!--hide-->
