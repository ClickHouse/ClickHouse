---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 60
toc_title: IPv6
---

## IPv6 {#ipv6}

`IPv6` に基づくドメインです `FixedString(16)` IPv6値を格納するための型指定された置換として機能します。 それは点検で人間に適する入出力フォーマットおよびコラムのタイプ情報を密集した貯蔵に与える。

### 基本的な使用法 {#basic-usage}

``` sql
CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY url;

DESCRIBE TABLE hits;
```

``` text
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
│ url  │ String │              │                    │         │                  │
│ from │ IPv6   │              │                    │         │                  │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┘
```

または、 `IPv6` キーとしてのドメイン:

``` sql
CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY from;
```

`IPv6` ドメイ:

``` sql
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '2a02:aa08:e000:3100::2')('https://clickhouse.com', '2001:44c8:129:2632:33:0:252:2')('https://clickhouse.com/docs/en/', '2a02:e980:1e::1');

SELECT * FROM hits;
```

``` text
┌─url────────────────────────────────┬─from──────────────────────────┐
│ https://clickhouse.com          │ 2001:44c8:129:2632:33:0:252:2 │
│ https://clickhouse.com/docs/en/ │ 2a02:e980:1e::1               │
│ https://wikipedia.org              │ 2a02:aa08:e000:3100::2        │
└────────────────────────────────────┴───────────────────────────────┘
```

値が格納されコンパクトにバイナリ形式:

``` sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

``` text
┌─toTypeName(from)─┬─hex(from)────────────────────────┐
│ IPv6             │ 200144C8012926320033000002520002 │
└──────────────────┴──────────────────────────────────┘
```

ドメイン値は、暗黙的に型以外に変換できません `FixedString(16)`.
変換したい場合 `IPv6` 文字列への値は、明示的にそれを行う必要があります `IPv6NumToString()` 関数:

``` sql
SELECT toTypeName(s), IPv6NumToString(from) as s FROM hits LIMIT 1;
```

``` text
┌─toTypeName(IPv6NumToString(from))─┬─s─────────────────────────────┐
│ String                            │ 2001:44c8:129:2632:33:0:252:2 │
└───────────────────────────────────┴───────────────────────────────┘
```

またはaにキャスト `FixedString(16)` 値:

``` sql
SELECT toTypeName(i), CAST(from as FixedString(16)) as i FROM hits LIMIT 1;
```

``` text
┌─toTypeName(CAST(from, 'FixedString(16)'))─┬─i───────┐
│ FixedString(16)                           │  ��� │
└───────────────────────────────────────────┴─────────┘
```

[元の記事](https://clickhouse.com/docs/en/data_types/domains/ipv6) <!--hide-->
