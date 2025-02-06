---
slug: /ja/sql-reference/functions/url-functions
sidebar_position: 200
sidebar_label: URLs
---

# URLを扱う関数

:::note
このセクションで言及している関数は、最大のパフォーマンスのために最適化されており、ほとんどの場合、RFC-3986標準に従いません。RFC-3986を実装している関数には、関数名の末尾に`RFC`が付いており、一般的に動作は遅くなります。
:::

一般に、ユーザー文字列や`@`記号を含まない公的に登録されたドメインを扱う場合、`RFC`でない関数の変種を使用できます。以下の表は、各`RFC`および非`RFC`変種によってURL内のどのシンボルを解析できる（`✔`）か、またはできない（`✗`）かを示しています：

|記号   | 非`RFC`  | `RFC` |
|-------|----------|-------|
| ' '   | ✗        |✗      |
|  \t   | ✗        |✗      |
|  <    | ✗        |✗      |
|  >    | ✗        |✗      |
|  %    | ✗        |✔*     |
|  {    | ✗        |✗      |
|  }    | ✗        |✗      |
|  \|   | ✗        |✗      |
|  \\\  | ✗        |✗      |
|  ^    | ✗        |✗      |
|  ~    | ✗        |✔*     |
|  [    | ✗        |✗      |
|  ]    | ✗        |✔      |
|  ;    | ✗        |✔*     |
|  =    | ✗        |✔*     |
|  &    | ✗        |✔*     |

`*`が付いているシンボルは、RFC 3986におけるサブデリミタであり、`@`記号に続くユーザー情報に許可されています。

## URLの一部を抽出する関数

URLに関連する部分が存在しない場合、空の文字列が返されます。

### protocol

URLからプロトコルを抽出します。

一般的に返される値の例： http, https, ftp, mailto, tel, magnet。

### domain

URLからホスト名を抽出します。

**構文**

``` sql
domain(url)
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。

プロトコルをつけてもつけなくても指定できます。例:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://clickhouse.com/time/
```

これらの例に対して、`domain`関数は以下の結果を返します：

``` text
some.svn-hosting.com
some.svn-hosting.com
clickhouse.com
```

**返される値**

- 入力文字列がURLとして解析できる場合はホスト名、そうでない場合は空の文字列。 [String](../data-types/string.md)。

**例**

``` sql
SELECT domain('svn+ssh://some.svn-hosting.com:80/repo/trunk');
```

``` text
┌─domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')─┐
│ some.svn-hosting.com                                   │
└────────────────────────────────────────────────────────┘
```

### domainRFC

URLからホスト名を抽出します。[domain](#domain)と似ていますが、RFC 3986に準拠しています。

**構文**

``` sql
domainRFC(url)
```

**引数**

- `url` — URL。 [String](../data-types/string.md)。

**返される値**

- 入力文字列がURLとして解析できる場合はホスト名、そうでない場合は空の文字列。 [String](../data-types/string.md)。

**例**

``` sql
SELECT
    domain('http://user:password@example.com:8080/path?query=value#fragment'),
    domainRFC('http://user:password@example.com:8080/path?query=value#fragment');
```

``` text
┌─domain('http://user:password@example.com:8080/path?query=value#fragment')─┬─domainRFC('http://user:password@example.com:8080/path?query=value#fragment')─┐
│                                                                           │ example.com                                                                  │
└───────────────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────────────┘
```

### domainWithoutWWW

先頭の`www.`を除いたドメインを返します。

**構文**

```sql
domainWithoutWWW(url)
```

**引数**

- `url` — URL。 [String](../data-types/string.md)。

**返される値**

- 入力文字列がURLとして解析できる場合はドメイン名（先頭の`www.`を除く）、そうでない場合は空の文字列。 [String](../data-types/string.md)。

**例**

``` sql
SELECT domainWithoutWWW('http://paul@www.example.com:80/');
```

``` text
┌─domainWithoutWWW('http://paul@www.example.com:80/')─┐
│ example.com                                         │
└─────────────────────────────────────────────────────┘
```

### domainWithoutWWWRFC

先頭の`www.`を除いたドメインを返します。[domainWithoutWWW](#domainwithoutwww)と似ていますが、RFC 3986に準拠しています。

**構文**

```sql
domainWithoutWWWRFC(url)
```

**引数**

- `url` — URL。 [String](../data-types/string.md)。

**返される値**

- 入力文字列がURLとして解析できる場合はドメイン名（先頭の`www.`を除く）、そうでない場合は空の文字列。 [String](../data-types/string.md)。

**例**

クエリ:

```sql
SELECT
    domainWithoutWWW('http://user:password@www.example.com:8080/path?query=value#fragment'),
    domainWithoutWWWRFC('http://user:password@www.example.com:8080/path?query=value#fragment');
```

結果:

```response
┌─domainWithoutWWW('http://user:password@www.example.com:8080/path?query=value#fragment')─┬─domainWithoutWWWRFC('http://user:password@www.example.com:8080/path?query=value#fragment')─┐
│                                                                                         │ example.com                                                                                │
└─────────────────────────────────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────┘
```

### topLevelDomain

URLからトップレベルドメインを抽出します。

``` sql
topLevelDomain(url)
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。

:::note
プロトコルをつけてもつけなくても指定できます。例:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://clickhouse.com/time/
```
:::

**返される値**

- 入力文字列がURLとして解析できる場合はドメイン名。そうでない場合は空の文字列。 [String](../../sql-reference/data-types/string.md)。

**例**

クエリ:

``` sql
SELECT topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk');
```

結果:

``` text
┌─topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')─┐
│ com                                                                │
└────────────────────────────────────────────────────────────────────┘
```

### topLevelDomainRFC

URLからトップレベルドメインを抽出します。[topLevelDomain](#topleveldomain)と似ていますが、RFC 3986に準拠しています。

``` sql
topLevelDomainRFC(url)
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。

:::note
プロトコルをつけてもつけなくても指定できます。例:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://clickhouse.com/time/
```
:::

**返される値**

- 入力文字列がURLとして解析できる場合はドメイン名。そうでない場合は空の文字列。 [String](../../sql-reference/data-types/string.md)。

**例**

クエリ:

``` sql
SELECT topLevelDomain('http://foo:foo%41bar@foo.com'), topLevelDomainRFC('http://foo:foo%41bar@foo.com');
```

結果:

``` text
┌─topLevelDomain('http://foo:foo%41bar@foo.com')─┬─topLevelDomainRFC('http://foo:foo%41bar@foo.com')─┐
│                                                │ com                                               │
└────────────────────────────────────────────────┴───────────────────────────────────────────────────┘
```

### firstSignificantSubdomain

「最初の重要なサブドメイン」を返します。
「最初の重要なサブドメイン」は、`com`、`net`、`org`、または`co`の場合は第二レベルドメイン、その他の場合は第三レベルドメインです。
例えば、`firstSignificantSubdomain ('https://news.clickhouse.com/') = 'clickhouse'、firstSignificantSubdomain ('https://news.clickhouse.com.tr/') = 'clickhouse'`。
「重要でない」第二レベルドメインやその他の実装の詳細は将来変更されることがあります。

**構文**

```sql
firstSignificantSubdomain(url)
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。

**返される値**

- 最初の重要なサブドメイン。 [String](../data-types/string.md)。

**例**

クエリ:

```sql
SELECT firstSignificantSubdomain('http://www.example.com/a/b/c?a=b')
```

結果:

```reference
┌─firstSignificantSubdomain('http://www.example.com/a/b/c?a=b')─┐
│ example                                                       │
└───────────────────────────────────────────────────────────────┘
```

### firstSignificantSubdomainRFC

「最初の重要なサブドメイン」を返します。
「最初の重要なサブドメイン」は、`com`、`net`、`org`、または`co`の場合は第二レベルドメイン、その他の場合は第三レベルドメインです。
例えば、`firstSignificantSubdomain (‘https://news.clickhouse.com/’) = ‘clickhouse’, firstSignificantSubdomain (‘https://news.clickhouse.com.tr/’) = ‘clickhouse’`。
「重要でない」第二レベルドメインやその他の実装の詳細は将来変更されることがあります。
[firstSignficantSubdomain](#firstsignificantsubdomain)に似ていますが、RFC 1034に準拠しています。

**構文**

```sql
firstSignificantSubdomainRFC(url)
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。

**返される値**

- 最初の重要なサブドメイン。 [String](../data-types/string.md)。

**例**

クエリ:

```sql
SELECT
    firstSignificantSubdomain('http://user:password@example.com:8080/path?query=value#fragment'),
    firstSignificantSubdomainRFC('http://user:password@example.com:8080/path?query=value#fragment');
```

結果:

```reference
┌─firstSignificantSubdomain('http://user:password@example.com:8080/path?query=value#fragment')─┬─firstSignificantSubdomainRFC('http://user:password@example.com:8080/path?query=value#fragment')─┐
│                                                                                              │ example                                                                                         │
└──────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### cutToFirstSignificantSubdomain

「最初の重要なサブドメイン」までトップレベルのサブドメインを含むドメインの部分を返します。

**構文**

```sql
cutToFirstSignificantSubdomain(url)
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。

**返される値**

- 可能であれば最初の重要なサブドメインまでのトップレベルのサブドメインを含むドメインの一部を返し、それ以外の場合は空の文字列を返します。 [String](../data-types/string.md)。

**例**

クエリ:

```sql
SELECT
    cutToFirstSignificantSubdomain('https://news.clickhouse.com.tr/'),
    cutToFirstSignificantSubdomain('www.tr'),
    cutToFirstSignificantSubdomain('tr');
```

結果:

```response
┌─cutToFirstSignificantSubdomain('https://news.clickhouse.com.tr/')─┬─cutToFirstSignificantSubdomain('www.tr')─┬─cutToFirstSignificantSubdomain('tr')─┐
│ clickhouse.com.tr                                                 │ tr                                       │                                      │
└───────────────────────────────────────────────────────────────────┴──────────────────────────────────────────┴──────────────────────────────────────┘
```

### cutToFirstSignificantSubdomainRFC

「最初の重要なサブドメイン」までトップレベルのサブドメインを含むドメインの部分を返します。
[cutToFirstSignificantSubdomain](#cuttofirstsignificantsubdomain)に似ていますが、RFC 3986に準拠しています。

**構文**

```sql
cutToFirstSignificantSubdomainRFC(url)
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。

**返される値**

- 可能であれば最初の重要なサブドメインまでのトップレベルのサブドメインを含むドメインの一部を返し、それ以外の場合は空の文字列を返します。 [String](../data-types/string.md)。

**例**

クエリ:

```sql
SELECT
    cutToFirstSignificantSubdomain('http://user:password@example.com:8080'),
    cutToFirstSignificantSubdomainRFC('http://user:password@example.com:8080');
```

結果:

```response
┌─cutToFirstSignificantSubdomain('http://user:password@example.com:8080')─┬─cutToFirstSignificantSubdomainRFC('http://user:password@example.com:8080')─┐
│                                                                         │ example.com                                                                │
└─────────────────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────────────┘
```

### cutToFirstSignificantSubdomainWithWWW

「最初の重要なサブドメイン」までトップレベルのサブドメインを含むドメインの部分を返し、`www`は削除しません。

**構文**

```sql
cutToFirstSignificantSubdomainWithWWW(url)
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。

**返される値**

- 可能であれば最初の重要なサブドメインまでのトップレベルのサブドメインを含むドメインの一部を返し（`www`を含む）、それ以外の場合は空の文字列を返します。 [String](../data-types/string.md)。

**例**

クエリ:

```sql
SELECT
    cutToFirstSignificantSubdomainWithWWW('https://news.clickhouse.com.tr/'),
    cutToFirstSignificantSubdomainWithWWW('www.tr'),
    cutToFirstSignificantSubdomainWithWWW('tr');
```

結果:

```response
┌─cutToFirstSignificantSubdomainWithWWW('https://news.clickhouse.com.tr/')─┬─cutToFirstSignificantSubdomainWithWWW('www.tr')─┬─cutToFirstSignificantSubdomainWithWWW('tr')─┐
│ clickhouse.com.tr                                                        │ www.tr                                          │                                             │
└──────────────────────────────────────────────────────────────────────────┴─────────────────────────────────────────────────┴─────────────────────────────────────────────┘
```

### cutToFirstSignificantSubdomainWithWWWRFC

「最初の重要なサブドメイン」までトップレベルのサブドメインを含むドメインの部分を返し、`www`は削除しません。
[cutToFirstSignificantSubdomainWithWWW](#cuttofirstsignificantsubdomaincustomwithwww)に似ていますが、RFC 3986に準拠しています。

**構文**

```sql
cutToFirstSignificantSubdomainWithWWW(url)
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。

**返される値**

- 可能であれば最初の重要なサブドメインまでのトップレベルのサブドメインを含むドメインの一部を返し（"www"を含む）、それ以外の場合は空の文字列を返します。 [String](../data-types/string.md)。

**例**

クエリ:

```sql
SELECT
    cutToFirstSignificantSubdomainWithWWW('http:%2F%2Fwwwww.nova@mail.ru/economicheskiy'),
    cutToFirstSignificantSubdomainWithWWWRFC('http:%2F%2Fwwwww.nova@mail.ru/economicheskiy');
```

結果:

```response
┌─cutToFirstSignificantSubdomainWithWWW('http:%2F%2Fwwwww.nova@mail.ru/economicheskiy')─┬─cutToFirstSignificantSubdomainWithWWWRFC('http:%2F%2Fwwwww.nova@mail.ru/economicheskiy')─┐
│                                                                                       │ mail.ru                                                                                  │
└───────────────────────────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────────────────────────┘
```

### cutToFirstSignificantSubdomainCustom

最初の重要なサブドメインまでトップレベルのサブドメインを含むドメインの部分を返します。
カスタムの[TLDリスト](https://en.wikipedia.org/wiki/List_of_Internet_top-level_domains)名を受け入れます。
新しいTLDリストが必要な場合やカスタムリストがある場合に便利です。

**設定例**

```xml
<!-- <top_level_domains_path>/var/lib/clickhouse/top_level_domains/</top_level_domains_path> -->
<top_level_domains_lists>
    <!-- https://publicsuffix.org/list/public_suffix_list.dat -->
    <public_suffix_list>public_suffix_list.dat</public_suffix_list>
    <!-- NOTE: path is under top_level_domains_path -->
</top_level_domains_lists>
```

**構文**

``` sql
cutToFirstSignificantSubdomain(url, tld)
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。
- `tld` — カスタムTLDリスト名。 [String](../../sql-reference/data-types/string.md)。

**返される値**

- 最初の重要なサブドメインまでトップレベルのサブドメインを含むドメインの一部。 [String](../../sql-reference/data-types/string.md)。

**例**

クエリ:

```sql
SELECT cutToFirstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list');
```

結果:

```text
┌─cutToFirstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list')─┐
│ foo.there-is-no-such-domain                                                                   │
└───────────────────────────────────────────────────────────────────────────────────────────────┘
```

**関連項目**

- [firstSignificantSubdomain](#firstsignificantsubdomain)。

### cutToFirstSignificantSubdomainCustomRFC

最初の重要なサブドメインまでトップレベルのサブドメインを含むドメインの部分を返します。
カスタム[TLDリスト](https://en.wikipedia.org/wiki/List_of_Internet_top-level_domains)名を受け入れます。
新しいTLDリストが必要な場合やカスタムリストがある場合に便利です。
[cutToFirstSignificantSubdomainCustom](#cuttofirstsignificantsubdomaincustom)に似ていますが、RFC 3986に準拠しています。

**構文**

``` sql
cutToFirstSignificantSubdomainRFC(url, tld)
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。
- `tld` — カスタムTLDリスト名。 [String](../../sql-reference/data-types/string.md)。

**返される値**

- 最初の重要なサブドメインまでトップレベルのサブドメインを含むドメインの一部。 [String](../../sql-reference/data-types/string.md)。

**関連項目**

- [firstSignificantSubdomain](#firstsignificantsubdomain)。

### cutToFirstSignificantSubdomainCustomWithWWW

最初の重要なサブドメインまでトップレベルのサブドメインを含むドメインの部分を返し、`www`は削除しません。
カスタムのTLDリスト名を受け入れます。
新しいTLDリストが必要な場合やカスタムリストがある場合に便利です。

**設定例**

```xml
<!-- <top_level_domains_path>/var/lib/clickhouse/top_level_domains/</top_level_domains_path> -->
<top_level_domains_lists>
    <!-- https://publicsuffix.org/list/public_suffix_list.dat -->
    <public_suffix_list>public_suffix_list.dat</public_suffix_list>
    <!-- NOTE: path is under top_level_domains_path -->
</top_level_domains_lists>
```

**構文**

```sql
cutToFirstSignificantSubdomainCustomWithWWW(url, tld)
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。
- `tld` — カスタムTLDリスト名。 [String](../../sql-reference/data-types/string.md)。

**返される値**

- 最初の重要なサブドメインまでトップレベルのサブドメインを含むドメインの一部（`www`を含む）。 [String](../data-types/string.md)。

**例**

クエリ:

```sql
SELECT cutToFirstSignificantSubdomainCustomWithWWW('www.foo', 'public_suffix_list');
```

結果:

```text
┌─cutToFirstSignificantSubdomainCustomWithWWW('www.foo', 'public_suffix_list')─┐
│ www.foo                                                                      │
└──────────────────────────────────────────────────────────────────────────────┘
```

**関連項目**

- [firstSignificantSubdomain](#firstsignificantsubdomain)。

### cutToFirstSignificantSubdomainCustomWithWWWRFC

最初の重要なサブドメインまでトップレベルのサブドメインを含むドメインの部分を返し、`www`は削除しません。
カスタムのTLDリスト名を受け入れます。
新しいTLDリストが必要な場合やカスタムリストがある場合に便利です。
[cutToFirstSignificantSubdomainCustomWithWWW](#cuttofirstsignificantsubdomaincustomwithwww)に似ていますが、RFC 3986に準拠しています。

**構文**

```sql
cutToFirstSignificantSubdomainCustomWithWWWRFC(url, tld)
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。
- `tld` — カスタムTLDリスト名。 [String](../../sql-reference/data-types/string.md)。

**返される値**

- 最初の重要なサブドメインまでトップレベルのサブドメインを含むドメインの一部（`www`を含む）。 [String](../../sql-reference/data-types/string.md)。

**関連項目**

- [firstSignificantSubdomain](#firstsignificantsubdomain)。

### firstSignificantSubdomainCustom

最初の重要なサブドメインを返します。
カスタムTLDリスト名を受け入れます。
新しいTLDリストが必要な場合やカスタムリストがある場合に便利です。

設定例：

```xml
<!-- <top_level_domains_path>/var/lib/clickhouse/top_level_domains/</top_level_domains_path> -->
<top_level_domains_lists>
    <!-- https://publicsuffix.org/list/public_suffix_list.dat -->
    <public_suffix_list>public_suffix_list.dat</public_suffix_list>
    <!-- NOTE: path is under top_level_domains_path -->
</top_level_domains_lists>
```

**構文**

```sql
firstSignificantSubdomainCustom(url, tld)
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。
- `tld` — カスタムTLDリスト名。 [String](../../sql-reference/data-types/string.md)。

**返される値**

- 最初の重要なサブドメイン。 [String](../../sql-reference/data-types/string.md)。

**例**

クエリ:

```sql
SELECT firstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list');
```

結果:

```text
┌─firstSignificantSubdomainCustom('bar.foo.there-is-no-such-domain', 'public_suffix_list')─┐
│ foo                                                                                      │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**関連項目**

- [firstSignificantSubdomain](#firstsignificantsubdomain)。

### firstSignificantSubdomainCustomRFC

最初の重要なサブドメインを返します。
カスタムTLDリスト名を受け入れます。
新しいTLDリストが必要な場合やカスタムリストがある場合に便利です。
[firstSignificantSubdomainCustom](#firstsignificantsubdomaincustom)に似ていますが、RFC 3986に準拠しています。

**構文**

```sql
firstSignificantSubdomainCustomRFC(url, tld)
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。
- `tld` — カスタムTLDリスト名。 [String](../../sql-reference/data-types/string.md)。

**返される値**

- 最初の重要なサブドメイン。 [String](../../sql-reference/data-types/string.md)。

**関連項目**

- [firstSignificantSubdomain](#firstsignificantsubdomain)。

### port

ポートを返すか、URLにポートが含まれていないか解析できない場合は`default_port`を返します。

**構文**

```sql
port(url [, default_port = 0])
```

**引数**

- `url` — URL。 [String](../data-types/string.md)。
- `default_port` — 返されるデフォルトのポート番号。 [UInt16](../data-types/int-uint.md)。

**返される値**

- URLにポートがない場合、または検証エラーがある場合にはポートまたはデフォルトのポート。 [UInt16](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT port('http://paul@www.example.com:80/');
```

結果:

```response
┌─port('http://paul@www.example.com:80/')─┐
│                                      80 │
└─────────────────────────────────────────┘
```

### portRFC

ポートを返すか、URLにポートが含まれていないか解析できない場合は`default_port`を返します。
[port](#port)に似ていますが、RFC 3986に準拠しています。

**構文**

```sql
portRFC(url [, default_port = 0])
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。
- `default_port` — 返されるデフォルトのポート番号。 [UInt16](../data-types/int-uint.md)。

**返される値**

- URLにポートがない場合、または検証エラーがある場合にはポートまたはデフォルトのポート。 [UInt16](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT
    port('http://user:password@example.com:8080'),
    portRFC('http://user:password@example.com:8080');
```

結果:

```resposne
┌─port('http://user:password@example.com:8080')─┬─portRFC('http://user:password@example.com:8080')─┐
│                                             0 │                                             8080 │
└───────────────────────────────────────────────┴──────────────────────────────────────────────────┘
```

### path

クエリ文字列を含まないパスを返します。

例: `/top/news.html`。

### pathFull

上記と同じですが、クエリ文字列とフラグメントを含みます。

例: `/top/news.html?page=2#comments`。

### protocol

URLからプロトコルを抽出します。

**構文**

```sql
protocol(url)
```

**引数**

- `url` — プロトコルを抽出するURL。 [String](../data-types/string.md)。

**返される値**

- プロトコル、または特定できない場合は空の文字列。 [String](../data-types/string.md)。

**例**

クエリ:

```sql
SELECT protocol('https://clickhouse.com/');
```

結果:

```response
┌─protocol('https://clickhouse.com/')─┐
│ https                               │
└─────────────────────────────────────┘
```

### queryString

先頭の質問符や`#`およびそれ以降を除いたクエリ文字列を返します。

例: `page=1&lr=213`。

### fragment

先頭のハッシュ記号を除いたフラグメント識別子を返します。

### queryStringAndFragment

クエリ文字列とフラグメント識別子を返します。

例: `page=1#29390`。

### extractURLParameter(url, name)

URLに`name`という名前のパラメータが存在する場合、その値を返し、存在しない場合は空の文字列を返します。
同じ名前のパラメータが複数ある場合は、最初の出現を返します。
この関数は、`url`パラメータ内のパラメータが`name`引数と同じ方法でエンコードされていると仮定します。

### extractURLParameters(url)

URLパラメータに対応する`name=value`文字列の配列を返します。
値はデコードされません。

### extractURLParameterNames(url)

URLパラメータの名前に対応する名前文字列の配列を返します。
値はデコードされません。

### URLHierarchy(url)

パスとクエリ文字列の中でURLを後方から`/`、`?`で切り詰めた配列を返します。
連続した区切り文字は一つとしてカウントされます。
切り取りは連続する区切り文字の後の位置で行われます。

### URLPathHierarchy(url)

上記と同じですが、結果にはプロトコルとホストが含まれません。 `/`要素（ルート）は含まれません。

``` text
URLPathHierarchy('https://example.com/browse/CONV-6788') =
[
    '/browse/',
    '/browse/CONV-6788'
]
```

### encodeURLComponent(url)

エンコードされたURLを返します。

例:

``` sql
SELECT encodeURLComponent('http://127.0.0.1:8123/?query=SELECT 1;') AS EncodedURL;
```

``` text
┌─EncodedURL───────────────────────────────────────────────┐
│ http%3A%2F%2F127.0.0.1%3A8123%2F%3Fquery%3DSELECT%201%3B │
└──────────────────────────────────────────────────────────┘
```

### decodeURLComponent(url)

デコードされたURLを返します。

例:

``` sql
SELECT decodeURLComponent('http://127.0.0.1:8123/?query=SELECT%201%3B') AS DecodedURL;
```

``` text
┌─DecodedURL─────────────────────────────┐
│ http://127.0.0.1:8123/?query=SELECT 1; │
└────────────────────────────────────────┘
```

### encodeURLFormComponent(url)

エンコードされたURLを返します。rfc-1866に従い、空白(` `)がプラス(`+`)としてエンコードされます。

例:

``` sql
SELECT encodeURLFormComponent('http://127.0.0.1:8123/?query=SELECT 1 2+3') AS EncodedURL;
```

``` text
┌─EncodedURL────────────────────────────────────────────────┐
│ http%3A%2F%2F127.0.0.1%3A8123%2F%3Fquery%3DSELECT+1+2%2B3 │
└───────────────────────────────────────────────────────────┘
```

### decodeURLFormComponent(url)

デコードされたURLを返します。rfc-1866に従い、通常のプラス(`+`)が空白(` `)としてデコードされます。

例:

``` sql
SELECT decodeURLFormComponent('http://127.0.0.1:8123/?query=SELECT%201+2%2B3') AS DecodedURL;
```

``` text
┌─DecodedURL────────────────────────────────┐
│ http://127.0.0.1:8123/?query=SELECT 1 2+3 │
└───────────────────────────────────────────┘
```

### netloc

URLからネットワークロケリティ（`username:password@host:port`）を抽出します。

**構文**

``` sql
netloc(url)
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。

**返される値**

- `username:password@host:port`。 [String](../data-types/string.md)。

**例**

クエリ:

``` sql
SELECT netloc('http://paul@www.example.com:80/');
```

結果:

``` text
┌─netloc('http://paul@www.example.com:80/')─┐
│ paul@www.example.com:80                   │
└───────────────────────────────────────────┘
```

## URLの一部を削除する関数

URLに類似するものがない場合、URLは変更されません。

### cutWWW

URLのドメインから`www.`を削除します（存在する場合）。

### cutQueryString

クエリ文字列を含む質問符を削除します。

### cutFragment

フラグメント識別子を含む番号記号を削除します。

### cutQueryStringAndFragment

クエリ文字列とフラグメント識別子を含む質問符と番号記号を削除します。

### cutURLParameter(url, name)

URLから`name`というパラメータを削除します（存在する場合）。
この関数は、パラメータ名の文字をエンコードまたはデコードしません。例えば、`Client ID`と`Client%20ID`は異なるパラメータ名として扱われます。

**構文**

``` sql
cutURLParameter(url, name)
```

**引数**

- `url` — URL。 [String](../../sql-reference/data-types/string.md)。
- `name` — URLパラメータの名前。 [String](../../sql-reference/data-types/string.md)またはStringsの[Array](../../sql-reference/data-types/array.md)。

**返される値**

- `name` URLパラメータを削除したURL。 [String](../data-types/string.md)。

**例**

クエリ:

``` sql
SELECT
    cutURLParameter('http://bigmir.net/?a=b&c=d&e=f#g', 'a') as url_without_a,
    cutURLParameter('http://bigmir.net/?a=b&c=d&e=f#g', ['c', 'e']) as url_without_c_and_e;
```

結果:

``` text
┌─url_without_a────────────────┬─url_without_c_and_e──────┐
│ http://bigmir.net/?c=d&e=f#g │ http://bigmir.net/?a=b#g │
└──────────────────────────────┴──────────────────────────┘
```
