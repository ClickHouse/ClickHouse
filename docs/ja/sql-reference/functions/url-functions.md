---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: "Url\u306E\u64CD\u4F5C"
---

# Urlを操作するための関数 {#functions-for-working-with-urls}

これらの関数はすべてRFCに従っていません。 それらは改善された性能のために最大限に簡単にされる。

## URLの一部を抽出する関数 {#functions-that-extract-parts-of-a-url}

関連する部分がURLにない場合は、空の文字列が返されます。

### プロトコル {#protocol}

URLからプロトコルを抽出します。

Examples of typical returned values: http, https, ftp, mailto, tel, magnet…

### ドメイン {#domain}

URLからホスト名を抽出します。

``` sql
domain(url)
```

**パラメータ**

-   `url` — URL. Type: [文字列](../../sql-reference/data-types/string.md).

URLは、スキームの有無にかかわらず指定できます。 例:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://yandex.com/time/
```

これらの例では、 `domain` 関数は、次の結果を返します:

``` text
some.svn-hosting.com
some.svn-hosting.com
yandex.com
```

**戻り値**

-   ホスト名。 ClickHouseが入力文字列をURLとして解析できる場合。
-   空の文字列。 ClickHouseが入力文字列をURLとして解析できない場合。

タイプ: `String`.

**例**

``` sql
SELECT domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')
```

``` text
┌─domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')─┐
│ some.svn-hosting.com                                   │
└────────────────────────────────────────────────────────┘
```

### domainWithoutWWW {#domainwithoutwww}

ドメインを返し、複数のドメインを削除しません ‘www.’ それの始めから、存在する場合。

### トップレベルドメイン {#topleveldomain}

URLからトップレベルドメインを抽出します。

``` sql
topLevelDomain(url)
```

**パラメータ**

-   `url` — URL. Type: [文字列](../../sql-reference/data-types/string.md).

URLは、スキームの有無にかかわらず指定できます。 例:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://yandex.com/time/
```

**戻り値**

-   ドメイン名。 ClickHouseが入力文字列をURLとして解析できる場合。
-   空の文字列。 ClickHouseが入力文字列をURLとして解析できない場合。

タイプ: `String`.

**例**

``` sql
SELECT topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')
```

``` text
┌─topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')─┐
│ com                                                                │
└────────────────────────────────────────────────────────────────────┘
```

### firstSignificantSubdomain {#firstsignificantsubdomain}

を返す。 “first significant subdomain”. これはYandexに固有の非標準的な概念です。メトリカ 最初の重要なサブドメインは、次の場合にセカンドレベルドメインです ‘com’, ‘net’, ‘org’,または ‘co’. それ以外の場合は、サードレベルドメインです。 例えば, `firstSignificantSubdomain (‘https://news.yandex.ru/’) = ‘yandex’, firstSignificantSubdomain (‘https://news.yandex.com.tr/’) = ‘yandex’`. のリスト “insignificant” セカンドレベルドメイ

### cutToFirstSignificantSubdomain {#cuttofirstsignificantsubdomain}

トップレベルのサブドメインを含むドメインの一部を返します。 “first significant subdomain” （上記の説明を参照）。

例えば, `cutToFirstSignificantSubdomain('https://news.yandex.com.tr/') = 'yandex.com.tr'`.

### パス {#path}

パスを返します。 例: `/top/news.html` パスにはクエリ文字列は含まれません。

### パスフル {#pathfull}

上記と同じですが、クエリ文字列とフラグメントを含みます。 例:/top/news.html?ページ=2#コメント

### クエリ文字列 {#querystring}

クエリ文字列を返します。 例:ページ=1&lr=213. query-stringには、最初の疑問符だけでなく、#と#の後のすべても含まれていません。

### 断片 {#fragment}

フラグメント識別子を返します。 fragmentには初期ハッシュ記号は含まれません。

### queryStringAndFragment {#querystringandfragment}

クエリ文字列とフラグメント識別子を返します。 例:ページ=1#29390.

### extractURLParameter(URL,名前) {#extracturlparameterurl-name}

の値を返します。 ‘name’ URLにパラメータがある場合。 それ以外の場合は、空の文字列です。 この名前のパラメーターが多い場合は、最初に出現するパラメーターを返します。 この関数は、パラメータ名が渡された引数とまったく同じ方法でURLにエンコードされるという前提の下で機能します。

### extractURLParameters(URL) {#extracturlparametersurl}

URLパラメーターに対応するname=value文字列の配列を返します。 値はどのような方法でもデコードされません。

### extractURLParameterNames(URL) {#extracturlparameternamesurl}

URLパラメーターの名前に対応する名前文字列の配列を返します。 値はどのような方法でもデコードされません。

### URLHierarchy(URL) {#urlhierarchyurl}

URLを含む配列を返します。 パスとクエリ文字列で。 連続セパレータ文字として数えます。 カットは、すべての連続した区切り文字の後の位置に行われます。

### URLPathHierarchy(URL) {#urlpathhierarchyurl}

上記と同じですが、結果にプロトコルとホストはありません。 要素(ルート)は含まれません。 例：関数は、ツリーを実装するために使用されるYandexの中のURLを報告します。 メートル法

``` text
URLPathHierarchy('https://example.com/browse/CONV-6788') =
[
    '/browse/',
    '/browse/CONV-6788'
]
```

### decodeURLComponent(URL) {#decodeurlcomponenturl}

デコードしたURLを返します。
例:

``` sql
SELECT decodeURLComponent('http://127.0.0.1:8123/?query=SELECT%201%3B') AS DecodedURL;
```

``` text
┌─DecodedURL─────────────────────────────┐
│ http://127.0.0.1:8123/?query=SELECT 1; │
└────────────────────────────────────────┘
```

## URLの一部を削除する関数 {#functions-that-remove-part-of-a-url}

URLに類似のものがない場合、URLは変更されません。

### cutWWW {#cutwww}

複数を削除しません ‘www.’ URLのドメインの先頭から存在する場合。

### cutQueryString {#cutquerystring}

クエリ文字列を削除します。 疑問符も削除されます。

### カットフラグメント {#cutfragment}

フラグメント識別子を削除します。 番号記号も削除されます。

### cutQueryStringAndFragment {#cutquerystringandfragment}

クエリ文字列とフラグメント識別子を削除します。 疑問符と番号記号も削除されます。

### cutURLParameter(URL,名前) {#cuturlparameterurl-name}

を削除します。 ‘name’ URLパラメータが存在する場合。 この関数は、パラメータ名が渡された引数とまったく同じ方法でURLにエンコードされるという前提の下で機能します。

[元の記事](https://clickhouse.com/docs/en/query_language/functions/url_functions/) <!--hide-->
