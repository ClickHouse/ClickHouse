---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 54
toc_title: "Url\u306E\u64CD\u4F5C"
---

# Urlを操作するための関数 {#functions-for-working-with-urls}

これらの関数はすべてrfcに従いません。 それらは改善された性能のために最大限に簡単である。

## URLの一部を抽出する関数 {#functions-that-extract-parts-of-a-url}

関連する部分がurlに存在しない場合は、空の文字列が返されます。

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

-   ホスト名。 clickhouseが入力文字列をurlとして解析できる場合。
-   空の文字列。 clickhouseが入力文字列をurlとして解析できない場合。

タイプ: `String`.

**例えば**

``` sql
SELECT domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')
```

``` text
┌─domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')─┐
│ some.svn-hosting.com                                   │
└────────────────────────────────────────────────────────┘
```

### ドメインウィズなしwww {#domainwithoutwww}

ドメインを返し、複数のドメインを削除します ‘www.’ それの初めから、存在する場合。

### topleveldomaincomment {#topleveldomain}

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

-   ドメイン名。 clickhouseが入力文字列をurlとして解析できる場合。
-   空の文字列。 clickhouseが入力文字列をurlとして解析できない場合。

タイプ: `String`.

**例えば**

``` sql
SELECT topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')
```

``` text
┌─topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')─┐
│ com                                                                │
└────────────────────────────────────────────────────────────────────┘
```

### 最初のサブドメイン {#firstsignificantsubdomain}

を返します “first significant subdomain”. これはYandex固有の非標準的な概念です。メトリカ 最初の重要なサブドメインは、セカンドレベルドメインです。 ‘com’, ‘net’, ‘org’、または ‘co’. それ以外の場合は、サードレベルのドメインです。 例えば, `firstSignificantSubdomain (‘https://news.yandex.ru/’) = ‘yandex’, firstSignificantSubdomain (‘https://news.yandex.com.tr/’) = ‘yandex’`. のリスト “insignificant” 二次レベルドメインおよびその他の実施内容に変化する可能性があります。

### cutToFirstSignificantSubdomain {#cuttofirstsignificantsubdomain}

トップレベルのサブドメインを含むドメインの部分を返します。 “first significant subdomain” （上記の説明を参照）。

例えば, `cutToFirstSignificantSubdomain('https://news.yandex.com.tr/') = 'yandex.com.tr'`.

### パス {#path}

パスを返します。 例えば: `/top/news.html` パスにはクエリ文字列は含まれません。

### pathFull {#pathfull}

上記と同じですが、クエリ文字列とフラグメントを含みます。 例：/トップ/ニュース。html?ページ=2\#コメント

### クエリ文字列 {#querystring}

クエリ文字列を返します。 例：ページ=1＆lr=213。 query-stringには、最初の疑問符と\#と\#後のすべてが含まれていません。

### 断片 {#fragment}

フラグメント識別子を返します。 fragmentには、最初のハッシュ記号は含まれません。

### queryStringAndFragment {#querystringandfragment}

クエリ文字列とフラグメント識別子を返します。 例:ページ=1\#29390.

### extractURLParameter(URL,名前) {#extracturlparameterurl-name}

の値を返します ‘name’ URL内にパラメータが存在する場合。 それ以外の場合は、空の文字列。 この名前のパラメータが多数ある場合は、最初のオカレンスが返されます。 この関数は、パラメータ名が渡された引数とまったく同じ方法でURLにエンコードされるという前提の下で機能します。

### extractURLParameters(URL) {#extracturlparametersurl}

URLパラメータに対応するname=value文字列の配列を返します。 値は決してデコードされません。

### extractURLParameterNames(URL) {#extracturlparameternamesurl}

URLパラメータの名前に対応する名前文字列の配列を返します。 値は決してデコードされません。

### URLHierarchy(URL) {#urlhierarchyurl}

最後に/,?記号で切り捨てられたurlを含む配列を返します。 パスとクエリ文字列で。 連続セパレータ文字として数えます。 カットは、すべての連続した区切り文字の後の位置に作られています。

### URLPathHierarchy(URL) {#urlpathhierarchyurl}

上記と同じですが、結果のプロトコルとホストはありません。 要素(ルート)は含まれません。 例：この関数は、ツリーを実装するために使用されるyandexのurlを報告します。 メトリック。

``` text
URLPathHierarchy('https://example.com/browse/CONV-6788') =
[
    '/browse/',
    '/browse/CONV-6788'
]
```

### decodeURLComponent(URL) {#decodeurlcomponenturl}

復号化されたurlを返します。
例えば:

``` sql
SELECT decodeURLComponent('http://127.0.0.1:8123/?query=SELECT%201%3B') AS DecodedURL;
```

``` text
┌─DecodedURL─────────────────────────────┐
│ http://127.0.0.1:8123/?query=SELECT 1; │
└────────────────────────────────────────┘
```

## URLの一部を削除する関数。 {#functions-that-remove-part-of-a-url}

URLに類似したものがない場合、URLは変更されません。

### cutWWW {#cutwww}

一つ以下を削除します ‘www.’ URLのドメインの先頭から、存在する場合。

### cutQueryString {#cutquerystring}

クエリ文字列を削除します。 疑問符も削除されます。

### カットフラグメント {#cutfragment}

フラグメント識別子を削除します。 番号記号も削除されます。

### cutQueryStringAndFragment {#cutquerystringandfragment}

クエリ文字列とフラグメント識別子を削除します。 疑問符と番号記号も削除されます。

### cutURLParameter(URL,名前) {#cuturlparameterurl-name}

削除する ‘name’ URLパラメーターがある場合。 この関数は、パラメータ名が渡された引数とまったく同じ方法でURLにエンコードされるという前提の下で機能します。

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/url_functions/) <!--hide-->
