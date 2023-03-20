# URL函数 {#urlhan-shu}

所有这些功能都不遵循RFC。它们被最大程度简化以提高性能。

## URL截取函数 {#urljie-qu-han-shu}

如果URL中没有要截取的内容则返回空字符串。

### 协议 {#protocol}

返回URL的协议。例如： http、ftp、mailto、magnet…

### 域 {#domain}

获取域名。

### domainwithoutww {#domainwithoutwww}

返回域名并删除第一个’www.’。

### topLevelDomain {#topleveldomain}

返回顶级域名。例如：.ru。

### 第一重要的元素分区域 {#firstsignificantsubdomain}

返回«第一个有效子域名»。这并不是一个标准概念，仅用于Yandex.Metrica。如果顶级域名为’com’，‘net’，‘org’或者‘co’则第一个有效子域名为二级域名。否则则返回三级域名。例如，irstSignificantSubdomain (’https://news.yandex.ru/‘) = ’yandex’， firstSignificantSubdomain (‘https://news.yandex.com.tr/’) = ‘yandex’。一些实现细节在未来可能会进行改变。

### cutToFirstSignificantSubdomain {#cuttofirstsignificantsubdomain}

返回包含顶级域名与第一个有效子域名之间的内容（请参阅上面的内容）。

例如， `cutToFirstSignificantSubdomain('https://news.yandex.com.tr/') = 'yandex.com.tr'`.

### 路径 {#path}

返回URL路径。例如：`/top/news.html`，不包含请求参数。

### pathFull {#pathfull}

与上面相同，但包括请求参数和fragment。例如：/top/news.html?page=2#comments

### 查询字符串 {#querystring}

返回请求参数。例如：page=1&lr=213。请求参数不包含问号已经# 以及# 之后所有的内容。

### 片段 {#fragment}

返回URL的fragment标识。fragment不包含#。

### querystring andfragment {#querystringandfragment}

返回请求参数和fragment标识。例如：page=1#29390。

### extractURLParameter(URL,name) {#extracturlparameterurl-name}

返回URL请求参数中名称为’name’的参数。如果不存在则返回一个空字符串。如果存在多个匹配项则返回第一个相匹配的。此函数假设参数名称与参数值在url中的编码方式相同。

### extractURLParameters(URL) {#extracturlparametersurl}

返回一个数组，其中以name=value的字符串形式返回url的所有请求参数。不以任何编码解析任何内容。

### extractURLParameterNames(URL) {#extracturlparameternamesurl}

返回一个数组，其中包含url的所有请求参数的名称。不以任何编码解析任何内容。

### URLHierarchy(URL) {#urlhierarchyurl}

返回一个数组，其中包含以/切割的URL的所有内容。？将被包含在URL路径以及请求参数中。连续的分割符号被记为一个。

### Urlpathhierarchy(URL) {#urlpathhierarchyurl}

与上面相同，但结果不包含协议和host部分。 /element(root)不包括在内。该函数用于在Yandex.Metric中实现导出URL的树形结构。

    URLPathHierarchy('https://example.com/browse/CONV-6788') =
    [
        '/browse/',
        '/browse/CONV-6788'
    ]

### decodeURLComponent(URL) {#decodeurlcomponenturl}

返回已经解码的URL。
例如:

``` sql
SELECT decodeURLComponent('http://127.0.0.1:8123/?query=SELECT%201%3B') AS DecodedURL;
```

    ┌─DecodedURL─────────────────────────────┐
    │ http://127.0.0.1:8123/?query=SELECT 1; │
    └────────────────────────────────────────┘

## 删除URL中的部分内容 {#shan-chu-urlzhong-de-bu-fen-nei-rong}

如果URL中不包含指定的部分，则URL不变。

### cutWWW {#cutwww}

删除开始的第一个’www.’。

### cutQueryString {#cutquerystring}

删除请求参数。问号也将被删除。

### cutFragment {#cutfragment}

删除fragment标识。#同样也会被删除。

### cutquerystring andfragment {#cutquerystringandfragment}

删除请求参数以及fragment标识。问号以及#也会被删除。

### cutURLParameter(URL,name) {#cuturlparameterurl-name}

删除URL中名称为’name’的参数。改函数假设参数名称以及参数值经过URL相同的编码。

[来源文章](https://clickhouse.com/docs/en/query_language/functions/url_functions/) <!--hide-->
