---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: "URL'ler ile \xE7al\u0131\u015Fma"
---

# URL'ler ile çalışmak için işlevler {#functions-for-working-with-urls}

Tüm bu işlevler RFC'Yİ takip etmez. Geliştirilmiş performans için maksimum derecede basitleştirilmişlerdir.

## Bir URL'nin bölümlerini Ayıklayan işlevler {#functions-that-extract-parts-of-a-url}

İlgili bölüm bir URL'de yoksa, boş bir dize döndürülür.

### protokol {#protocol}

Protokolü bir URL'den ayıklar.

Examples of typical returned values: http, https, ftp, mailto, tel, magnet…

### etki {#domain}

Ana bilgisayar adını bir URL'den ayıklar.

``` sql
domain(url)
```

**Parametre**

-   `url` — URL. Type: [Dize](../../sql-reference/data-types/string.md).

URL, bir şema ile veya şema olmadan belirtilebilir. Örnekler:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://yandex.com/time/
```

Bu örnekler için, `domain` işlev aşağıdaki sonuçları döndürür:

``` text
some.svn-hosting.com
some.svn-hosting.com
yandex.com
```

**Döndürülen değerler**

-   Adı ana. ClickHouse giriş dizesini bir URL olarak ayrıştırırsa.
-   Boş dize. ClickHouse giriş dizesini bir URL olarak ayrıştıramazsa.

Tür: `String`.

**Örnek**

``` sql
SELECT domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')
```

``` text
┌─domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')─┐
│ some.svn-hosting.com                                   │
└────────────────────────────────────────────────────────┘
```

### domainWithoutWWW {#domainwithoutwww}

Etki alanını döndürür ve birden fazla kaldırır ‘www.’ başlangıcına, eğer var dan.

### topLevelDomain {#topleveldomain}

Üst düzey etki alanını bir URL'den ayıklar.

``` sql
topLevelDomain(url)
```

**Parametre**

-   `url` — URL. Type: [Dize](../../sql-reference/data-types/string.md).

URL, bir şema ile veya şema olmadan belirtilebilir. Örnekler:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://yandex.com/time/
```

**Döndürülen değerler**

-   Etki alanı adı. ClickHouse giriş dizesini bir URL olarak ayrıştırırsa.
-   Boş dize. ClickHouse giriş dizesini bir URL olarak ayrıştıramazsa.

Tür: `String`.

**Örnek**

``` sql
SELECT topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')
```

``` text
┌─topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')─┐
│ com                                                                │
└────────────────────────────────────────────────────────────────────┘
```

### firstSignificantSubdomain {#firstsignificantsubdomain}

Ret theur thens the “first significant subdomain”. Bu, Yandex'e özgü standart olmayan bir kavramdır.Metrica. İlk önemli alt etki alanı ise ikinci düzey bir etki alanıdır ‘com’, ‘net’, ‘org’, veya ‘co’. Aksi takdirde, üçüncü düzey bir alandır. Mesela, `firstSignificantSubdomain (‘https://news.yandex.ru/’) = ‘yandex’, firstSignificantSubdomain (‘https://news.yandex.com.tr/’) = ‘yandex’`. Listesi “insignificant” ikinci düzey etki alanları ve diğer uygulama ayrıntıları gelecekte değişebilir.

### cutToFirstSignificantSubdomain {#cuttofirstsignificantsubdomain}

En üst düzey alt etki alanlarını içeren etki alanının bir bölümünü döndürür. “first significant subdomain” (yukarıdaki açıklamaya bakınız).

Mesela, `cutToFirstSignificantSubdomain('https://news.yandex.com.tr/') = 'yandex.com.tr'`.

### yol {#path}

Yolu döndürür. Örnek: `/top/news.html` Yol sorgu dizesini içermez.

### pathFull {#pathfull}

Yukarıdaki ile aynı, ancak sorgu dizesi ve parça dahil. Örnek: / top / haberler.html?Sayfa = 2 # yorumlar

### queryString {#querystring}

Sorgu dizesini döndürür. Örnek: Sayfa = 1 & lr = 213. sorgu dizesi, ilk soru işaretinin yanı sıra # ve # sonrası her şeyi içermez.

### parça {#fragment}

Parça tanımlayıcısını döndürür. fragment ilk karma sembolü içermez.

### queryStringAndFragment {#querystringandfragment}

Sorgu dizesini ve parça tanımlayıcısını döndürür. Örnek: Sayfa = 1#29390.

### extractURLParameter (URL, isim) {#extracturlparameterurl-name}

Değerini döndürür ‘name’ varsa, URL'DEKİ parametre. Aksi takdirde, boş bir dize. Bu ada sahip birçok parametre varsa, ilk oluşumu döndürür. Bu işlev, parametre adının URL'de geçirilen bağımsız değişkenle aynı şekilde kodlandığı varsayımı altında çalışır.

### extractURLParameters (URL) {#extracturlparametersurl}

Bir dizi döndürür name = URL parametrelerine karşılık gelen değer dizeleri. Değerler hiçbir şekilde deşifre edilmez.

### extractURLParameterNames(URL) {#extracturlparameternamesurl}

URL parametrelerinin adlarına karşılık gelen bir dizi ad dizesi döndürür. Değerler hiçbir şekilde deşifre edilmez.

### URLHierarchy(URL) {#urlhierarchyurl}

Sonunda/,? simgeleriyle kesilen URL'yi içeren bir dizi döndürür yol ve sorgu dizesinde. Ardışık ayırıcı karakterler bir olarak sayılır. Kesim, tüm ardışık ayırıcı karakterlerden sonra pozisyonda yapılır.

### URLPathHierarchy(URL) {#urlpathhierarchyurl}

Yukarıdaki ile aynı, ancak sonuçta protokol ve ana bilgisayar olmadan. / Eleman (kök) dahil değildir. Örnek: işlev, yandex'te URL'yi ağaç raporları uygulamak için kullanılır. Ölçü.

``` text
URLPathHierarchy('https://example.com/browse/CONV-6788') =
[
    '/browse/',
    '/browse/CONV-6788'
]
```

### decodeURLComponent (URL) {#decodeurlcomponenturl}

Çözülmüş URL'yi döndürür.
Örnek:

``` sql
SELECT decodeURLComponent('http://127.0.0.1:8123/?query=SELECT%201%3B') AS DecodedURL;
```

``` text
┌─DecodedURL─────────────────────────────┐
│ http://127.0.0.1:8123/?query=SELECT 1; │
└────────────────────────────────────────┘
```

## URL'nin bir bölümünü kaldıran işlevler {#functions-that-remove-part-of-a-url}

URL'de benzer bir şey yoksa, URL değişmeden kalır.

### cutWWW {#cutwww}

Birden fazla kaldırır ‘www.’ varsa, URL'nin etki alanının başından itibaren.

### cutQueryString {#cutquerystring}

Sorgu dizesini kaldırır. Soru işareti de kaldırılır.

### cutFragment {#cutfragment}

Parça tanımlayıcısını kaldırır. Sayı işareti de kaldırılır.

### cutQueryStringAndFragment {#cutquerystringandfragment}

Sorgu dizesini ve parça tanımlayıcısını kaldırır. Soru işareti ve sayı işareti de kaldırılır.

### cutURLParameter (URL, isim) {#cuturlparameterurl-name}

Kaldırır ‘name’ Varsa URL parametresi. Bu işlev, parametre adının URL'de geçirilen bağımsız değişkenle aynı şekilde kodlandığı varsayımı altında çalışır.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/url_functions/) <!--hide-->
