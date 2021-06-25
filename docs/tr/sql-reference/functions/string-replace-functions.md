---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 42
toc_title: "Dizelerde de\u011Fi\u015Ftirilmesi i\xE7in"
---

# Dizelerde arama ve değiştirme işlevleri {#functions-for-searching-and-replacing-in-strings}

## replaceOne(Samanlık, desen, değiştirme) {#replaceonehaystack-pattern-replacement}

Varsa, ilk oluş replacesumun yerini ‘pattern’ substring içinde ‘haystack’ ile... ‘replacement’ dize.
Ahiret, ‘pattern’ ve ‘replacement’ sabitleri olması gerekir.

## replaceAll (Samanlık, desen, değiştirme), değiştirin (Samanlık, desen, değiştirme) {#replaceallhaystack-pattern-replacement-replacehaystack-pattern-replacement}

Tüm oluşumları değiştirir ‘pattern’ substring içinde ‘haystack’ ile... ‘replacement’ dize.

## replaceRegexpOne(Samanlık, desen, değiştirme) {#replaceregexponehaystack-pattern-replacement}

Kullanarak değiştirme ‘pattern’ düzenli ifade. Re2 düzenli ifade.
Varsa, yalnızca ilk oluşumu değiştirir.
Bir desen olarak belirtilebilir ‘replacement’. Bu desen değiştirmeleri içerebilir `\0-\9`.
İkame `\0` tüm düzenli ifadeyi içerir. İkameler `\1-\9` alt desene karşılık gelir numbers.To use the `\` bir şablondaki karakter, kullanarak kaçış `\`.
Ayrıca, bir dize literalinin ekstra bir kaçış gerektirdiğini unutmayın.

Örnek 1. Tarihi Amerikan format convertingına dönüştürme:

``` sql
SELECT DISTINCT
    EventDate,
    replaceRegexpOne(toString(EventDate), '(\\d{4})-(\\d{2})-(\\d{2})', '\\2/\\3/\\1') AS res
FROM test.hits
LIMIT 7
FORMAT TabSeparated
```

``` text
2014-03-17      03/17/2014
2014-03-18      03/18/2014
2014-03-19      03/19/2014
2014-03-20      03/20/2014
2014-03-21      03/21/2014
2014-03-22      03/22/2014
2014-03-23      03/23/2014
```

Örnek 2. Bir dize on kez kopyalama:

``` sql
SELECT replaceRegexpOne('Hello, World!', '.*', '\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0') AS res
```

``` text
┌─res────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World! │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## replaceRegexpAll(Samanlık, desen, değiştirme) {#replaceregexpallhaystack-pattern-replacement}

Bu aynı şeyi yapar, ancak tüm oluşumların yerini alır. Örnek:

``` sql
SELECT replaceRegexpAll('Hello, World!', '.', '\\0\\0') AS res
```

``` text
┌─res────────────────────────┐
│ HHeelllloo,,  WWoorrlldd!! │
└────────────────────────────┘
```

Normal bir ifade boş bir alt dize üzerinde çalıştıysa, bir istisna olarak, değiştirme birden çok kez yapılmaz.
Örnek:

``` sql
SELECT replaceRegexpAll('Hello, World!', '^', 'here: ') AS res
```

``` text
┌─res─────────────────┐
│ here: Hello, World! │
└─────────────────────┘
```

## regexpQuoteMeta (s) {#regexpquotemetas}

İşlev, dizedeki bazı önceden tanımlanmış karakterlerden önce bir ters eğik çizgi ekler.
Önceden tanımlanmış karakterler: ‘0’, ‘\\’, ‘\|’, ‘(’, ‘)’, ‘^’, ‘$’, ‘.’, ‘\[’, '\]', ‘?’, '\*‘,’+‘,’{‘,’:‘,’-'.
Bu uygulama biraz re2::RE2::QuoteMeta farklıdır. Sıfır bayttan 00 yerine \\0 olarak çıkar ve yalnızca gerekli karakterlerden kaçar.
Daha fazla bilgi için bağlantıya bakın: [RE2](https://github.com/google/re2/blob/master/re2/re2.cc#L473)

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/string_replace_functions/) <!--hide-->
