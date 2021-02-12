---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: arrayJoin
---

# arrayjoin işlevi {#functions_arrayjoin}

Bu çok sıradışı bir işlevdir.

Normal işlevler bir dizi satırı değiştirmez, ancak her satırdaki değerleri değiştirir (harita).
Toplama işlevleri bir dizi satırı sıkıştırır (katlayın veya azaltın).
Bu ‘arrayJoin’ işlev her satırı alır ve bir dizi satır oluşturur (açılır).

Bu işlev bir diziyi bağımsız değişken olarak alır ve kaynak satırı dizideki öğe sayısı için birden çok satıra yayar.
Sütunlardaki tüm değerler, bu işlevin uygulandığı sütundaki değerler dışında kopyalanır; karşılık gelen dizi değeri ile değiştirilir.

Bir sorgu birden çok kullanabilirsiniz `arrayJoin` işlevler. Bu durumda, dönüşüm birden çok kez gerçekleştirilir.

Daha geniş olanaklar sağlayan SELECT sorgusunda dizi birleştirme sözdizimini not alın.

Örnek:

``` sql
SELECT arrayJoin([1, 2, 3] AS src) AS dst, 'Hello', src
```

``` text
┌─dst─┬─\'Hello\'─┬─src─────┐
│   1 │ Hello     │ [1,2,3] │
│   2 │ Hello     │ [1,2,3] │
│   3 │ Hello     │ [1,2,3] │
└─────┴───────────┴─────────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/array_join/) <!--hide-->
