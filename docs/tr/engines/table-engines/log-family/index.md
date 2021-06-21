---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "G\xFCnl\xFCk Aile"
toc_priority: 29
toc_title: "Giri\u015F"
---

# Log Engine Ailesi {#log-engine-family}

Bu motorlar, birçok küçük tabloyu (yaklaşık 1 milyon satıra kadar) hızlı bir şekilde yazmanız ve daha sonra bir bütün olarak okumanız gerektiğinde senaryolar için geliştirilmiştir.

Ailenin motorları:

-   [StripeLog](stripelog.md)
-   [Günlük](log.md)
-   [TinyLog](tinylog.md)

## Ortak Özellikler {#common-properties}

Motorlar:

-   Verileri bir diskte saklayın.

-   Yazarken dosyanın sonuna veri ekleyin.

-   Eşzamanlı veri erişimi için destek kilitleri.

    Sırasında `INSERT` sorgular, tablo kilitlenir ve veri okumak ve yazmak için diğer sorgular hem tablonun kilidini açmak için bekler. Veri yazma sorguları varsa, herhangi bir sayıda veri okuma sorguları aynı anda gerçekleştirilebilir.

-   Destek yok [mutasyon](../../../sql-reference/statements/alter.md#alter-mutations) harekat.

-   Dizinleri desteklemez.

    Bu demektir ki `SELECT` veri aralıkları için sorgular verimli değildir.

-   Atomik veri yazmayın.

    Bir şey yazma işlemini bozarsa, örneğin anormal sunucu kapatma gibi bozuk verilerle bir tablo alabilirsiniz.

## Farklılıklar {#differences}

Bu `TinyLog` motor, ailenin en basitidir ve en fakir işlevselliği ve en düşük verimliliği sağlar. Bu `TinyLog` motor, birkaç iş parçacığı tarafından paralel veri okumayı desteklemez. Paralel okumayı destekleyen ailedeki diğer motorlardan daha yavaş veri okur ve neredeyse birçok tanımlayıcı kullanır `Log` motor, her sütunu ayrı bir dosyada sakladığı için. Basit düşük yük senaryolarında kullanın.

Bu `Log` ve `StripeLog` motorlar paralel veri okumayı destekler. Veri okurken, ClickHouse birden çok iş parçacığı kullanır. Her iş parçacığı ayrı bir veri bloğu işler. Bu `Log` engine, tablonun her sütunu için ayrı bir dosya kullanır. `StripeLog` tüm verileri tek bir dosyada saklar. Sonuç olarak, `StripeLog` motor işletim sisteminde daha az tanımlayıcı kullanır, ancak `Log` motor veri okurken daha yüksek verimlilik sağlar.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/log_family/) <!--hide-->
