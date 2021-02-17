---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 6
toc_title: Performans
---

# Performans {#performance}

Yandex'deki dahili test sonuçlarına göre, ClickHouse, test için mevcut olan sınıfının sistemleri arasında karşılaştırılabilir işletim senaryoları için en iyi performansı (hem uzun sorgular için en yüksek verim hem de kısa sorgularda en düşük gecikme süresi) gösterir. Test sonuçlarını bir [ayrı sayfa](https://clickhouse.tech/benchmark/dbms/).

Çok sayıda bağımsız kriterler benzer sonuçlara geldi. Bir internet araması kullanarak bulmak zor değildir veya görebilirsiniz [ilgili bağlantı ourlardan oluşan küçük koleksiyon collectionumuz](https://clickhouse.tech/#independent-benchmarks).

## Tek bir büyük sorgu için çıktı {#throughput-for-a-single-large-query}

Verim, saniyede satır veya saniyede megabayt olarak ölçülebilir. Veriler sayfa önbelleğine yerleştirilirse, çok karmaşık olmayan bir sorgu, modern donanım üzerinde tek bir sunucuda yaklaşık 2-10 GB/s sıkıştırılmamış veri hızında işlenir (en basit durumlar için, hız 30 GB/s'ye ulaşabilir). Veri sayfa önbelleğine yerleştirilmezse, hız disk alt sistemine ve veri sıkıştırma hızına bağlıdır. Örneğin, disk alt sistemi 400 MB/s veri okuma izin verir ve veri sıkıştırma hızı 3 ise, hız 1.2 GB / s civarında olması bekleniyor. saniyede satır hızı elde etmek için hızı saniyede bayt cinsinden sorguda kullanılan sütunların toplam boyutuna bölün. Örneğin, 10 bayt sütun ayıklanırsa, hızın saniyede yaklaşık 100-200 milyon satır olması beklenir.

İşlem hızı, dağıtılmış işlem için neredeyse doğrusal olarak artar, ancak yalnızca toplama veya sıralamadan kaynaklanan satır sayısı çok büyük değilse.

## Kısa Sorguları İşlerken Gecikme Süresi {#latency-when-processing-short-queries}

Bir sorgu birincil anahtar kullanır ve çok fazla sütun ve satır (yüzbinlerce) işlemek için seçmez, veri sayfa önbelleğine yerleştirilirse, 50 milisaniyeden daha az gecikme süresi (en iyi durumda milisaniye tek basamak) bekleyebilirsiniz. Aksi takdirde, gecikme çoğunlukla arama sayısı tarafından hakimdir. Aşırı yüklenmemiş bir sistem için dönen disk sürücüleri kullanırsanız, gecikme bu formülle tahmin edilebilir: `seek time (10 ms) * count of columns queried * count of data parts`.

## Büyük miktarda kısa sorgu işlerken verim {#throughput-when-processing-a-large-quantity-of-short-queries}

Aynı koşullar altında, ClickHouse tek bir sunucuda saniyede birkaç yüz sorgu işleyebilir (en iyi durumda birkaç bine kadar). Bu senaryo analitik DBMSs için tipik olmadığından, saniyede en fazla 100 sorgu beklemenizi öneririz.

## Veri Eklerken Performans {#performance-when-inserting-data}

Verileri en az 1000 satırlık paketlere veya saniyede tek bir istekten daha fazla olmayan paketlere eklemenizi öneririz. Sekmeyle ayrılmış bir dökümden MergeTree tablosuna eklerken, ekleme hızı 50 ila 200 MB/s arasında olabilir. eklenen satırlar yaklaşık 1 Kb boyutundaysa, hız saniyede 50.000 ila 200.000 satır olacaktır. Satırlar küçükse, performans saniyede satırlarda daha yüksek olabilir (Banner sistem verileri üzerinde -`>` Saniyede 500 rows.000 satır; Graf ;it ver ;ilerinde -`>` Saniyede 1.000.000 satır). Performansı artırmak için, paralel olarak doğrusal olarak ölçeklenen birden çok ekleme sorgusu yapabilirsiniz.

[Orijinal makale](https://clickhouse.tech/docs/en/introduction/performance/) <!--hide-->
