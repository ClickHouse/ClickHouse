---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: "Haf\u0131za"
---

# Hafıza {#memory}

Bellek altyapısı verileri RAM, sıkıştırılmamış biçimde depolar. Veri okunduğunda alınan tam olarak aynı biçimde saklanır. Başka bir deyişle, bu tablodan okuma tamamen ücretsizdir.
Eşzamanlı veri erişimi senkronize edilir. Kilitler kısa: okuma ve yazma işlemleri birbirini engellemez.
Dizinler desteklenmiyor. Okuma paralelleştirilmiştir.
Basit sorgularda maksimum üretkenliğe (10 GB/sn'den fazla) ulaşılır, çünkü diskten okuma, açma veya veri serisini kaldırma yoktur. (Birçok durumda MergeTree motorunun verimliliğinin neredeyse yüksek olduğunu unutmamalıyız.)
Bir sunucu yeniden başlatılırken, veri tablodan kaybolur ve tablo boş olur.
Normalde, bu tablo motorunu kullanmak haklı değildir. Bununla birlikte, testler ve nispeten az sayıda satırda (yaklaşık 100.000.000'a kadar) maksimum hızın gerekli olduğu görevler için kullanılabilir.

Bellek motoru, harici sorgu verilerine sahip geçici tablolar için sistem tarafından kullanılır (bkz. “External data for processing a query”) ve GLOBAL In uygulanması için (bkz. “IN operators”).

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/memory/) <!--hide-->
