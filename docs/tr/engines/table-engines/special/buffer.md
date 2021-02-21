---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: Arabellek
---

# Arabellek {#buffer}

RAM'de yazmak için verileri tamponlar, periyodik olarak başka bir tabloya temizler. Okuma işlemi sırasında veri arabellekten ve diğer tablodan aynı anda okunur.

``` sql
Buffer(database, table, num_layers, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes)
```

Motor parametreleri:

-   `database` – Database name. Instead of the database name, you can use a constant expression that returns a string.
-   `table` – Table to flush data to.
-   `num_layers` – Parallelism layer. Physically, the table will be represented as `num_layers` bağımsız tamponların. Önerilen değer: 16.
-   `min_time`, `max_time`, `min_rows`, `max_rows`, `min_bytes`, ve `max_bytes` – Conditions for flushing data from the buffer.

Veri arabellekten temizlendi ve hedef tabloya yazılır eğer tüm `min*` koşulları veya en az bir `max*` koşul karşı arelanır.

-   `min_time`, `max_time` – Condition for the time in seconds from the moment of the first write to the buffer.
-   `min_rows`, `max_rows` – Condition for the number of rows in the buffer.
-   `min_bytes`, `max_bytes` – Condition for the number of bytes in the buffer.

Yazma işlemi sırasında veri bir `num_layers` rastgele tampon sayısı. Veya, eklenecek veri kısmı yeterince büyükse (daha büyük `max_rows` veya `max_bytes`), arabelleği atlayarak doğrudan hedef tabloya yazılır.

Verilerin yıkanması için koşullar, her biri için ayrı ayrı hesaplanır. `num_layers` arabellekler. Örneğin, `num_layers = 16` ve `max_bytes = 100000000`, maksimum RAM tüketimi 1,6 GB'DİR.

Örnek:

``` sql
CREATE TABLE merge.hits_buffer AS merge.hits ENGINE = Buffer(merge, hits, 16, 10, 100, 10000, 1000000, 10000000, 100000000)
```

Oluşturma Bir ‘merge.hits_buffer’ ile aynı yapıya sahip tablo ‘merge.hits’ ve Tampon motorunu kullanarak. Bu tabloya yazarken, veriler RAM'de arabelleğe alınır ve daha sonra ‘merge.hits’ Tablo. 16 tamponlar oluşturulur. 100 saniye geçti veya bir milyon satır yazılmış veya 100 MB veri yazılmıştır; ya da aynı anda 10 saniye geçti ve 10.000 satır ve 10 MB veri yazılmıştır, bunların her veri temizlendi. Örneğin, sadece bir satır yazılmışsa, 100 saniye sonra ne olursa olsun, yıkanacaktır. Ancak, birçok satır yazılmışsa, veriler daha erken temizlenecektir.

Sunucu DROP TABLE veya DETACH TABLE ile durdurulduğunda, arabellek verileri de hedef tabloya temizlendi.

Veritabanı ve tablo adı için tek tırnak içinde boş dizeleri ayarlayabilirsiniz. Bu, bir hedef tablonun yokluğunu gösterir. Bu durumda, Veri Temizleme koşullarına ulaşıldığında, arabellek basitçe temizlenir. Bu, bir veri penceresini bellekte tutmak için yararlı olabilir.

Bir arabellek tablosundan okurken, veriler hem arabellekten hem de hedef tablodan (varsa) işlenir.
Arabellek tabloları bir dizin desteklemediğini unutmayın. Başka bir deyişle, arabellekteki veriler tamamen taranır, bu da büyük arabellekler için yavaş olabilir. (Alt tablodaki veriler için, desteklediği dizin kullanılacaktır.)

Arabellek tablosundaki sütun kümesi, alt tablodaki sütun kümesiyle eşleşmiyorsa, her iki tabloda da bulunan sütunların bir alt kümesi eklenir.

Türleri arabellek tablo ve alt tablo sütunlarından biri için eşleşmiyorsa, sunucu günlüğüne bir hata iletisi girilir ve arabellek temizlenir.
Arabellek temizlendiğinde alt tablo yoksa aynı şey olur.

Eğer bağımlı bir tablo ve Tampon tablo için ALTER çalıştırmak gerekiyorsa, ilk Tampon tablo silme, alt tablo için ALTER çalışan, sonra tekrar Tampon tablo oluşturma öneririz.

Sunucu anormal şekilde yeniden başlatılırsa, arabellekteki veriler kaybolur.

Son ve örnek arabellek tabloları için düzgün çalışmıyor. Bu koşullar hedef tabloya geçirilir, ancak arabellekte veri işlemek için kullanılmaz. Bu özellikler gerekiyorsa, hedef tablodan okurken yalnızca yazma için arabellek tablosunu kullanmanızı öneririz.

Bir arabelleğe veri eklerken, arabelleklerden biri kilitlenir. Bir okuma işlemi aynı anda tablodan gerçekleştiriliyor, bu gecikmelere neden olur.

Bir arabellek tablosuna eklenen veriler, alt tabloda farklı bir sırada ve farklı bloklarda sonuçlanabilir. Bu nedenle, bir arabellek tablo CollapsingMergeTree doğru yazmak için kullanmak zordur. Sorunları önlemek için şunları ayarlayabilirsiniz ‘num_layers’ 1'e.

Hedef tablo yinelenirse, bir arabellek tablosuna yazarken yinelenmiş tabloların bazı beklenen özellikleri kaybolur. Satır ve veri parçaları boyutlarda sipariş için rasgele değişiklikler veri çoğaltma güvenilir olması mümkün olmadığını ifade eden çalışma, kapanmasına neden ‘exactly once’ çoğaltılan tablolara yazın.

Bu dezavantajlardan dolayı, nadir durumlarda yalnızca bir arabellek tablosu kullanmanızı önerebiliriz.

Bir arabellek tablosu, bir zaman birimi üzerinden çok sayıda sunucudan çok fazla ekleme alındığında kullanılır ve ekleme işleminden önce veri arabelleğe alınamaz, bu da eklerin yeterince hızlı çalışamayacağı anlamına gelir.

Arabellek tabloları için bile, her seferinde bir satır veri eklemek mantıklı olmadığını unutmayın. Bu, yalnızca saniyede birkaç bin satırlık bir hız üretirken, daha büyük veri blokları eklemek saniyede bir milyondan fazla satır üretebilir (bölüme bakın “Performance”).

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/buffer/) <!--hide-->
