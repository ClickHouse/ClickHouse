---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: Katmak
---

# Katmak {#join}

Kullanılmak üzere hazırlanmış veri yapısı [JOIN](../../../sql-reference/statements/select/join.md#select-join) harekat.

## Tablo oluşturma {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
) ENGINE = Join(join_strictness, join_type, k1[, k2, ...])
```

Ayrıntılı açıklamasına bakın [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) sorgu.

**Motor Parametreleri**

-   `join_strictness` – [Katılık katılın](../../../sql-reference/statements/select/join.md#select-join-types).
-   `join_type` – [Birleştirme türü](../../../sql-reference/statements/select/join.md#select-join-types).
-   `k1[, k2, ...]` – Key columns from the `USING` fık thera: `JOIN` işlemi yapılmamaktadır.

Girmek `join_strictness` ve `join_type` tırnak işaretleri olmadan parametreler, örneğin, `Join(ANY, LEFT, col1)`. Onlar eşleşmelidir `JOIN` tablo için kullanılacak işlem. Parametreler eşleşmezse, ClickHouse bir istisna atmaz ve yanlış veri döndürebilir.

## Tablo Kullanımı {#table-usage}

### Örnek {#example}

Sol taraftaki tablo oluşturma:

``` sql
CREATE TABLE id_val(`id` UInt32, `val` UInt32) ENGINE = TinyLog
```

``` sql
INSERT INTO id_val VALUES (1,11)(2,12)(3,13)
```

Sağ tarafı oluşturma `Join` Tablo:

``` sql
CREATE TABLE id_val_join(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id)
```

``` sql
INSERT INTO id_val_join VALUES (1,21)(1,22)(3,23)
```

Tabloları birleştirme:

``` sql
SELECT * FROM id_val ANY LEFT JOIN id_val_join USING (id) SETTINGS join_use_nulls = 1
```

``` text
┌─id─┬─val─┬─id_val_join.val─┐
│  1 │  11 │              21 │
│  2 │  12 │            ᴺᵁᴸᴸ │
│  3 │  13 │              23 │
└────┴─────┴─────────────────┘
```

Alternatif olarak, veri alabilirsiniz `Join` tablo, birleştirme anahtarı değerini belirterek:

``` sql
SELECT joinGet('id_val_join', 'val', toUInt32(1))
```

``` text
┌─joinGet('id_val_join', 'val', toUInt32(1))─┐
│                                         21 │
└────────────────────────────────────────────┘
```

### Veri seçme ve ekleme {#selecting-and-inserting-data}

Kullanabilirsiniz `INSERT` veri eklemek için sorgular `Join`- motor masaları. Tablo ile oluşturulmuş ise `ANY` katılık, yinelenen anahtarlar için veriler göz ardı edilir. İle... `ALL` katılık, tüm satırlar eklenir.

Gerçekleştir aemezsiniz `SELECT` doğrudan tablodan sorgulayın. Bunun yerine, aşağıdaki yöntemlerden birini kullanın:

-   Tabloyu sağ tarafa yerleştirin. `JOIN` yan.
-   Call the [joinGet](../../../sql-reference/functions/other-functions.md#joinget) tablodan bir sözlükten aynı şekilde veri ayıklamanızı sağlayan işlev.

### Sınırlamalar ve Ayarlar {#join-limitations-and-settings}

Bir tablo oluştururken aşağıdaki ayarlar uygulanır:

-   [join\_use\_nulls](../../../operations/settings/settings.md#join_use_nulls)
-   [max\_rows\_in\_join](../../../operations/settings/query-complexity.md#settings-max_rows_in_join)
-   [max\_bytes\_in\_join](../../../operations/settings/query-complexity.md#settings-max_bytes_in_join)
-   [join\_overflow\_mode](../../../operations/settings/query-complexity.md#settings-join_overflow_mode)
-   [join\_any\_take\_last\_row](../../../operations/settings/settings.md#settings-join_any_take_last_row)

Bu `Join`- motor tabloları kullanılamaz `GLOBAL JOIN` harekat.

Bu `Join`- motor kullanımına izin verir [join\_use\_nulls](../../../operations/settings/settings.md#join_use_nulls) ayarı `CREATE TABLE` deyim. Ve [SELECT](../../../sql-reference/statements/select/index.md) sorgu kullanımına izin verir `join_use_nulls` çok. Eğer farklı varsa `join_use_nulls` ayarlar, tablo birleştirme bir hata alabilirsiniz. Bu katılmak türüne bağlıdır. Kullandığınızda [joinGet](../../../sql-reference/functions/other-functions.md#joinget) fonksiyonu, aynı kullanmak zorunda `join_use_nulls` ayarı `CRATE TABLE` ve `SELECT` deyimler.

## Veri Depolama {#data-storage}

`Join` tablo verileri her zaman RAM'de bulunur. Bir tabloya satır eklerken, sunucu yeniden başlatıldığında geri yüklenebilir, böylece ClickHouse disk üzerindeki dizine veri bloklarını yazar.

Sunucu yanlış yeniden başlatılırsa, diskteki veri bloğu kaybolabilir veya zarar görebilir. Bu durumda, dosyayı hasarlı verilerle el ile silmeniz gerekebilir.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/join/) <!--hide-->
