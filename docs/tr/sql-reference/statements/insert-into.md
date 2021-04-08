---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: INSERT INTO
---

## INSERT {#insert}

Veri ekleme.

Temel sorgu biçimi:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

Sorgu eklemek için sütunların bir listesini belirtebilirsiniz `[(c1, c2, c3)]`. Bu durumda, sütunların geri kalanı ile doldurulur:

-   Hesaplanan değerler `DEFAULT` tablo tanımında belirtilen ifadeler.
-   Sıfırlar ve boş dizeler, eğer `DEFAULT` ifadeler tanımlanmamıştır.

Eğer [strict\_ınsert\_defaults = 1](../../operations/settings/settings.md), sahip olmayan sütunlar `DEFAULT` tanımlanan sorguda listelenmelidir.

Veri herhangi bir İNSERT geçirilebilir [biçimli](../../interfaces/formats.md#formats) ClickHouse tarafından desteklenmektedir. Biçim sorguda açıkça belirtilmelidir:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

For example, the following query format is identical to the basic version of INSERT … VALUES:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse, veriden önce tüm boşlukları ve bir satır beslemesini (varsa) kaldırır. Bir sorgu oluştururken, sorgu işleçlerinden sonra verileri yeni bir satıra koymanızı öneririz (veriler boşluklarla başlarsa bu önemlidir).

Örnek:

``` sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

Komut satırı istemcisini veya HTTP arabirimini kullanarak verileri sorgudan ayrı olarak ekleyebilirsiniz. Daha fazla bilgi için bölüme bakın “[Arabirimler](../../interfaces/index.md#interfaces)”.

### Kısıtlamalar {#constraints}

Tablo varsa [kısıtlamalar](create.md#constraints), their expressions will be checked for each row of inserted data. If any of those constraints is not satisfied — server will raise an exception containing constraint name and expression, the query will be stopped.

### Sonuçları ekleme `SELECT` {#insert_query_insert-select}

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
```

Sütunlar, SELECT yan tümcesindeki konumlarına göre eşleştirilir. Ancak, SELECT ifadesi ve INSERT için tablo adları farklı olabilir. Gerekirse, tip döküm yapılır.

Değerler dışındaki veri biçimlerinin hiçbiri, aşağıdaki gibi ifadelere değerler ayarlamasına izin vermez `now()`, `1 + 2` ve bu yüzden. Değerler biçimi, ifadelerin sınırlı kullanımına izin verir, ancak bu önerilmez, çünkü bu durumda verimsiz kod yürütme için kullanılır.

Veri bölümlerini değiştirmek için diğer sorgular desteklenmiyor: `UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`.
Ancak, eski verileri kullanarak silebilirsiniz `ALTER TABLE ... DROP PARTITION`.

`FORMAT` yan tümcesi sorgu sonunda belirtilmelidir eğer `SELECT` yan tümcesi tablo işlevi içerir [girdi()](../table-functions/input.md).

### Performans Konuları {#performance-considerations}

`INSERT` giriş verilerini birincil anahtarla sıralar ve bunları bir bölüm anahtarı ile bölümlere ayırır. Bir kerede birkaç bölüme veri eklerseniz, bu veri tabanının performansını önemli ölçüde azaltabilir. `INSERT` sorgu. Bunu önlemek için:

-   Bir seferde 100.000 satır gibi oldukça büyük gruplar halinde veri ekleyin.
-   Clickhouse'a yüklemeden önce verileri bir bölüm anahtarıyla gruplandırın.

Eğer performans azalmaz:

-   Veri gerçek zamanlı olarak eklenir.
-   Genellikle zamana göre sıralanır veri yükleyin.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/insert_into/) <!--hide-->
