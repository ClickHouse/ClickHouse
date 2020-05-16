---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "\u0130\u015Flevler"
toc_priority: 32
toc_title: "Giri\u015F"
---

# İşlevler {#functions}

En az\* iki tür fonksiyon vardır-düzenli Fonksiyonlar (sadece denir “functions”) and aggregate functions. These are completely different concepts. Regular functions work as if they are applied to each row separately (for each row, the result of the function doesn't depend on the other rows). Aggregate functions accumulate a set of values from various rows (i.e. they depend on the entire set of rows).

Bu bölümde düzenli işlevleri tartışıyoruz. Toplama işlevleri için bölüme bakın “Aggregate functions”.

\* - Üçüncü bir işlev türü vardır ‘arrayJoin’ fonksiyon aittir; tablo fonksiyonları da ayrı ayrı belirtilebilir.\*

## Güçlü Yazarak {#strong-typing}

Standart SQL aksine, ClickHouse güçlü yazarak vardır. Başka bir deyişle, türler arasında örtük dönüşümler yapmaz. Her işlev belirli bir tür kümesi için çalışır. Bu, bazen tür dönüştürme işlevlerini kullanmanız gerektiği anlamına gelir.

## Ortak Subexpression Eliminasyonu {#common-subexpression-elimination}

Aynı AST (aynı kayıt veya sözdizimsel ayrıştırma aynı sonucu) olan bir sorgudaki tüm ifadeler aynı değerlere sahip olarak kabul edilir. Bu tür ifadeler bir kez birleştirilir ve yürütülür. Aynı alt sorgular da bu şekilde elimine edilir.

## Sonuç türleri {#types-of-results}

Tüm işlevler sonuç olarak tek bir dönüş döndürür (birkaç değer değil, sıfır değer değil). Sonuç türü genellikle değerlerle değil, yalnızca bağımsız değişken türleriyle tanımlanır. Özel durumlar tupleElement işlevi (a.n işleci) ve tofixedstring işlevidir.

## Devamlılar {#constants}

Basitlik için, bazı işlevler yalnızca bazı argümanlar için sabitlerle çalışabilir. Örneğin, LİKE operatörünün doğru argümanı sabit olmalıdır.
Hemen hemen tüm işlevler sabit argümanlar için bir sabit döndürür. İstisna, rasgele sayılar üreten işlevlerdir.
Bu ‘now’ işlev, farklı zamanlarda çalıştırılan sorgular için farklı değerler döndürür, ancak sonuç sabit olarak kabul edilir, çünkü sabitlik yalnızca tek bir sorguda önemlidir.
Sabit bir ifade de sabit olarak kabul edilir (örneğin, LİKE operatörünün sağ yarısı birden fazla sabitten oluşturulabilir).

Fonksiyonlar sabit ve sabit olmayan argümanlar için farklı şekillerde uygulanabilir (farklı kod yürütülür). Ancak, bir sabit ve yalnızca aynı değeri içeren gerçek bir sütun için sonuçlar birbiriyle eşleşmelidir.

## NULL işleme {#null-processing}

Fonksiyonlar aşağıdaki davranışlara sahiptir:

-   İşlevin argümanlarından en az biri ise `NULL`, fonksiyon sonucu da `NULL`.
-   Her işlevin açıklamasında ayrı ayrı belirtilen özel davranış. ClickHouse kaynak kodunda, bu işlevler `UseDefaultImplementationForNulls=false`.

## Süreklilik {#constancy}

Functions can't change the values of their arguments – any changes are returned as the result. Thus, the result of calculating separate functions does not depend on the order in which the functions are written in the query.

## Hata İşleme {#error-handling}

Veriler geçersizse bazı işlevler bir istisna oluşturabilir. Bu durumda, sorgu iptal edilir ve bir hata metni istemciye döndürülür. Dağıtılmış işlem için sunuculardan birinde bir özel durum oluştuğunda, diğer sunucular da sorguyu iptal etmeye çalışır.

## Argüman ifadelerinin değerlendirilmesi {#evaluation-of-argument-expressions}

Hemen hemen tüm programlama dillerinde, argümanlardan biri belirli operatörler için değerlendirilmeyebilir. Bu genellikle operatörler `&&`, `||`, ve `?:`.
Ancak Clickhouse'da, fonksiyonların (operatörler) argümanları her zaman değerlendirilir. Bunun nedeni, sütunların tüm bölümlerinin her satırı ayrı ayrı hesaplamak yerine bir kerede değerlendirilmesidir.

## Dağıtılmış sorgu işleme işlevleri gerçekleştirme {#performing-functions-for-distributed-query-processing}

Dağıtılmış sorgu işleme için, sorgu işlemenin mümkün olduğu kadar çok aşaması uzak sunucularda gerçekleştirilir ve aşamaların geri kalanı (Ara sonuçları ve bundan sonra her şeyi birleştirme) istek sahibi sunucuda gerçekleştirilir.

Bu, işlevlerin farklı sunucularda gerçekleştirilebileceği anlamına gelir.
Örneğin, sorguda `SELECT f(sum(g(x))) FROM distributed_table GROUP BY h(y),`

-   eğer bir `distributed_table` en az iki parçaya sahiptir, fonksiyonlar ‘g’ ve ‘h’ uzak sunucularda gerçekleştirilir ve işlev ‘f’ ıstekçi sunucuda gerçekleştirilir.
-   eğer bir `distributed_table` sadece bir parça var, tüm ‘f’, ‘g’, ve ‘h’ fonksiyonlar bu shard'ın sunucusunda gerçekleştirilir.

Bir işlevin sonucu genellikle hangi sunucuda gerçekleştirildiğine bağlı değildir. Ancak, bazen bu önemlidir.
Örneğin, sözlüklerle çalışan işlevler, üzerinde çalışmakta oldukları sunucuda bulunan sözlüğü kullanır.
Başka bir örnek ise `hostName` yapmak için üzerinde çalıştığı sunucunun adını döndüren işlev `GROUP BY` sunucular tarafından bir `SELECT` sorgu.

Eğer sorguda bir işlevi istemcisi sunucu üzerinde yapılır, ama uzak sunucularda bunu gerçekleştirmek için ihtiyacınız varsa, bir saramaz mısın ‘any’ toplama işlevi veya bir anahtara ekleyin `GROUP BY`.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/) <!--hide-->
