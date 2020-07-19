---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 50
toc_title: Enum
---

# Enum {#enum}

Adlandırılmış değerlerden oluşan numaralandırılmış tür.

Adlandırılmış değerler olarak bildirilmelidir `'string' = integer` çiftliler. ClickHouse yalnızca sayıları saklar, ancak adları aracılığıyla değerlerle işlemleri destekler.

ClickHouse destekler:

-   8-bit `Enum`. En fazla 256 değerleri numaralandırılmış içerebilir `[-128, 127]` Aralık.
-   16-bit `Enum`. En fazla 65536 değerleri numaralandırılmış içerebilir `[-32768, 32767]` Aralık.

ClickHouse otomatik olarak türünü seçer `Enum` veri eklendiğinde. Ayrıca kullanabilirsiniz `Enum8` veya `Enum16` türleri depolama boyutunda emin olmak için.

## Kullanım Örnekleri {#usage-examples}

Burada bir tablo oluşturuyoruz `Enum8('hello' = 1, 'world' = 2)` type Col columnum columnn:

``` sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world' = 2)
)
ENGINE = TinyLog
```

Sütun `x` yalnızca tür tanımında listelenen değerleri depolayabilir: `'hello'` veya `'world'`. Başka bir değer kaydetmeye çalışırsanız, ClickHouse bir özel durum yükseltir. Bunun için 8-bit boyutu `Enum` otomatik olarak seçilir.

``` sql
INSERT INTO t_enum VALUES ('hello'), ('world'), ('hello')
```

``` text
Ok.
```

``` sql
INSERT INTO t_enum values('a')
```

``` text
Exception on client:
Code: 49. DB::Exception: Unknown element 'a' for type Enum('hello' = 1, 'world' = 2)
```

Tablodan veri sorguladığınızda, ClickHouse dize değerleri `Enum`.

``` sql
SELECT * FROM t_enum
```

``` text
┌─x─────┐
│ hello │
│ world │
│ hello │
└───────┘
```

Satırların sayısal eşdeğerlerini görmeniz gerekiyorsa, `Enum` tamsayı türüne değer.

``` sql
SELECT CAST(x, 'Int8') FROM t_enum
```

``` text
┌─CAST(x, 'Int8')─┐
│               1 │
│               2 │
│               1 │
└─────────────────┘
```

Bir sorguda bir Enum değeri oluşturmak için, ayrıca kullanmanız gerekir `CAST`.

``` sql
SELECT toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))
```

``` text
┌─toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))─┐
│ Enum8('a' = 1, 'b' = 2)                             │
└─────────────────────────────────────────────────────┘
```

## Genel Kurallar ve kullanım {#general-rules-and-usage}

Değerlerin her birine aralıkta bir sayı atanır `-128 ... 127` için `Enum8` veya aralık inta `-32768 ... 32767` için `Enum16`. Tüm dizeler ve sayılar farklı olmalıdır. Boş bir dize izin verilir. Bu tür belirtilmişse (bir tablo tanımında), sayılar rasgele bir sırada olabilir. Ancak, sipariş önemli değil.

Ne dize ne de sayısal değer bir `Enum` olabilir [NULL](../../sql-reference/syntax.md).

Bir `Enum` içerdiği olabilir [Nullable](nullable.md) tür. Yani sorguyu kullanarak bir tablo oluşturursanız

``` sql
CREATE TABLE t_enum_nullable
(
    x Nullable( Enum8('hello' = 1, 'world' = 2) )
)
ENGINE = TinyLog
```

bu mağaza değil sadece `'hello'` ve `'world'`, ama `NULL`, yanında.

``` sql
INSERT INTO t_enum_nullable Values('hello'),('world'),(NULL)
```

RAM, bir `Enum` sütun aynı şekilde saklanır `Int8` veya `Int16` karşılık gelen sayısal değerlerin.

Metin formunda okurken, ClickHouse değeri bir dize olarak ayrıştırır ve karşılık gelen dizeyi Enum değerleri kümesinden arar. Bulunmazsa, bir istisna atılır. Metin biçiminde okurken, dize okunur ve karşılık gelen sayısal değer aranır. Bulunmazsa bir istisna atılır.
Metin formunda yazarken, değeri karşılık gelen dize olarak yazar. Sütun verileri çöp içeriyorsa (geçerli kümeden olmayan sayılar), bir özel durum atılır. İkili formda okurken ve yazarken, Int8 ve Int16 veri türleri ile aynı şekilde çalışır.
Örtülü varsayılan değer, en düşük sayıya sahip değerdir.

Sırasında `ORDER BY`, `GROUP BY`, `IN`, `DISTINCT` ve böylece, Enumlar karşılık gelen sayılarla aynı şekilde davranır. Örneğin, sipariş onları sayısal olarak sıralar. Eşitlik ve karşılaştırma işleçleri, alttaki sayısal değerler üzerinde yaptıkları gibi Enumlarda aynı şekilde çalışır.

Enum değerleri sayılarla karşılaştırılamaz. Enums sabit bir dize ile karşılaştırılabilir. Karşılaştırılan dize Enum için geçerli bir değer değilse, bir özel durum atılır. IN operatörü, sol taraftaki Enum ve sağ taraftaki bir dizi dizeyle desteklenir. Dizeler, karşılık gelen Enumun değerleridir.

Most numeric and string operations are not defined for Enum values, e.g. adding a number to an Enum or concatenating a string to an Enum.
Ancak, Enum doğal bir `toString` dize değerini döndüren işlev.

Enum değerleri de kullanarak sayısal türlere dönüştürülebilir `toT` fonksiyon, burada t sayısal bir türdür. T enum'un temel sayısal türüne karşılık geldiğinde, bu dönüşüm sıfır maliyetlidir.
Enum türü, yalnızca değer kümesi değiştirilirse, alter kullanılarak maliyet olmadan değiştirilebilir. Her iki ekleme ve Alter kullanarak Enum üyeleri kaldırmak mümkündür (kaldırma yalnızca kaldırılan değer tabloda hiç kullanılmadıysa güvenlidir). Bir koruma olarak, önceden tanımlanmış bir Enum üyesinin sayısal değerini değiştirmek bir istisna atar.

ALTER kullanarak, bir Enum8 için bir Enum16 veya tam tersi, Int8 için Int16 değiştirme gibi değiştirmek mümkündür.

[Orijinal makale](https://clickhouse.tech/docs/en/data_types/enum/) <!--hide-->
