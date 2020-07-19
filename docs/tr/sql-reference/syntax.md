---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 31
toc_title: "S\xF6zdizimi"
---

# Sözdizimi {#syntax}

Sistemde iki tür ayrıştırıcı vardır: tam SQL ayrıştırıcısı (özyinelemeli bir iniş ayrıştırıcısı) ve veri biçimi ayrıştırıcısı (hızlı akış ayrıştırıcısı).
Dışında her durumda `INSERT` sorgu, sadece tam SQL ayrıştırıcı kullanılır.
Bu `INSERT` sorgu her iki ayrıştırıcıyı da kullanır:

``` sql
INSERT INTO t VALUES (1, 'Hello, world'), (2, 'abc'), (3, 'def')
```

Bu `INSERT INTO t VALUES` parça tam ayrıştırıcı tarafından ayrıştırılır ve veriler `(1, 'Hello, world'), (2, 'abc'), (3, 'def')` hızlı akış ayrıştırıcısı tarafından ayrıştırılır. Ayrıca kullanarak veriler için tam ayrıştırıcı açabilirsiniz [ınput\_format\_values\_interpret\_expressions](../operations/settings/settings.md#settings-input_format_values_interpret_expressions) ayar. Ne zaman `input_format_values_interpret_expressions = 1`, ClickHouse önce hızlı akış ayrıştırıcısı ile değerleri ayrıştırmaya çalışır. Başarısız olursa, ClickHouse veriler için tam ayrıştırıcıyı kullanmaya çalışır ve bir SQL gibi davranır [ifade](#syntax-expressions).

Veri herhangi bir biçime sahip olabilir. Bir sorgu alındığında, sunucu daha fazla hesaplar [max\_query\_size](../operations/settings/settings.md#settings-max_query_size) istek bayt RAM (varsayılan olarak, 1 MB) ve geri kalanı akış ayrıştırılır.
Bu büyük sorunları önlemek için izin verir `INSERT` sorgular.

Kullanırken `Values` biçim içinde bir `INSERT` sorgu, verilerin bir ifadedeki ifadelerle aynı şekilde ayrıştırıldığı görünebilir `SELECT` sorgu, ancak bu doğru değil. Bu `Values` biçim çok daha sınırlıdır.

Bu makalenin geri kalanı tam çözümleyici kapsar. Biçim ayrıştırıcıları hakkında daha fazla bilgi için bkz: [Biçimliler](../interfaces/formats.md) bölme.

## Alanlar {#spaces}

Sözdizimsel yapılar arasında (bir sorgunun başlangıcı ve sonu dahil) herhangi bir sayıda boşluk simgesi olabilir. Boşluk sembolleri boşluk, sekme, satır beslemesi, CR ve form beslemesini içerir.

## Yorumlar {#comments}

ClickHouse, SQL stili ve C stili yorumlarını destekler.
SQL tarzı yorumlar ile başlar `--` ve hattın sonuna kadar devam, bir boşluk sonra `--` atlanmış olabilir.
C-style dan `/*` -e doğru `*/`ve çok satırlı olabilir, boşluklar da gerekli değildir.

## Kelimeler {#syntax-keywords}

Anahtar kelimeler karşılık geldiğinde büyük / küçük harf duyarsızdır:

-   SQL standardı. Mesela, `SELECT`, `select` ve `SeLeCt` hepsi geçerlidir.
-   Bazı popüler DBMS'DE (MySQL veya Postgres) uygulama. Mesela, `DateTime` ile aynıdır `datetime`.

Veri türü adı büyük / küçük harf duyarlı olup olmadığını denetlenebilir `system.data_type_families` Tablo.

Standart SQL'İN aksine, diğer tüm anahtar kelimeler (işlev adları dahil) şunlardır **büyük küçük harf duyarlı**.

Anahtar kelimeler ayrılmış değildir; sadece karşılık gelen bağlamda bu şekilde ele alınır. Kullanıyorsanız [tanıtıcılar](#syntax-identifiers) anahtar kelimelerle aynı ada sahip olarak, bunları çift tırnak veya backticks içine alın. Örneğin, sorgu `SELECT "FROM" FROM table_name` tablo geçerli ise `table_name` adı ile sütun vardır `"FROM"`.

## Tanıtıcılar {#syntax-identifiers}

Tanımlay areıcılar:

-   Küme, veritabanı, tablo, bölüm ve sütun adları.
-   İşlevler.
-   Veri türleri.
-   [İfade takma adları](#syntax-expression_aliases).

Tanımlayıcılar alıntılanabilir veya alıntılanamaz. İkincisi tercih edilir.

Alıntılanmamış tanımlayıcılar regex ile eşleşmelidir `^[a-zA-Z_][0-9a-zA-Z_]*$` ve eşit olamaz [kelimeler](#syntax-keywords). Örnekler: `x, _1, X_y__Z123_.`

Tanımlayıcıları anahtar kelimelerle aynı şekilde kullanmak istiyorsanız veya tanımlayıcılarda başka semboller kullanmak istiyorsanız, örneğin çift tırnak işaretleri veya backticks kullanarak alıntı yapın, `"id"`, `` `id` ``.

## Harfler {#literals}

Sayısal, dize, bileşik ve `NULL` harfler.

### Sayısal {#numeric}

Sayısal literal ayrıştırılmaya çalışılıyor:

-   İlk olarak, 64-bit imzalı bir sayı olarak, [strtoull](https://en.cppreference.com/w/cpp/string/byte/strtoul) İşlev.
-   Başarısız olursa, 64-bit imzasız bir sayı olarak, [strtoll](https://en.cppreference.com/w/cpp/string/byte/strtol) İşlev.
-   Başarısız olursa, kayan noktalı sayı olarak [strtod](https://en.cppreference.com/w/cpp/string/byte/strtof) İşlev.
-   Aksi takdirde, bir hata döndürür.

Hazır bilgi değeri, değerin sığdığı en küçük türe sahiptir.
Örneğin, 1 olarak ayrıştırılır `UInt8`, ancak 256 olarak ayrıştırılır `UInt16`. Daha fazla bilgi için, bkz. [Veri türleri](../sql-reference/data-types/index.md).

Örnekler: `1`, `18446744073709551615`, `0xDEADBEEF`, `01`, `0.1`, `1e100`, `-1e-100`, `inf`, `nan`.

### Dize {#syntax-string-literal}

Tek tırnak yalnızca dize değişmezleri desteklenir. Kapalı karakterler ters eğik çizgi kaçabilir. Aşağıdaki kaçış dizileri karşılık gelen özel bir değere sahiptir: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\a`, `\v`, `\xHH`. Diğer tüm durumlarda, çıkış dizileri biçiminde `\c`, nere `c` herhangi bir karakter, dönüştürülür `c`. Bu dizileri kullanabileceğiniz anlamına gelir `\'`ve`\\`. Değeri olacak [Dize](../sql-reference/data-types/string.md) tür.

Dize değişmezlerinde, en azından kaçmanız gerekir `'` ve `\`. Tek tırnak tek Alıntı ile kaçabilir, değişmez `'It\'s'` ve `'It''s'` eşittir.

### Bileşik {#compound}

Diziler köşeli parantez ile inşa edilmiştir `[1, 2, 3]`. Nuples yuvarlak parantez ile inşa edilmiştir `(1, 'Hello, world!', 2)`.
Teknik olarak bunlar değişmezler değil, sırasıyla dizi oluşturma işleci ve tuple oluşturma işleci ile ifadeler.
Bir dizi en az bir öğeden oluşmalı ve bir tuple en az iki öğeye sahip olmalıdır.
İçinde tuples göründüğünde ayrı bir durum var `IN` CLA ause of a `SELECT` sorgu. Sorgu sonuçları tuples içerebilir, ancak tuples bir veritabanına kaydedilemez (tablolar hariç [Hafıza](../engines/table-engines/special/memory.md) motor).

### NULL {#null-literal}

Değerin eksik olduğunu gösterir.

Saklamak için `NULL` bir tablo alanında, bu olmalıdır [Nullable](../sql-reference/data-types/nullable.md) tür.

Veri formatına bağlı olarak (giriş veya çıkış), `NULL` farklı bir temsili olabilir. Daha fazla bilgi için belgelere bakın [veri formatları](../interfaces/formats.md#formats).

İşleme için birçok nüans var `NULL`. Örneğin, bir karşılaştırma işleminin argümanlarından en az biri ise `NULL`, bu işlemin sonucu da `NULL`. Aynı şey çarpma, toplama ve diğer işlemler için de geçerlidir. Daha fazla bilgi için her işlem için belgeleri okuyun.

Sorgularda, kontrol edebilirsiniz `NULL` kullanarak [IS NULL](operators/index.md#operator-is-null) ve [IS NOT NULL](operators/index.md) operatörler ve ilgili fonksiyonlar `isNull` ve `isNotNull`.

## İşlevler {#functions}

İşlev çağrıları, yuvarlak parantez içinde bir argüman listesi (muhtemelen boş) olan bir tanımlayıcı gibi yazılır. Standart SQL'İN aksine, boş bir argüman listesi için bile parantezler gereklidir. Örnek: `now()`.
Düzenli ve agrega işlevleri vardır (bkz. “Aggregate functions”). Bazı toplama işlevleri parantez içinde iki bağımsız değişken listesi içerebilir. Örnek: `quantile (0.9) (x)`. Bu toplama fonksiyonları denir “parametric” fonksiyonlar ve ilk listedeki argümanlar çağrılır “parameters”. Parametresiz toplama işlevlerinin sözdizimi, normal işlevlerle aynıdır.

## Operatörler {#operators}

Operatörler, sorgu ayrıştırma sırasında önceliklerini ve ilişkilendirmelerini dikkate alarak karşılık gelen işlevlerine dönüştürülür.
Örneğin, ifade `1 + 2 * 3 + 4` dönüştür toülür `plus(plus(1, multiply(2, 3)), 4)`.

## Veri türleri ve veritabanı tablosu motorları {#data_types-and-database-table-engines}

Veri türleri ve tablo motorları `CREATE` sorgu tanımlayıcıları veya işlevleri aynı şekilde yazılır. Başka bir deyişle, parantez içinde bir argüman listesi içerebilir veya içermeyebilir. Daha fazla bilgi için bölümlere bakın “Data types,” “Table engines,” ve “CREATE”.

## İfade Takma Adları {#syntax-expression_aliases}

Diğer ad, sorgudaki ifade için kullanıcı tanımlı bir addır.

``` sql
expr AS alias
```

-   `AS` — The keyword for defining aliases. You can define the alias for a table name or a column name in a `SELECT` kullanmadan fık thera `AS` kelime.

        For example, `SELECT table_name_alias.column_name FROM table_name table_name_alias`.

        In the [CAST](sql_reference/functions/type_conversion_functions.md#type_conversion_function-cast) function, the `AS` keyword has another meaning. See the description of the function.

-   `expr` — Any expression supported by ClickHouse.

        For example, `SELECT column_name * 2 AS double FROM some_table`.

-   `alias` — Name for `expr`. Takma adlar ile uyumlu olmalıdır [tanıtıcılar](#syntax-identifiers) sözdizimi.

        For example, `SELECT "table t".column_name FROM table_name AS "table t"`.

### Kullanımı ile ilgili notlar {#notes-on-usage}

Diğer adlar bir sorgu veya alt sorgu için geneldir ve herhangi bir ifade için sorgunun herhangi bir bölümünde bir diğer ad tanımlayabilirsiniz. Mesela, `SELECT (1 AS n) + 2, n`.

Diğer adlar alt sorgularda ve alt sorgular arasında görünmez. Örneğin, sorgu yürütülürken `SELECT (SELECT sum(b.a) + num FROM b) - a.a AS num FROM a` ClickHouse istisna oluşturur `Unknown identifier: num`.

Sonuç sütunları için bir diğer ad tanımlanmışsa `SELECT` bir alt sorgunun yan tümcesi, bu sütunlar dış sorguda görülebilir. Mesela, `SELECT n + m FROM (SELECT 1 AS n, 2 AS m)`.

Sütun veya tablo adlarıyla aynı olan diğer adlara dikkat edin. Aşağıdaki örneği ele alalım:

``` sql
CREATE TABLE t
(
    a Int,
    b Int
)
ENGINE = TinyLog()
```

``` sql
SELECT
    argMax(a, b),
    sum(b) AS b
FROM t
```

``` text
Received exception from server (version 18.14.17):
Code: 184. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: Aggregate function sum(b) is found inside another aggregate function in query.
```

Bu örnekte, tablo ilan ettik `t` sütun ile `b`. Ardından, veri seçerken, `sum(b) AS b` takma ad. Takma adlar küresel olduğundan, ClickHouse literal yerine `b` ifad theesinde `argMax(a, b)` ifad theesiyle `sum(b)`. Bu ikame istisnaya neden oldu.

## Yıldız işareti {#asterisk}

İn a `SELECT` sorgu, bir yıldız ifadesinin yerini alabilir. Daha fazla bilgi için bölüme bakın “SELECT”.

## İfadeler {#syntax-expressions}

Bir ifade, bir işlev, tanımlayıcı, değişmez, bir operatörün uygulaması, parantez içindeki ifade, alt sorgu veya yıldız işaretidir. Ayrıca bir takma ad içerebilir.
İfadelerin listesi, virgülle ayrılmış bir veya daha fazla ifadedir.
Fonksiyonlar ve operatörler, sırayla, argüman olarak ifadelere sahip olabilirler.

[Orijinal makale](https://clickhouse.tech/docs/en/sql_reference/syntax/) <!--hide-->
