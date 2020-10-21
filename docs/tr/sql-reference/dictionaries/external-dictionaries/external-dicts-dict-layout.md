---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "S\xF6zl\xFCkleri bellekte saklama"
---

# Sözlükleri bellekte saklama {#dicts-external-dicts-dict-layout}

Sözlükleri bellekte saklamanın çeşitli yolları vardır.

Biz tavsiye [düzlük](#flat), [karıştırıyordu](#dicts-external_dicts_dict_layout-hashed) ve [complex_key_hashed](#complex-key-hashed). hangi optimum işleme hızı sağlamak.

Önbelleğe alma, potansiyel olarak düşük performans ve en uygun parametreleri seçmede zorluklar nedeniyle önerilmez. Bölümünde devamını oku “[önbellek](#cache)”.

Sözlük performansını artırmanın birkaç yolu vardır:

-   Sonra sözlük ile çalışmak için işlevi çağırın `GROUP BY`.
-   Mark enjektif olarak ayıklamak için nitelikler. Farklı öznitelik değerleri farklı anahtarlara karşılık geliyorsa, bir öznitelik ınjective olarak adlandırılır. Yani ne zaman `GROUP BY` anahtar tarafından bir öznitelik değeri getiren bir işlev kullanır, bu işlev otomatik olarak dışarı alınır `GROUP BY`.

ClickHouse, sözlüklerdeki hatalar için bir istisna oluşturur. Hata örnekleri:

-   Erişilen sözlük yüklenemedi.
-   Bir sorgulama hatası `cached` sözlük.

Sen dış sözlükler ve durumları listesini görüntüleyebilirsiniz `system.dictionaries` Tablo.

Yapılandırma şöyle görünüyor:

``` xml
<yandex>
    <dictionary>
        ...
        <layout>
            <layout_type>
                <!-- layout settings -->
            </layout_type>
        </layout>
        ...
    </dictionary>
</yandex>
```

İlgili [DDL-sorgu](../../statements/create.md#create-dictionary-query):

``` sql
CREATE DICTIONARY (...)
...
LAYOUT(LAYOUT_TYPE(param value)) -- layout settings
...
```

## Sözlükleri bellekte saklamanın yolları {#ways-to-store-dictionaries-in-memory}

-   [düzlük](#flat)
-   [karıştırıyordu](#dicts-external_dicts_dict_layout-hashed)
-   [sparse_hashed](#dicts-external_dicts_dict_layout-sparse_hashed)
-   [önbellek](#cache)
-   [direkt](#direct)
-   [range_hashed](#range-hashed)
-   [complex_key_hashed](#complex-key-hashed)
-   [complex_key_cache](#complex-key-cache)
-   [ıp_trie](#ip-trie)

### düzlük {#flat}

Sözlük tamamen düz diziler şeklinde bellekte saklanır. Sözlük ne kadar bellek kullanıyor? Miktar, en büyük anahtarın boyutuyla orantılıdır (kullanılan alanda).

Sözlük anahtarı vardır `UInt64` yazın ve değeri 500.000 ile sınırlıdır. Sözlük oluştururken daha büyük bir anahtar bulunursa, ClickHouse bir özel durum atar ve sözlüğü oluşturmaz.

Her türlü kaynak desteklenmektedir. Güncellerken, veriler (bir dosyadan veya bir tablodan) bütünüyle okunur.

Bu yöntem, sözlüğü saklamak için mevcut tüm yöntemler arasında en iyi performansı sağlar.

Yapılandırma örneği:

``` xml
<layout>
  <flat />
</layout>
```

veya

``` sql
LAYOUT(FLAT())
```

### karıştırıyordu {#dicts-external_dicts_dict_layout-hashed}

Sözlük tamamen bir karma tablo şeklinde bellekte saklanır. Sözlük, uygulamada herhangi bir tanımlayıcıya sahip herhangi bir sayıda öğe içerebilir, anahtar sayısı on milyonlarca öğeye ulaşabilir.

Her türlü kaynak desteklenmektedir. Güncellerken, veriler (bir dosyadan veya bir tablodan) bütünüyle okunur.

Yapılandırma örneği:

``` xml
<layout>
  <hashed />
</layout>
```

veya

``` sql
LAYOUT(HASHED())
```

### sparse_hashed {#dicts-external_dicts_dict_layout-sparse_hashed}

Benzer `hashed`, ancak daha fazla CPU kullanımı lehine daha az bellek kullanır.

Yapılandırma örneği:

``` xml
<layout>
  <sparse_hashed />
</layout>
```

``` sql
LAYOUT(SPARSE_HASHED())
```

### complex_key_hashed {#complex-key-hashed}

Bu tür depolama kompozit ile kullanım içindir [anahtarlar](external-dicts-dict-structure.md). Benzer `hashed`.

Yapılandırma örneği:

``` xml
<layout>
  <complex_key_hashed />
</layout>
```

``` sql
LAYOUT(COMPLEX_KEY_HASHED())
```

### range_hashed {#range-hashed}

Sözlük, sıralı bir aralık dizisi ve bunlara karşılık gelen değerleri olan bir karma tablo şeklinde bellekte saklanır.

Bu depolama yöntemi, hashed ile aynı şekilde çalışır ve anahtara ek olarak tarih/saat (rasgele sayısal tür) aralıklarının kullanılmasına izin verir.

Örnek: tablo, her reklamveren için biçimdeki indirimleri içerir:

``` text
+---------|-------------|-------------|------+
| advertiser id | discount start date | discount end date | amount |
+===============+=====================+===================+========+
| 123           | 2015-01-01          | 2015-01-15        | 0.15   |
+---------|-------------|-------------|------+
| 123           | 2015-01-16          | 2015-01-31        | 0.25   |
+---------|-------------|-------------|------+
| 456           | 2015-01-01          | 2015-01-15        | 0.05   |
+---------|-------------|-------------|------+
```

Tarih aralıkları için bir örnek kullanmak için, `range_min` ve `range_max` element inler [yapılı](external-dicts-dict-structure.md). Bu elemanlar elemanları içermelidir `name` ve`type` (eğer `type` belirtilmemişse, varsayılan tür kullanılır-Tarih). `type` herhangi bir sayısal tür olabilir (Date / DateTime / Uint64 / Int32 / others).

Örnek:

``` xml
<structure>
    <id>
        <name>Id</name>
    </id>
    <range_min>
        <name>first</name>
        <type>Date</type>
    </range_min>
    <range_max>
        <name>last</name>
        <type>Date</type>
    </range_max>
    ...
```

veya

``` sql
CREATE DICTIONARY somedict (
    id UInt64,
    first Date,
    last Date
)
PRIMARY KEY id
LAYOUT(RANGE_HASHED())
RANGE(MIN first MAX last)
```

Bu sözlüklerle çalışmak için, `dictGetT` bir aralığın seçildiği işlev:

``` sql
dictGetT('dict_name', 'attr_name', id, date)
```

Bu işlev belirtilen değerin değerini döndürür `id`s ve geçirilen tarihi içeren tarih aralığı.

Algoritmanın detayları:

-   Eğer... `id` not fo orund veya a range is not fo aund for the `id`, sözlük için varsayılan değeri döndürür.
-   Çakışan aralıklar varsa, herhangi birini kullanabilirsiniz.
-   Aralık sınırlayıcı ise `NULL` veya geçersiz bir tarih (örneğin 1900-01-01 veya 2039-01-01), Aralık açık bırakılır. Aralık her iki tarafta da açık olabilir.

Yapılandırma örneği:

``` xml
<yandex>
        <dictionary>

                ...

                <layout>
                        <range_hashed />
                </layout>

                <structure>
                        <id>
                                <name>Abcdef</name>
                        </id>
                        <range_min>
                                <name>StartTimeStamp</name>
                                <type>UInt64</type>
                        </range_min>
                        <range_max>
                                <name>EndTimeStamp</name>
                                <type>UInt64</type>
                        </range_max>
                        <attribute>
                                <name>XXXType</name>
                                <type>String</type>
                                <null_value />
                        </attribute>
                </structure>

        </dictionary>
</yandex>
```

veya

``` sql
CREATE DICTIONARY somedict(
    Abcdef UInt64,
    StartTimeStamp UInt64,
    EndTimeStamp UInt64,
    XXXType String DEFAULT ''
)
PRIMARY KEY Abcdef
RANGE(MIN StartTimeStamp MAX EndTimeStamp)
```

### önbellek {#cache}

Sözlük, sabit sayıda hücre içeren bir önbellekte saklanır. Bu hücreler sık kullanılan elementleri içerir.

Bir sözlük ararken, önce önbellek aranır. Her veri bloğu için, önbellekte bulunmayan veya güncel olmayan tüm anahtarlar, kaynak kullanılarak istenir `SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`. Alınan veriler daha sonra önbelleğe yazılır.

Önbellek sözlükleri için, sona erme [ömür](external-dicts-dict-lifetime.md) önbellekteki verilerin ayarlanabilir. Eğer daha fazla zaman `lifetime` bir hücrede veri yüklenmesinden bu yana geçti, hücrenin değeri kullanılmaz ve bir dahaki sefere kullanılması gerektiğinde yeniden istenir.
Bu, sözlükleri saklamanın tüm yollarından en az etkilidir. Önbelleğin hızı, doğru ayarlara ve kullanım senaryosuna bağlıdır. Bir önbellek türü sözlüğü, yalnızca isabet oranları yeterince yüksek olduğunda (önerilen %99 ve daha yüksek) iyi performans gösterir. Sen ortalama isabet oranı görebilirsiniz `system.dictionaries` Tablo.

Önbellek performansını artırmak için bir alt sorgu ile kullanın `LIMIT`, ve harici sözlük ile işlevini çağırın.

Destek [kaynaklar](external-dicts-dict-sources.md): MySQL, ClickHouse, yürütülebilir, HTTP.

Ayarlar örneği:

``` xml
<layout>
    <cache>
        <!-- The size of the cache, in number of cells. Rounded up to a power of two. -->
        <size_in_cells>1000000000</size_in_cells>
    </cache>
</layout>
```

veya

``` sql
LAYOUT(CACHE(SIZE_IN_CELLS 1000000000))
```

Yeterince büyük bir önbellek boyutu ayarlayın. Sen hücre sayısını seçmek için deneme gerekir:

1.  Bazı değer ayarlayın.
2.  Önbellek tamamen doluncaya kadar sorguları çalıştırın.
3.  Kullanarak bellek tüketimini değerlendirmek `system.dictionaries` Tablo.
4.  Gerekli bellek tüketimine ulaşılana kadar hücre sayısını artırın veya azaltın.

!!! warning "Uyarıcı"
    Rasgele okuma ile sorguları işlemek için yavaş olduğundan, ClickHouse kaynak olarak kullanmayın.

### complex_key_cache {#complex-key-cache}

Bu tür depolama kompozit ile kullanım içindir [anahtarlar](external-dicts-dict-structure.md). Benzer `cache`.

### direkt {#direct}

Sözlük bellekte saklanmaz ve bir isteğin işlenmesi sırasında doğrudan kaynağa gider.

Sözlük anahtarı vardır `UInt64` tür.

Her türlü [kaynaklar](external-dicts-dict-sources.md), yerel dosyalar dışında desteklenir.

Yapılandırma örneği:

``` xml
<layout>
  <direct />
</layout>
```

veya

``` sql
LAYOUT(DIRECT())
```

### ıp_trie {#ip-trie}

Bu tür depolama, ağ öneklerini (IP adresleri) asn gibi meta verilere eşlemek içindir.

Örnek: tablo, ağ önekleri ve bunlara karşılık gelen sayı ve ülke kodu içerir:

``` text
  +-----------|-----|------+
  | prefix          | asn   | cca2   |
  +=================+=======+========+
  | 202.79.32.0/20  | 17501 | NP     |
  +-----------|-----|------+
  | 2620:0:870::/48 | 3856  | US     |
  +-----------|-----|------+
  | 2a02:6b8:1::/48 | 13238 | RU     |
  +-----------|-----|------+
  | 2001:db8::/32   | 65536 | ZZ     |
  +-----------|-----|------+
```

Bu tür bir düzen kullanırken, yapının bileşik bir anahtarı olmalıdır.

Örnek:

``` xml
<structure>
    <key>
        <attribute>
            <name>prefix</name>
            <type>String</type>
        </attribute>
    </key>
    <attribute>
            <name>asn</name>
            <type>UInt32</type>
            <null_value />
    </attribute>
    <attribute>
            <name>cca2</name>
            <type>String</type>
            <null_value>??</null_value>
    </attribute>
    ...
```

veya

``` sql
CREATE DICTIONARY somedict (
    prefix String,
    asn UInt32,
    cca2 String DEFAULT '??'
)
PRIMARY KEY prefix
```

Anahtarın izin verilen bir IP öneki içeren yalnızca bir dize türü özniteliği olması gerekir. Diğer türler henüz desteklenmiyor.

Sorgular için aynı işlevleri kullanmanız gerekir (`dictGetT` bir tuple ile) kompozit tuşları ile sözlükler gelince:

``` sql
dictGetT('dict_name', 'attr_name', tuple(ip))
```

İşlev ya alır `UInt32` IPv4 için veya `FixedString(16)` IPv6 için:

``` sql
dictGetString('prefix', 'asn', tuple(IPv6StringToNum('2001:db8::1')))
```

Diğer türler henüz desteklenmiyor. İşlev, bu IP adresine karşılık gelen önek için özniteliği döndürür. Örtüşen önekler varsa, en spesifik olanı döndürülür.

Veri bir saklanan `trie`. Tamamen RAM'e uyması gerekir.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_layout/) <!--hide-->
