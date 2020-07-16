---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 42
toc_title: "S\xF6zl\xFCk G\xFCncellemeleri"
---

# Sözlük Güncellemeleri {#dictionary-updates}

ClickHouse sözlükleri periyodik olarak günceller. Tam olarak karşıdan yüklenen sözlükler için Güncelleştirme aralığı ve önbelleğe alınmış sözlükler için geçersiz kılma aralığı `<lifetime>` saniyeler içinde etiketleyin.

Sözlük güncelleştirmeleri (ilk kullanım için yükleme dışında) sorguları engellemez. Güncellemeler sırasında, bir sözlüğün eski sürümü kullanılır. Güncelleştirme sırasında bir hata oluşursa, hata sunucu günlüğüne yazılır ve sorgular sözlüklerin eski sürümünü kullanmaya devam eder.

Ayarlar örneği:

``` xml
<dictionary>
    ...
    <lifetime>300</lifetime>
    ...
</dictionary>
```

``` sql
CREATE DICTIONARY (...)
...
LIFETIME(300)
...
```

Ayar `<lifetime>0</lifetime>` (`LIFETIME(0)`) söz dictionarieslük .lerin güncel updatinglenmesini engeller.

Yükseltmeler için bir zaman aralığı ayarlayabilirsiniz ve ClickHouse bu aralıkta eşit rastgele bir zaman seçecektir. Bu, çok sayıda sunucuda yükseltme yaparken yükü sözlük kaynağına dağıtmak için gereklidir.

Ayarlar örneği:

``` xml
<dictionary>
    ...
    <lifetime>
        <min>300</min>
        <max>360</max>
    </lifetime>
    ...
</dictionary>
```

veya

``` sql
LIFETIME(MIN 300 MAX 360)
```

Eğer `<min>0</min>` ve `<max>0</max>`, ClickHouse sözlüğü zaman aşımı ile yeniden yüklemez.
Bu durumda, Sözlük yapılandırma dosyası değiştirilmişse veya ClickHouse sözlüğü daha önce yeniden yükleyebilir. `SYSTEM RELOAD DICTIONARY` komut yürütüldü.

Sözlükleri yükseltirken, ClickHouse sunucusu türüne bağlı olarak farklı mantık uygular [kaynaklı](external-dicts-dict-sources.md):

Sözlükleri yükseltirken, ClickHouse sunucusu türüne bağlı olarak farklı mantık uygular [kaynaklı](external-dicts-dict-sources.md):

-   Bir metin dosyası için değişiklik zamanını kontrol eder. Zaman önceden kaydedilmiş zaman farklıysa, sözlük güncelleştirilir.
-   Myısam tabloları için, değişiklik zamanı bir `SHOW TABLE STATUS` sorgu.
-   Diğer kaynaklardan gelen sözlükler varsayılan olarak her zaman güncellenir.

MySQL (InnoDB), ODBC ve ClickHouse kaynakları için, sözlükleri her seferinde değil, gerçekten değiştiyse güncelleyecek bir sorgu ayarlayabilirsiniz. Bunu yapmak için şu adımları izleyin:

-   Sözlük tablosu, kaynak verileri güncelleştirildiğinde her zaman değişen bir alana sahip olmalıdır.
-   Kaynak ayarları, değişen alanı alan bir sorgu belirtmeniz gerekir. ClickHouse sunucu sorgu sonucu bir satır olarak yorumlar ve bu satır önceki durumuna göre değişmişse, sözlük güncelleştirilir. Sorguda belirtme `<invalidate_query>` için ayar fieldlardaki alan [kaynaklı](external-dicts-dict-sources.md).

Ayarlar örneği:

``` xml
<dictionary>
    ...
    <odbc>
      ...
      <invalidate_query>SELECT update_time FROM dictionary_source where id = 1</invalidate_query>
    </odbc>
    ...
</dictionary>
```

veya

``` sql
...
SOURCE(ODBC(... invalidate_query 'SELECT update_time FROM dictionary_source where id = 1'))
...
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_lifetime/) <!--hide-->
