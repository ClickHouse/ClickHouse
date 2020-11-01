---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: "Genel A\xE7\u0131klama"
---

# Dış Söz Dictionarieslükler {#dicts-external-dicts}

Çeşitli veri kaynaklarından kendi sözlükleri ekleyebilirsiniz. Bir sözlük için veri kaynağı, yerel bir metin veya yürütülebilir dosya, bir HTTP(s) kaynağı veya başka bir DBMS olabilir. Daha fazla bilgi için, bkz. “[Dış sözlükler için kaynaklar](external-dicts-dict-sources.md)”.

ClickHouse:

-   Sözlükleri RAM'de tamamen veya kısmen saklar.
-   Sözlükleri periyodik olarak günceller ve eksik değerleri dinamik olarak yükler. Başka bir deyişle, sözlükler dinamik olarak yüklenebilir.
-   Xml dosyaları ile harici sözlükler oluşturmak için izin verir veya [DDL sorguları](../../statements/create.md#create-dictionary-query).

Dış sözlüklerin yapılandırması bir veya daha fazla xml dosyasında bulunabilir. Yapılandırma yolu belirtilen [dictionaries_config](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_config) parametre.

Sözlükler sunucu başlangıçta veya ilk kullanımda, bağlı olarak yüklenebilir [dictionaries_lazy_load](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load) ayar.

Bu [sözlükler](../../../operations/system-tables.md#system_tables-dictionaries) sistem tablosu sunucuda yapılandırılmış sözlükler hakkında bilgi içerir. Her sözlük için orada bulabilirsiniz:

-   Sözlük durumu.
-   Yapılandırma parametreleri.
-   Sözlük için ayrılan RAM miktarı veya sözlük başarıyla yüklendiğinden bu yana bir dizi sorgu gibi metrikler.

Sözlük yapılandırma dosyası aşağıdaki biçime sahiptir:

``` xml
<yandex>
    <comment>An optional element with any content. Ignored by the ClickHouse server.</comment>

    <!--Optional element. File name with substitutions-->
    <include_from>/etc/metrika.xml</include_from>


    <dictionary>
        <!-- Dictionary configuration. -->
        <!-- There can be any number of <dictionary> sections in the configuration file. -->
    </dictionary>

</yandex>
```

Yapabilirsin [yapılandırmak](external-dicts-dict.md) aynı dosyada sözlükler herhangi bir sayıda.

[Sözlükler için DDL sorguları](../../statements/create.md#create-dictionary-query) sunucu yapılandırmasında herhangi bir ek kayıt gerektirmez. Tablolar veya görünümler gibi birinci sınıf varlıklar olarak sözlüklerle çalışmaya izin verirler.

!!! attention "Dikkat"
    Küçük bir sözlük için değerleri, bir `SELECT` sorgu (bkz. [dönüştürmek](../../../sql-reference/functions/other-functions.md) işlev). Bu işlevsellik harici sözlüklerle ilgili değildir.

## Ayrıca Bakınız {#ext-dicts-see-also}

-   [Harici bir sözlük yapılandırma](external-dicts-dict.md)
-   [Sözlükleri bellekte saklama](external-dicts-dict-layout.md)
-   [Sözlük Güncellemeleri](external-dicts-dict-lifetime.md)
-   [Dış Sözlüklerin kaynakları](external-dicts-dict-sources.md)
-   [Sözlük anahtarı ve alanları](external-dicts-dict-structure.md)
-   [Harici Sözlüklerle çalışmak için işlevler](../../../sql-reference/functions/ext-dict-functions.md)

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts/) <!--hide-->
