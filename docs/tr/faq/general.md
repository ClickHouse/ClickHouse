---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 78
toc_title: Genel Sorular
---

# Genel Sorular {#general-questions}

## Neden MapReduce Gibi Bir Şey Kullanmıyorsun? {#why-not-use-something-like-mapreduce}

Mapreduce gibi sistemlere, azaltma işleminin dağıtılmış sıralamaya dayandığı dağıtılmış bilgi işlem sistemleri olarak başvurabiliriz. Bu sınıftaki en yaygın açık kaynak çözümü [Apache Hadoop](http://hadoop.apache.org). Yandex, şirket içi çözümünü, YT'Yİ kullanıyor.

Bu sistemler, yüksek gecikme süreleri nedeniyle çevrimiçi sorgular için uygun değildir. Başka bir deyişle, bir web arayüzü için arka uç olarak kullanılamazlar. Bu tür sistemler gerçek zamanlı veri güncellemeleri için yararlı değildir. Dağıtılmış sıralama, işlemin sonucu ve tüm ara sonuçlar (varsa) tek bir sunucunun RAM'İNDE bulunuyorsa, genellikle çevrimiçi sorgular için geçerli olan işlemleri azaltmanın en iyi yolu değildir. Böyle bir durumda, bir karma tablo azaltma işlemlerini gerçekleştirmek için en uygun yoldur. Harita azaltma görevlerini optimize etmek için ortak bir yaklaşım, RAM'de bir karma tablo kullanarak ön toplama (kısmi azaltma) ' dir. Kullanıcı bu optimizasyonu manuel olarak gerçekleştirir. Dağıtılmış sıralama, basit harita azaltma görevlerini çalıştırırken düşük performansın ana nedenlerinden biridir.

Çoğu MapReduce uygulaması, bir kümede rasgele kod çalıştırmanıza izin verir. Ancak bildirimsel bir sorgu dili, deneyleri hızlı bir şekilde çalıştırmak için OLAP için daha uygundur. Örneğin, Hadoop kovanı ve domuz vardır. Ayrıca Spark için Cloudera Impala veya Shark'ı (modası geçmiş) ve Spark SQL, Presto ve Apache Drill'i de düşünün. Bu tür görevleri çalıştırırken performans, özel sistemlere kıyasla oldukça düşük bir seviyededir, ancak nispeten yüksek gecikme, bu sistemleri bir web arayüzü için arka uç olarak kullanmayı gerçekçi kılmaktadır.

## Oracle aracılığıyla ODBC kullanırken Kodlamalarla ilgili bir sorunum varsa ne olur? {#oracle-odbc-encodings}

Oracle ODBC sürücüsü aracılığıyla dış sözlükler kaynağı olarak kullanırsanız, doğru değeri ayarlamanız gerekir. `NLS_LANG` ortam değişkeni `/etc/default/clickhouse`. Daha fazla bilgi için, bkz: [Oracle NLS\_LANG SSS](https://www.oracle.com/technetwork/products/globalization/nls-lang-099431.html).

**Örnek**

``` sql
NLS_LANG=RUSSIAN_RUSSIA.UTF8
```

## Clickhouse'dan bir dosyaya verileri nasıl dışa aktarırım? {#how-to-export-to-file}

### INTO OUTFİLE yan tümcesini kullanma {#using-into-outfile-clause}

Add an [INTO OUTFILE](../sql-reference/statements/select/into-outfile.md#into-outfile-clause) sorgunuza yan tümce.

Mesela:

``` sql
SELECT * FROM table INTO OUTFILE 'file'
```

Varsayılan olarak, ClickHouse kullanır [TabSeparated](../interfaces/formats.md#tabseparated) çıktı verileri için Biçim. Seçmek için [Veri formatı](../interfaces/formats.md), use the [FORMAT CLA clauseuse](../sql-reference/statements/select/format.md#format-clause).

Mesela:

``` sql
SELECT * FROM table INTO OUTFILE 'file' FORMAT CSV
```

### Dosya altyapısı tablosu kullanma {#using-a-file-engine-table}

Görmek [Dosya](../engines/table-engines/special/file.md).

### Komut Satırı Yeniden Yönlendirmesini Kullanma {#using-command-line-redirection}

``` sql
$ clickhouse-client --query "SELECT * from table" --format FormatName > result.txt
```

Görmek [clickhouse-müşteri](../interfaces/cli.md).

{## [Orijinal makale](https://clickhouse.tech/docs/en/faq/general/) ##}
