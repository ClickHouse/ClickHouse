---
machine_translated: true
machine_translated_rev: e8cd92bba3269f47787db090899f7c242adf7818
toc_priority: 58
toc_title: "Genel bak\u0131\u015F"
---

# Etkiler {#domains}

Etki alanları, varolan temel türün üstüne bazı ek özellikler ekleyen, ancak temel veri türünün kablolu ve disk üstü biçimini sağlam bırakan özel amaçlı türlerdir. Şu anda, ClickHouse kullanıcı tanımlı etki alanlarını desteklemiyor.

Örneğin, ilgili taban türünün kullanılabileceği her yerde etki alanlarını kullanabilirsiniz:

-   Etki alanı türünde bir sütun oluşturma
-   Alan sütunundan/alanına değerleri okuma / yazma
-   Bir temel türü bir dizin olarak kullanılabilir, bir dizin olarak kullanın
-   Etki alanı sütun değerleri ile çağrı fonksiyonları

### Alanların Ekstra özellikleri {#extra-features-of-domains}

-   Açık sütun türü adı `SHOW CREATE TABLE` veya `DESCRIBE TABLE`
-   İle insan dostu format inputtan giriş `INSERT INTO domain_table(domain_column) VALUES(...)`
-   İçin insan dostu forma outputta çıktı `SELECT domain_column FROM domain_table`
-   Harici bir kaynaktan insan dostu biçimde veri yükleme: `INSERT INTO domain_table FORMAT CSV ...`

### Sınırlamalar {#limitations}

-   Temel türün dizin sütununu etki alanı türüne dönüştürülemiyor `ALTER TABLE`.
-   Başka bir sütun veya tablodan veri eklerken dize değerlerini dolaylı olarak etki alanı değerlerine dönüştüremez.
-   Etki alanı, depolanan değerler üzerinde hiçbir kısıtlama ekler.

[Orijinal makale](https://clickhouse.tech/docs/en/data_types/domains/overview) <!--hide-->
