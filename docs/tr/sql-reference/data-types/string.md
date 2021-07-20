---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: Dize
---

# Dize {#string}

Keyfi uzunlukta dizeler. Uzunluk sınırlı değildir. Değer, boş bayt da dahil olmak üzere rasgele bir bayt kümesi içerebilir.
Dize türü türleri değiştirir VARCHAR, BLOB, CLOB, ve diğerleri diğer DBMSs.

## Kodlamalar {#encodings}

ClickHouse kodlama kavramına sahip değildir. Dizeler, depolanan ve olduğu gibi çıkan rasgele bir bayt kümesi içerebilir.
Metinleri saklamanız gerekiyorsa, UTF-8 kodlamasını kullanmanızı öneririz. En azından, terminaliniz UTF-8 kullanıyorsa (önerildiği gibi), dönüşüm yapmadan değerlerinizi okuyabilir ve yazabilirsiniz.
Benzer şekilde, dizelerle çalışmak için belirli işlevler, dizenin UTF-8 kodlu bir metni temsil eden bir bayt kümesi içerdiği varsayımı altında çalışan ayrı varyasyonlara sahiptir.
Örneğin, ‘length’ işlev, bayt cinsinden dize uzunluğunu hesaplar; ‘lengthUTF8’ işlev, değerin UTF-8 kodlanmış olduğunu varsayarak Unicode kod noktalarındaki dize uzunluğunu hesaplar.

[Orijinal makale](https://clickhouse.tech/docs/en/data_types/string/) <!--hide-->
