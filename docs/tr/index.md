---
machine_translated: true
machine_translated_rev: e8cd92bba3269f47787db090899f7c242adf7818
toc_priority: 0
toc_title: "Genel bak\u0131\u015F"
---

# ClickHouse nedir? {#what-is-clickhouse}

ClickHouse, sorguların çevrimiçi analitik işlenmesi (OLAP) için sütun odaklı bir veritabanı yönetim sistemidir (DBMS).

İn a “normal” satır yönelimli DBMS, veri bu sırayla saklanır:

| Satır | Watchıd     | JavaEnable | Başlık               | GoodEvent | EventTime           |
|-------|-------------|------------|----------------------|-----------|---------------------|
| \#0   | 89354350662 | 1          | Yatırımcı İlişkileri | 1         | 2016-05-18 05:19:20 |
| \#1   | 90329509958 | 0          | Bize ulaşın          | 1         | 2016-05-18 08:10:20 |
| \#2   | 89953706054 | 1          | Görev                | 1         | 2016-05-18 07:38:00 |
| \#N   | …           | …          | …                    | …         | …                   |

Başka bir deyişle, bir satırla ilgili tüm değerler fiziksel olarak yan yana depolanır.

Satır yönelimli DBMS örnekleri MySQL, Postgres ve MS SQL Server'dır.

Sütun yönelimli bir DBMS'DE, veriler şu şekilde saklanır:

| Satır:      | \#0                  | \#1                 | \#2                 | \#N |
|-------------|----------------------|---------------------|---------------------|-----|
| Watchıd:    | 89354350662          | 90329509958         | 89953706054         | …   |
| JavaEnable: | 1                    | 0                   | 1                   | …   |
| Başlık:     | Yatırımcı İlişkileri | Bize ulaşın         | Görev               | …   |
| GoodEvent:  | 1                    | 1                   | 1                   | …   |
| EventTime:  | 2016-05-18 05:19:20  | 2016-05-18 08:10:20 | 2016-05-18 07:38:00 | …   |

Bu örnekler yalnızca verilerin düzenlendiği sırayı gösterir. Farklı sütunlardaki değerler ayrı olarak depolanır ve aynı sütundaki veriler birlikte depolanır.

Bir sütun odaklı DBMS örnekleri: Vertica, Paraccel (Actian Matrix ve Amazon Redshift), Sybase IQ, Exasol, Infobright, InfiniDB, MonetDB (VectorWise ve Actian vektör), LucidDB, SAP HANA, Google Dremel, Google PowerDrill, Druid ve kdb+.

Different orders for storing data are better suited to different scenarios. The data access scenario refers to what queries are made, how often, and in what proportion; how much data is read for each type of query – rows, columns, and bytes; the relationship between reading and updating data; the working size of the data and how locally it is used; whether transactions are used, and how isolated they are; requirements for data replication and logical integrity; requirements for latency and throughput for each type of query, and so on.

Sistem üzerindeki yük ne kadar yüksek olursa, kullanım senaryosunun gereksinimlerine uyacak şekilde ayarlanmış sistemi özelleştirmek o kadar önemlidir ve bu özelleştirme o kadar ince taneli olur. Önemli ölçüde farklı senaryolara eşit derecede uygun bir sistem yoktur. Bir sistem geniş bir senaryo kümesine uyarlanabilirse, yüksek bir yük altında, sistem tüm senaryoları eşit derecede zayıf bir şekilde ele alır veya olası senaryolardan yalnızca biri veya birkaçı için iyi çalışır.

## OLAP senaryosunun temel özellikleri {#key-properties-of-olap-scenario}

-   İsteklerin büyük çoğunluğu okuma erişimi içindir.
-   Veriler, tek satırlarla değil, oldukça büyük gruplar halinde (\> 1000 satır) güncellenir; veya hiç güncellenmez.
-   Veri DB eklenir, ancak değiştirilmez.
-   Okumalar için, dB'den oldukça fazla sayıda satır çıkarılır,ancak yalnızca küçük bir sütun alt kümesi.
-   Tablolar şunlardır “wide,” çok sayıda sütun içerdikleri anlamına gelir.
-   Sorgular nispeten nadirdir (genellikle sunucu başına yüzlerce sorgu veya saniyede daha az).
-   Basit sorgular için, 50 ms civarında gecikmelere izin verilir.
-   Sütun değerleri oldukça küçüktür: sayılar ve kısa dizeler (örneğin, URL başına 60 bayt).
-   Tek bir sorguyu işlerken yüksek verim gerektirir (sunucu başına saniyede milyarlarca satıra kadar).
-   İşlemler gerekli değildir.
-   Veri tutarlılığı için düşük gereksinimler.
-   Sorgu başına bir büyük tablo var. Biri hariç tüm tablolar küçüktür.
-   Bir sorgu sonucu, kaynak veriden önemli ölçüde daha küçüktür. Başka bir deyişle, veriler filtrelenir veya toplanır, böylece sonuç tek bir sunucunun RAM'İNE sığar.

OLAP senaryosunun diğer popüler senaryolardan (OLTP veya anahtar değeri erişimi gibi) çok farklı olduğunu görmek kolaydır. Bu nedenle, iyi bir performans elde etmek istiyorsanız, analitik sorguları işlemek için OLTP veya anahtar değeri DB'Yİ kullanmayı denemek mantıklı değildir. Örneğin, analitik için MongoDB veya Redis kullanmaya çalışırsanız, OLAP veritabanlarına kıyasla çok düşük performans elde edersiniz.

## Sütun yönelimli veritabanları OLAP senaryosunda neden daha iyi çalışır {#why-column-oriented-databases-work-better-in-the-olap-scenario}

Sütun yönelimli veritabanları OLAP senaryolarına daha uygundur: çoğu sorgunun işlenmesinde en az 100 kat daha hızlıdır. Nedenleri aşağıda ayrıntılı olarak açıklanmıştır, ancak gerçek görsel olarak göstermek daha kolaydır:

**Satır yönelimli DBMS**

![Row-oriented](images/row_oriented.gif#)

**Sütun yönelimli DBMS**

![Column-oriented](images/column_oriented.gif#)

Farkı görüyor musun?

### Giriş/çıkış {#inputoutput}

1.  Analitik bir sorgu için, yalnızca az sayıda tablo sütununun okunması gerekir. Sütun yönelimli bir veritabanında, yalnızca ihtiyacınız olan verileri okuyabilirsiniz. Örneğin, 100 üzerinden 5 sütun gerekiyorsa, g/Ç'de 20 kat azalma bekleyebilirsiniz.
2.  Veri paketler halinde okunduğundan, sıkıştırılması daha kolaydır. Sütunlardaki verilerin sıkıştırılması da daha kolaydır. Bu, G/Ç hacmini daha da azaltır.
3.  Azaltılmış G/Ç nedeniyle, sistem önbelleğine daha fazla veri sığar.

Örneğin, sorgu “count the number of records for each advertising platform” bir okuma gerektirir “advertising platform ID” 1 bayt sıkıştırılmamış kadar alır sütun. Trafiğin çoğu reklam platformlarından değilse, bu sütunun en az 10 kat sıkıştırılmasını bekleyebilirsiniz. Hızlı bir sıkıştırma algoritması kullanırken, saniyede en az birkaç gigabayt sıkıştırılmamış veri hızında veri dekompresyonu mümkündür. Başka bir deyişle, bu sorgu, tek bir sunucuda saniyede yaklaşık birkaç milyar satır hızında işlenebilir. Bu hız aslında pratikte elde edilir.

<details markdown="1">

<summary>Örnek</summary>

``` bash
$ clickhouse-client
ClickHouse client version 0.0.52053.
Connecting to localhost:9000.
Connected to ClickHouse server version 0.0.52053.
```

``` sql
SELECT CounterID, count() FROM hits GROUP BY CounterID ORDER BY count() DESC LIMIT 20
```

``` text
┌─CounterID─┬──count()─┐
│    114208 │ 56057344 │
│    115080 │ 51619590 │
│      3228 │ 44658301 │
│     38230 │ 42045932 │
│    145263 │ 42042158 │
│     91244 │ 38297270 │
│    154139 │ 26647572 │
│    150748 │ 24112755 │
│    242232 │ 21302571 │
│    338158 │ 13507087 │
│     62180 │ 12229491 │
│     82264 │ 12187441 │
│    232261 │ 12148031 │
│    146272 │ 11438516 │
│    168777 │ 11403636 │
│   4120072 │ 11227824 │
│  10938808 │ 10519739 │
│     74088 │  9047015 │
│    115079 │  8837972 │
│    337234 │  8205961 │
└───────────┴──────────┘
```

</details>

### CPU {#cpu}

Bir sorguyu yürütmek çok sayıda satırı işlemeyi gerektirdiğinden, ayrı satırlar yerine tüm vektörler için tüm işlemlerin gönderilmesine veya sorgu motorunun neredeyse hiç gönderim maliyeti olmaması için uygulanmasına yardımcı olur. Bunu yapmazsanız, yarı iyi bir disk alt sistemi ile, sorgu yorumlayıcısı kaçınılmaz olarak CPU'yu durdurur. Hem verileri sütunlarda depolamak hem de mümkün olduğunda sütunlarla işlemek mantıklıdır.

Bunu yapmanın iki yolu vardır:

1.  Bir vektör motoru. Tüm işlemler ayrı değerler yerine vektörler için yazılır. Bu, işlemleri çok sık aramanıza gerek olmadığı ve sevkiyatın maliyetlerinin ihmal edilebilir olduğu anlamına gelir. İşlem kodu optimize edilmiş bir iç döngü içerir.

2.  Kod üretimi. Sorgu için oluşturulan kod, içindeki tüm dolaylı çağrılara sahiptir.

Bu yapılmaz “normal” veritabanları, çünkü basit sorguları çalıştırırken mantıklı değil. Ancak, istisnalar vardır. Örneğin, MemSQL SQL sorgularını işlerken gecikmeyi azaltmak için kod oluşturma kullanır. (Karşılaştırma için, analitik Dbms'ler gecikme değil, verim optimizasyonunu gerektirir .)

CPU verimliliği için sorgu dilinin bildirimsel (SQL veya MDX) veya en az bir vektör (J, K) olması gerektiğini unutmayın. Sorgu yalnızca en iyi duruma getirme için izin veren örtük döngüler içermelidir.

{## [Orijinal makale](https://clickhouse.tech/docs/en/) ##}
