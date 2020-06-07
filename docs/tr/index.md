---
toc_priority: 0
toc_title: "Genel bak\u0131\u015F"
---

# ClickHouse Nedir? {#what-is-clickhouse}

ClickHouse, sorguların çevrimiçi analitik işlenmesi (*Online Analytical Processing* - OLAP) için sütun odaklı bir Veritabanı Yönetim Sistemidir (*DataBase Management System* - DBMS).

“Normal” bir satır odaklı DBMS içinde veriler şu şekilde saklanır:

| Satır | WatchId     | JavaEnable | Başlık               | İyiOlay | OlayZamanı          |
|-------|-------------|------------|----------------------|---------|---------------------|
| \#0   | 89354350662 | 1          | Yatırımcı İlişkileri | 1       | 2016-05-18 05:19:20 |
| \#1   | 90329509958 | 0          | Bize ulaşın          | 1       | 2016-05-18 08:10:20 |
| \#2   | 89953706054 | 1          | Görev                | 1       | 2016-05-18 07:38:00 |
| \#N   | …           | …          | …                    | …       | …                   |

Başka bir deyişle, bir satırla ilgili tüm değerler fiziksel olarak yan yana depolanır.

MySQL, Postgres ve MS SQL Server gibi veritabanları satır odaklı DBMS örnekleridir.

Sütun odaklı bir DBMS’de ise veriler şu şekilde saklanır:

| Satır:      | \#0                  | \#1                 | \#2                 | \#N |
|-------------|----------------------|---------------------|---------------------|-----|
| WatchId:    | 89354350662          | 90329509958         | 89953706054         | …   |
| JavaEnable: | 1                    | 0                   | 1                   | …   |
| Başlık:     | Yatırımcı İlişkileri | Bize ulaşın         | Görev               | …   |
| İyiOlay:    | 1                    | 1                   | 1                   | …   |
| OlayZamanı: | 2016-05-18 05:19:20  | 2016-05-18 08:10:20 | 2016-05-18 07:38:00 | …   |

Bu örnekler yalnızca verilerin düzenlendiği sırayı gösterir. Farklı sütunlardaki değerler ayrı olarak depolanır ve aynı sütundaki veriler birlikte depolanır.

Sütun odaklı DBMS örnekleri: Vertica, Paraccel (Actian Matrix ve Amazon Redshift), Sybase IQ, Exasol, Infobright, InfiniDB, MonetDB (VectorWise ve Actian vektör), LucidDB, SAP HANA, Google Dremel, Google PowerDrill, Druid ve kdb+.

Verinin farklı bir şekilde sıralanarak depolanması, bazı veri erişim senaryoları için daha uygundur. Veri erişim senaryosu, hangi sorguların ne kadar sıklıkla yapıldığını, ne kadar verinin okunduğu, bunların hangi tiplerde hangi kolonlardan, satırlardan ve hangi miktarda(bayt olarak) okunacağını; verinin okunması ile güncellenmesi arasındaki ilişkiyi; verinin işlenen boyutu ve ne kadar yerel olduğunu; veri değiş-tokuşunun(transaction) olup olmayacağını, olacaksa diğer işlemlerden ne kadat yalıtılacağını; verilerin kopyalanması ve mantıksal bütünlük intiyaçlarını; her sorgu türünün gecikme ve iletim debisi ihtiyaçlarını gösterir.

Sistem üzerindeki yük ne kadar fazlaysa, sistem ayarlarının kullanım senaryolarına uyarlanması ve bu ayarların ne kadar hassas olduğu da o kadar önemli hale gelir. Birbirinden büyük ölçüde farklı olan veri erişim senaryolarına tam uyum sağlayan, yani her işe ve yüke gelen bir sistem yoktur. Eğer bir sistem yük altında her türlü veri erişim senaryosuna adapte olabiliyorsa, o halde böyle bir sistem ya tüm senaryolara ya da senaryoların bir veya birkaçına karşı zayıp bir performans gösterir.

## OLAP Senaryosunun Temel özellikleri {#key-properties-of-olap-scenario}

-   İsteklerin büyük çoğunluğu, okuma erişimi içindir.
-   Veriler, tek satırlarla değil, oldukça büyük gruplar halinde (\> 1000 satır) güncellenir; veya hiç güncellenmez.
-   Veri, veritabanına eklenir, ancak değiştirilmez.
-   Bazı sorgular için veritabanından den oldukça fazla sayıda satır çekilir, ancak sonuç sadece birkaç satır ve sütunludur.
-   Tablolar “geniştir”, yani bir tabloda çok sayıda kolon vardır(onlarca).
-   Sorgular sıkılığı diğer senaryolara göre daha azdır (genellikle sunucu başına saniyede 100 veya daha az sorgu gelir).
-   Basit sorgular için, 50 ms civarında gecikmelere izin verilir.
-   Saklanan veriler oldukça küçüktür: genelde sadece sayılar ve kısa metinler içerir(örneğin, URL başına 60 bayt).
-   Tek bir sorguyu işlemek yüksek miktarda veri okunmasını gerektirir(sunucu başına saniyede milyarlarca satıra kadar).
-   Veri değiş-tokuşu(transaction) gerekli değildir.
-   Veri tutarlılığı o kadar da önemli değildir.
-   Genelde bir tane çok büyük tablo vardır, gerisi küçük tablolardan oluşur
-   Bir sorgu sonucu elde edilen veri, okuanan veri miktarından oldukça küçüktür. Başka bir deyişle, milyarlarca satır içinden veriler süzgeçlenerek veya birleştirilerek elde edilen verilerin tek bir sunucunun RAM’ine sığar.

OLAP senaryosunun diğer popüler senaryolardan (*Online Transactional Processing* - OLTP veya *Key-Value* veritabanı) çok farklı olduğu açıkça görülebilir. Bu nedenle, iyi bir performans elde etmek istiyorsanız, analitik sorguları işlemek için OLTP veya *Key-Value* veritabanlarını kullanmak pek mantıklı olmaz. Örneğin, analitik için MongoDB veya Redis kullanmaya çalışırsanız, OLAP veritabanlarına kıyasla çok düşük performans elde edersiniz.

## Sütun yönelimli veritabanları OLAP Senaryosunda Neden Daha Iyi çalışır {#why-column-oriented-databases-work-better-in-the-olap-scenario}

Sütun yönelimli veritabanları OLAP senaryolarına daha uygundur: hatta o kadar ki, çoğu sorgunun işlenmesi en az 100 kat daha hızlıdır. Her ne kadar OLAP veritabanlarının neden bu kadar hızlı olduğuna dair nedenler aşağıda ayrıntılı verilmiş olsa da görseller üzerinden anlatmak daha kolay olacakttır:

**Satır yönelimli DBMS**

![Row-oriented](images/row-oriented.gif#)

**Sütun yönelimli DBMS**

![Column-oriented](images/column-oriented.gif#)

Farkı görüyor musunuz?

### Giriş/çıkış {#inputoutput}

1.  Analitik bir sorgu için, yalnızca az sayıda tablo sütununun okunması gerekir. Sütun yönelimli bir veritabanında, yalnızca ihtiyacınız olan verileri okuyabilirsiniz. Örneğin, 100 üzerinden 5 sütun gerekiyorsa, g/Ç’de 20 kat azalma bekleyebilirsiniz.
2.  Veri paketler halinde okunduğundan, sıkıştırılması daha kolaydır. Sütunlardaki verilerin sıkıştırılması da daha kolaydır. Bu, G/Ç hacmini daha da azaltır.
3.  Azaltılmış G/Ç nedeniyle, sistem önbelleğine daha fazla veri sığar.

Örneğin, sorgu “count the number of records for each advertising platform” bir okuma gerektirir “advertising platform ID” 1 bayt sıkıştırılmamış kadar alır sütun. Trafiğin çoğu reklam platformlarından değilse, bu sütunun en az 10 kat sıkıştırılmasını bekleyebilirsiniz. Hızlı bir sıkıştırma algoritması kullanırken, saniyede en az birkaç gigabayt sıkıştırılmamış veri hızında veri dekompresyonu mümkündür. Başka bir deyişle, bu sorgu, tek bir sunucuda saniyede yaklaşık birkaç milyar satır hızında işlenebilir. Bu hız aslında pratikte elde edilir.

### CPU {#cpu}

Bir sorguyu yürütmek çok sayıda satırı işlemeyi gerektirdiğinden, ayrı satırlar yerine tüm vektörler için tüm işlemlerin gönderilmesine veya sorgu motorunun neredeyse hiç gönderim maliyeti olmaması için uygulanmasına yardımcı olur. Bunu yapmazsanız, yarı iyi bir disk alt sistemi ile, sorgu yorumlayıcısı kaçınılmaz olarak CPU’yu durdurur. Hem verileri sütunlarda depolamak hem de mümkün olduğunda sütunlarla işlemek mantıklıdır.

Bunu yapmanın iki yolu vardır:

1.  Bir vektör motoru. Tüm işlemler ayrı değerler yerine vektörler için yazılır. Bu, işlemleri çok sık aramanıza gerek olmadığı ve sevkiyatın maliyetlerinin ihmal edilebilir olduğu anlamına gelir. İşlem kodu optimize edilmiş bir iç döngü içerir.

2.  Kod üretimi. Sorgu için oluşturulan kod, içindeki tüm dolaylı çağrılara sahiptir.

Bu yapılmaz “normal” veritabanları, çünkü basit sorguları çalıştırırken mantıklı değil. Ancak, istisnalar vardır. Örneğin, MemSQL SQL sorgularını işlerken gecikmeyi azaltmak için kod oluşturma kullanır. (Karşılaştırma için, analitik Dbms’ler gecikme değil, verim optimizasyonunu gerektirir .)

CPU verimliliği için sorgu dilinin bildirimsel (SQL veya MDX) veya en az bir vektör (J, K) olması gerektiğini unutmayın. Sorgu yalnızca en iyi duruma getirme için izin veren örtük döngüler içermelidir.

{## [Orijinal makale](https://clickhouse.tech/docs/en/) ##}
