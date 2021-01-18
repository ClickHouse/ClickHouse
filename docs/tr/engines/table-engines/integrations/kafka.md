---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 32
toc_title: Kafka
---

# Kafka {#kafka}

Bu motor ile çalışır [Apache Kafka](http://kafka.apache.org/).

Kafka sağlar:

-   Veri akışlarını yayınlayın veya abone olun.
-   Hataya dayanıklı depolama düzenlemek.
-   Kullanılabilir hale geldikçe akışları işleyin.

## Tablo oluşturma {#table_engine-kafka-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'host:port',
    kafka_topic_list = 'topic1,topic2,...',
    kafka_group_name = 'group_name',
    kafka_format = 'data_format'[,]
    [kafka_row_delimiter = 'delimiter_symbol',]
    [kafka_schema = '',]
    [kafka_num_consumers = N,]
    [kafka_max_block_size = 0,]
    [kafka_skip_broken_messages = N,]
    [kafka_commit_every_batch = 0]
```

Gerekli parametreler:

-   `kafka_broker_list` – A comma-separated list of brokers (for example, `localhost:9092`).
-   `kafka_topic_list` – A list of Kafka topics.
-   `kafka_group_name` – A group of Kafka consumers. Reading margins are tracked for each group separately. If you don't want messages to be duplicated in the cluster, use the same group name everywhere.
-   `kafka_format` – Message format. Uses the same notation as the SQL `FORMAT` fonksiyon gibi `JSONEachRow`. Daha fazla bilgi için, bkz: [Biçimliler](../../../interfaces/formats.md) bölme.

İsteğe bağlı parametreler:

-   `kafka_row_delimiter` – Delimiter character, which ends the message.
-   `kafka_schema` – Parameter that must be used if the format requires a schema definition. For example, [Cap'n Proto](https://capnproto.org/) şema dosyasının yolunu ve kök adını gerektirir `schema.capnp:Message` nesne.
-   `kafka_num_consumers` – The number of consumers per table. Default: `1`. Bir tüketicinin verimi yetersizse daha fazla tüketici belirtin. Bölüm başına yalnızca bir tüketici atanabileceğinden, toplam tüketici sayısı konudaki bölüm sayısını geçmemelidir.
-   `kafka_max_block_size` - Anket için maksimum toplu iş boyutu (mesajlarda) (varsayılan: `max_block_size`).
-   `kafka_skip_broken_messages` – Kafka message parser tolerance to schema-incompatible messages per block. Default: `0`. Eğer `kafka_skip_broken_messages = N` sonra motor atlar *N* Ayrıştırılamayan Kafka iletileri (bir ileti bir veri satırına eşittir).
-   `kafka_commit_every_batch` - Bütün bir blok yazdıktan sonra tek bir taahhüt yerine tüketilen ve işlenen her toplu işlemi gerçekleştirin (varsayılan: `0`).

Örnekler:

``` sql
  CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  SELECT * FROM queue LIMIT 5;

  CREATE TABLE queue2 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka SETTINGS kafka_broker_list = 'localhost:9092',
                            kafka_topic_list = 'topic',
                            kafka_group_name = 'group1',
                            kafka_format = 'JSONEachRow',
                            kafka_num_consumers = 4;

  CREATE TABLE queue2 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1')
              SETTINGS kafka_format = 'JSONEachRow',
                       kafka_num_consumers = 4;
```

<details markdown="1">

<summary>Bir tablo oluşturmak için kullanımdan kaldırılan yöntem</summary>

!!! attention "Dikkat"
    Bu yöntemi yeni projelerde kullanmayın. Mümkünse, eski projeleri yukarıda açıklanan yönteme geçin.

``` sql
Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format
      [, kafka_row_delimiter, kafka_schema, kafka_num_consumers, kafka_skip_broken_messages])
```

</details>

## Açıklama {#description}

Teslim edilen mesajlar otomatik olarak izlenir, bu nedenle bir gruptaki her mesaj yalnızca bir kez sayılır. Verileri iki kez almak istiyorsanız, tablonun başka bir grup adıyla bir kopyasını oluşturun.

Gruplar esnek ve kümede senkronize edilir. Örneğin, bir kümede 10 konu ve bir tablonun 5 kopyası varsa, her kopya 2 konu alır. Kopya sayısı değişirse, konular kopyalar arasında otomatik olarak yeniden dağıtılır. Bu konuda daha fazla bilgi edinin http://kafka.apache.org/intro.

`SELECT` mesajları okumak için özellikle yararlı değildir (hata ayıklama hariç), çünkü her mesaj yalnızca bir kez okunabilir. Hayata görünümler kullanarak gerçek zamanlı iş parçacıkları oluşturmak daha pratiktir. Bunu yapmak için :

1.  Bir Kafka tüketici oluşturmak için motoru kullanın ve bir veri akışı düşünün.
2.  İstenen yapıya sahip bir tablo oluşturun.
3.  Verileri motordan dönüştüren ve daha önce oluşturulmuş bir tabloya koyan materyalleştirilmiş bir görünüm oluşturun.

Ne zaman `MATERIALIZED VIEW` motora katılır, arka planda veri toplamaya başlar. Bu, kafka'dan sürekli olarak mesaj almanızı ve bunları kullanarak gerekli biçime dönüştürmenizi sağlar `SELECT`.
Bir kafka tablosu istediğiniz kadar materialized görüşe sahip olabilir, kafka tablosundan doğrudan veri okumazlar, ancak yeni kayıtlar (bloklar halinde) alırlar, bu şekilde farklı ayrıntı seviyesine sahip birkaç tabloya yazabilirsiniz (gruplama-toplama ve olmadan).

Örnek:

``` sql
  CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  CREATE TABLE daily (
    day Date,
    level String,
    total UInt64
  ) ENGINE = SummingMergeTree(day, (day, level), 8192);

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() as total
    FROM queue GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

Performansı artırmak için, alınan iletiler bloklar halinde gruplandırılır [max\_ınsert\_block\_size](../../../operations/server-configuration-parameters/settings.md#settings-max_insert_block_size). İçinde blok oluş ifma ifdıysa [stream\_flush\_interval\_ms](../../../operations/server-configuration-parameters/settings.md) milisaniye, veri blok bütünlüğü ne olursa olsun tabloya temizlendi.

Konu verilerini almayı durdurmak veya dönüşüm mantığını değiştirmek için, hayata geçirilmiş görünümü ayırın:

``` sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

Kullanarak hedef tabloyu değiştirmek istiyorsanız `ALTER` hedef tablo ile görünümdeki veriler arasındaki tutarsızlıkları önlemek için malzeme görünümünü devre dışı bırakmanızı öneririz.

## Yapılandırma {#configuration}

GraphiteMergeTree benzer şekilde, Kafka motoru ClickHouse yapılandırma dosyasını kullanarak genişletilmiş yapılandırmayı destekler. Kullanabileceğiniz iki yapılandırma anahtarı vardır: global (`kafka`) ve konu düzeyinde (`kafka_*`). Genel yapılandırma önce uygulanır ve sonra konu düzeyinde yapılandırma uygulanır (varsa).

``` xml
  <!-- Global configuration options for all tables of Kafka engine type -->
  <kafka>
    <debug>cgrp</debug>
    <auto_offset_reset>smallest</auto_offset_reset>
  </kafka>

  <!-- Configuration specific for topic "logs" -->
  <kafka_logs>
    <retry_backoff_ms>250</retry_backoff_ms>
    <fetch_min_bytes>100000</fetch_min_bytes>
  </kafka_logs>
```

Olası yapılandırma seçeneklerinin listesi için bkz. [librdkafka yapılandırma referansı](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md). Alt çizgiyi kullan (`_`) ClickHouse yapılandırmasında bir nokta yerine. Mesela, `check.crcs=true` olacak `<check_crcs>true</check_crcs>`.

## Sanal Sütunlar {#virtual-columns}

-   `_topic` — Kafka topic.
-   `_key` — Key of the message.
-   `_offset` — Offset of the message.
-   `_timestamp` — Timestamp of the message.
-   `_partition` — Partition of Kafka topic.

**Ayrıca Bakınız**

-   [Sanal sütunlar](../index.md#table_engines-virtual_columns)

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/kafka/) <!--hide-->
