---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: CatBoost Modellerini Uygulamak
---

# Clickhouse'da bir Catboost modeli uygulamak {#applying-catboost-model-in-clickhouse}

[CatBoost](https://catboost.ai) geliştirilen ücretsiz ve açık kaynak kodlu bir GRA anddi libraryent kütüphan aesidir. [Yandex](https://yandex.com/company/) makine öğrenimi için.

Bu Talimatla, Sql'den model çıkarımı çalıştırarak Clickhouse'da önceden eğitilmiş modelleri uygulamayı öğreneceksiniz.

Clickhouse'da bir CatBoost modeli uygulamak için:

1.  [Tablo oluşturma](#create-table).
2.  [Verileri tabloya ekleme](#insert-data-to-table).
3.  [Catboost'u Clickhouse'a entegre edin](#integrate-catboost-into-clickhouse) (İsteğe bağlı adım).
4.  [SQL'DEN Model Çıkarımını çalıştırın](#run-model-inference).

Eğitim CatBoost modelleri hakkında daha fazla bilgi için bkz [Eğitim ve uygulama modelleri](https://catboost.ai/docs/features/training.html#training).

## Önkoşullar {#prerequisites}

Eğer yoksa [Docker](https://docs.docker.com/install/) yine de yükleyin.

!!! note "Not"
    [Docker](https://www.docker.com) sistemin geri kalanından bir CatBoost ve ClickHouse kurulumunu izole eden kaplar oluşturmanıza olanak sağlayan bir yazılım platformudur.

Bir CatBoost modeli uygulamadan önce:

**1.** P pullull the [Docker görüntü](https://hub.docker.com/r/yandex/tutorial-catboost-clickhouse) kayıt defter theinden:

``` bash
$ docker pull yandex/tutorial-catboost-clickhouse
```

Kod, çalışma zamanı, kütüphaneler, ortam değişkenleri ve Yapılandırma Dosyaları: bu Docker görüntü catboost ve ClickHouse çalıştırmak için gereken her şeyi içerir.

**2.** Docker görüntüsünün başarıyla çekildiğinden emin olun:

``` bash
$ docker image ls
REPOSITORY                            TAG                 IMAGE ID            CREATED             SIZE
yandex/tutorial-catboost-clickhouse   latest              622e4d17945b        22 hours ago        1.37GB
```

**3.** Bu görüntüye dayalı bir Docker kabı başlatın:

``` bash
$ docker run -it -p 8888:8888 yandex/tutorial-catboost-clickhouse
```

## 1. Tablo oluşturma {#create-table}

Eğitim örneği için bir ClickHouse tablosu oluşturmak için:

**1.** Etkileşimli modda ClickHouse konsol istemcisini başlatın:

``` bash
$ clickhouse client
```

!!! note "Not"
    Clickhouse sunucusu Docker kapsayıcısı içinde zaten çalışıyor.

**2.** Komutu kullanarak tablo oluşturun:

``` sql
:) CREATE TABLE amazon_train
(
    date Date MATERIALIZED today(),
    ACTION UInt8,
    RESOURCE UInt32,
    MGR_ID UInt32,
    ROLE_ROLLUP_1 UInt32,
    ROLE_ROLLUP_2 UInt32,
    ROLE_DEPTNAME UInt32,
    ROLE_TITLE UInt32,
    ROLE_FAMILY_DESC UInt32,
    ROLE_FAMILY UInt32,
    ROLE_CODE UInt32
)
ENGINE = MergeTree ORDER BY date
```

**3.** ClickHouse konsol istemcisinden çıkış:

``` sql
:) exit
```

## 2. Verileri tabloya ekleme {#insert-data-to-table}

Verileri eklemek için:

**1.** Aşağıdaki komutu çalıştırın:

``` bash
$ clickhouse client --host 127.0.0.1 --query 'INSERT INTO amazon_train FORMAT CSVWithNames' < ~/amazon/train.csv
```

**2.** Etkileşimli modda ClickHouse konsol istemcisini başlatın:

``` bash
$ clickhouse client
```

**3.** Verilerin yüklendiğinden emin olun:

``` sql
:) SELECT count() FROM amazon_train

SELECT count()
FROM amazon_train

+-count()-+
|   65538 |
+-------+
```

## 3. Catboost'u Clickhouse'a entegre edin {#integrate-catboost-into-clickhouse}

!!! note "Not"
    **İsteğe bağlı adım.** Docker görüntü catboost ve ClickHouse çalıştırmak için gereken her şeyi içerir.

Catboost'u Clickhouse'a entegre etmek için:

**1.** Değerlendirme kitaplığı oluşturun.

Bir CatBoost modelini değerlendirmenin en hızlı yolu derlemedir `libcatboostmodel.<so|dll|dylib>` kitaplık. Kitaplığın nasıl oluşturulacağı hakkında daha fazla bilgi için bkz. [CatBoost belgeleri](https://catboost.ai/docs/concepts/c-plus-plus-api_dynamic-c-pluplus-wrapper.html).

**2.** Herhangi bir yerde ve herhangi bir adla yeni bir dizin oluşturun, örneğin, `data` ve oluşturulan kütüphaneyi içine koyun. Docker görüntüsü zaten kütüphaneyi içeriyor `data/libcatboostmodel.so`.

**3.** Yapılandırma modeli için herhangi bir yerde ve herhangi bir adla yeni bir dizin oluşturun, örneğin, `models`.

**4.** Örneğin, herhangi bir ada sahip bir model yapılandırma dosyası oluşturun, `models/amazon_model.xml`.

**5.** Model yapılandırmasını açıklayın:

``` xml
<models>
    <model>
        <!-- Model type. Now catboost only. -->
        <type>catboost</type>
        <!-- Model name. -->
        <name>amazon</name>
        <!-- Path to trained model. -->
        <path>/home/catboost/tutorial/catboost_model.bin</path>
        <!-- Update interval. -->
        <lifetime>0</lifetime>
    </model>
</models>
```

**6.** Catboost yolunu ve model yapılandırmasını ClickHouse yapılandırmasına ekleyin:

``` xml
<!-- File etc/clickhouse-server/config.d/models_config.xml. -->
<catboost_dynamic_library_path>/home/catboost/data/libcatboostmodel.so</catboost_dynamic_library_path>
<models_config>/home/catboost/models/*_model.xml</models_config>
```

## 4. SQL'DEN Model Çıkarımını çalıştırın {#run-model-inference}

Test modeli için ClickHouse istemcisini çalıştırın `$ clickhouse client`.

Modelin çalıştığından emin olalım:

``` sql
:) SELECT
    modelEvaluate('amazon',
                RESOURCE,
                MGR_ID,
                ROLE_ROLLUP_1,
                ROLE_ROLLUP_2,
                ROLE_DEPTNAME,
                ROLE_TITLE,
                ROLE_FAMILY_DESC,
                ROLE_FAMILY,
                ROLE_CODE) > 0 AS prediction,
    ACTION AS target
FROM amazon_train
LIMIT 10
```

!!! note "Not"
    İşlev [modelEvaluate](../sql-reference/functions/other-functions.md#function-modelevaluate) multiclass modelleri için sınıf başına ham tahminleri ile tuple döndürür.

Olasılığı tahmin edelim:

``` sql
:) SELECT
    modelEvaluate('amazon',
                RESOURCE,
                MGR_ID,
                ROLE_ROLLUP_1,
                ROLE_ROLLUP_2,
                ROLE_DEPTNAME,
                ROLE_TITLE,
                ROLE_FAMILY_DESC,
                ROLE_FAMILY,
                ROLE_CODE) AS prediction,
    1. / (1 + exp(-prediction)) AS probability,
    ACTION AS target
FROM amazon_train
LIMIT 10
```

!!! note "Not"
    Hakkında daha fazla bilgi [exp()](../sql-reference/functions/math-functions.md) İşlev.

Örnek üzerinde LogLoss hesaplayalım:

``` sql
:) SELECT -avg(tg * log(prob) + (1 - tg) * log(1 - prob)) AS logloss
FROM
(
    SELECT
        modelEvaluate('amazon',
                    RESOURCE,
                    MGR_ID,
                    ROLE_ROLLUP_1,
                    ROLE_ROLLUP_2,
                    ROLE_DEPTNAME,
                    ROLE_TITLE,
                    ROLE_FAMILY_DESC,
                    ROLE_FAMILY,
                    ROLE_CODE) AS prediction,
        1. / (1. + exp(-prediction)) AS prob,
        ACTION AS tg
    FROM amazon_train
)
```

!!! note "Not"
    Hakkında daha fazla bilgi [avg()](../sql-reference/aggregate-functions/reference.md#agg_function-avg) ve [günlük()](../sql-reference/functions/math-functions.md) işlevler.

[Orijinal makale](https://clickhouse.tech/docs/en/guides/apply_catboost_model/) <!--hide-->
