---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: "Graph\u0131temergetree"
---

# Graphıtemergetree {#graphitemergetree}

Bu motor inceltme ve toplama/ortalama (toplaması) için tasarlanmıştır) [Grafit](http://graphite.readthedocs.io/en/latest/index.html) veriler. Clickhouse'u Grafit için bir veri deposu olarak kullanmak isteyen geliştiriciler için yararlı olabilir.

Toplamaya ihtiyacınız yoksa Grafit verilerini depolamak için herhangi bir ClickHouse tablo motorunu kullanabilirsiniz, ancak bir toplamaya ihtiyacınız varsa `GraphiteMergeTree`. Motor, depolama hacmini azaltır ve grafitten gelen sorguların verimliliğini arttırır.

Motor özellikleri devralır [MergeTree](mergetree.md).

## Tablo oluşturma {#creating-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    Path String,
    Time DateTime,
    Value <Numeric_type>,
    Version <Numeric_type>
    ...
) ENGINE = GraphiteMergeTree(config_section)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Ayrıntılı bir açıklamasını görmek [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) sorgu.

Grafit verileri için bir tablo aşağıdaki veriler için aşağıdaki sütunlara sahip olmalıdır:

-   Metrik adı (Grafit sensörü). Veri türü: `String`.

-   Metrik ölçme zamanı. Veri türü: `DateTime`.

-   Metrik değeri. Veri türü: herhangi bir sayısal.

-   Metrik sürümü. Veri türü: herhangi bir sayısal.

    ClickHouse en yüksek sürümü veya sürümleri aynı ise son yazılan satırları kaydeder. Veri parçalarının birleştirilmesi sırasında diğer satırlar silinir.

Bu sütunların adları toplaması yapılandırmasında ayarlanmalıdır.

**Graphıtemergetree parametreleri**

-   `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

**Sorgu yan tümceleri**

Oluştururken bir `GraphiteMergeTree` tablo, aynı [yanlar](mergetree.md#table_engine-mergetree-creating-a-table) oluşturul ,urken olduğu gibi gerekli `MergeTree` Tablo.

<details markdown="1">

<summary>Bir tablo oluşturmak için kullanımdan kaldırılan yöntem</summary>

!!! attention "Dikkat"
    Bu yöntemi yeni projelerde kullanmayın ve mümkünse eski projeleri yukarıda açıklanan yönteme geçin.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    EventDate Date,
    Path String,
    Time DateTime,
    Value <Numeric_type>,
    Version <Numeric_type>
    ...
) ENGINE [=] GraphiteMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, config_section)
```

Hariç tüm parametreler `config_section` içinde olduğu gibi aynı anlama sahip `MergeTree`.

-   `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

</details>

## Toplaması Yapılandırması {#rollup-configuration}

Toplaması için ayarları tarafından tanımlanan [graphite\_rollup](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-graphite) sunucu yapılandırmasında parametre. Parametrenin adı herhangi biri olabilir. Birkaç yapılandırma oluşturabilir ve bunları farklı tablolar için kullanabilirsiniz.

Toplaması yapılandırma yapısı:

      required-columns
      patterns

### Gerekli Sütunlar {#required-columns}

-   `path_column_name` — The name of the column storing the metric name (Graphite sensor). Default value: `Path`.
-   `time_column_name` — The name of the column storing the time of measuring the metric. Default value: `Time`.
-   `value_column_name` — The name of the column storing the value of the metric at the time set in `time_column_name`. Varsayılan değer: `Value`.
-   `version_column_name` — The name of the column storing the version of the metric. Default value: `Timestamp`.

### Desenler {#patterns}

Bu yapı `patterns` bölme:

``` text
pattern
    regexp
    function
pattern
    regexp
    age + precision
    ...
pattern
    regexp
    function
    age + precision
    ...
pattern
    ...
default
    function
    age + precision
    ...
```

!!! warning "Dikkat"
    Desenler kesinlikle sipariş edilmelidir:

      1. Patterns without `function` or `retention`.
      1. Patterns with both `function` and `retention`.
      1. Pattern `default`.

Bir satır işlerken, ClickHouse kuralları denetler `pattern` bölmeler. Tüm `pattern` (içeren `default`) bölümler içerebilir `function` toplama için parametre, `retention` parametreler veya her ikisi. Metrik adı eşleşirse `regexp` gelen kuralları `pattern` bölüm (veya bölümler) uygulanır; aksi takdirde, kurallar `default` bölüm kullanılır.

Alanlar için `pattern` ve `default` bölmeler:

-   `regexp`– A pattern for the metric name.
-   `age` – The minimum age of the data in seconds.
-   `precision`– How precisely to define the age of the data in seconds. Should be a divisor for 86400 (seconds in a day).
-   `function` – The name of the aggregating function to apply to data whose age falls within the range `[age, age + precision]`.

### Yapılandırma Örneği {#configuration-example}

``` xml
<graphite_rollup>
    <version_column_name>Version</version_column_name>
    <pattern>
        <regexp>click_cost</regexp>
        <function>any</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup>
```

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/graphitemergetree/) <!--hide-->
