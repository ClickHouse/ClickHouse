---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: clickhouse-benchmark
---

# clickhouse-benchmark {#clickhouse-benchmark}

Bir ClickHouse sunucusuna bağlanır ve art arda belirtilen sorguları gönderir.

Sözdizimi:

``` bash
$ echo "single query" | clickhouse-benchmark [keys]
```

veya

``` bash
$ clickhouse-benchmark [keys] <<< "single query"
```

Bir dizi sorgu göndermek istiyorsanız, Bir metin dosyası oluşturun ve her sorguyu bu dosyadaki tek tek dizeye yerleştirin. Mesela:

``` sql
SELECT * FROM system.numbers LIMIT 10000000
SELECT 1
```

Sonra bu dosyayı standart bir girişe geçirin `clickhouse-benchmark`.

``` bash
clickhouse-benchmark [keys] < queries_file
```

## Anahtarlar {#clickhouse-benchmark-keys}

-   `-c N`, `--concurrency=N` — Number of queries that `clickhouse-benchmark` aynı anda gönderir. Varsayılan değer: 1.
-   `-d N`, `--delay=N` — Interval in seconds between intermediate reports (set 0 to disable reports). Default value: 1.
-   `-h WORD`, `--host=WORD` — Server host. Default value: `localhost`. İçin [karşılaştırma modu](#clickhouse-benchmark-comparison-mode) birden fazla kullanabilirsiniz `-h` anahtarlar.
-   `-p N`, `--port=N` — Server port. Default value: 9000. For the [karşılaştırma modu](#clickhouse-benchmark-comparison-mode) birden fazla kullanabilirsiniz `-p` anahtarlar.
-   `-i N`, `--iterations=N` — Total number of queries. Default value: 0.
-   `-r`, `--randomize` — Random order of queries execution if there is more then one input query.
-   `-s`, `--secure` — Using TLS connection.
-   `-t N`, `--timelimit=N` — Time limit in seconds. `clickhouse-benchmark` belirtilen zaman sınırına ulaşıldığında sorgu göndermeyi durdurur. Varsayılan değer: 0 (zaman sınırı devre dışı).
-   `--confidence=N` — Level of confidence for T-test. Possible values: 0 (80%), 1 (90%), 2 (95%), 3 (98%), 4 (99%), 5 (99.5%). Default value: 5. In the [karşılaştırma modu](#clickhouse-benchmark-comparison-mode) `clickhouse-benchmark` gerçekleştirir [Bağımsız iki örnek öğrencinin t-testi](https://en.wikipedia.org/wiki/Student%27s_t-test#Independent_two-sample_t-test) iki dağıtımın seçilen güven düzeyi ile farklı olup olmadığını belirlemek için sınayın.
-   `--cumulative` — Printing cumulative data instead of data per interval.
-   `--database=DATABASE_NAME` — ClickHouse database name. Default value: `default`.
-   `--json=FILEPATH` — JSON output. When the key is set, `clickhouse-benchmark` belirtilen json dosyasına bir rapor verir.
-   `--user=USERNAME` — ClickHouse user name. Default value: `default`.
-   `--password=PSWD` — ClickHouse user password. Default value: empty string.
-   `--stacktrace` — Stack traces output. When the key is set, `clickhouse-bencmark` çıkışlar özel durumların izlerini yığın.
-   `--stage=WORD` — Query processing stage at server. ClickHouse stops query processing and returns answer to `clickhouse-benchmark` belirtilen aşamada. Olası değerler: `complete`, `fetch_columns`, `with_mergeable_state`. Varsayılan değer: `complete`.
-   `--help` — Shows the help message.

Bazı uygulamak istiyorsanız [ayarlar](../../operations/settings/index.md) sorgular için bunları bir anahtar olarak geçirin `--<session setting name>= SETTING_VALUE`. Mesela, `--max_memory_usage=1048576`.

## Çıktı {#clickhouse-benchmark-output}

Varsayılan olarak, `clickhouse-benchmark` her biri için raporlar `--delay` aralıklı.

Rapor örneği:

``` text
Queries executed: 10.

localhost:9000, queries 10, QPS: 6.772, RPS: 67904487.440, MiB/s: 518.070, result RPS: 67721584.984, result MiB/s: 516.675.

0.000%      0.145 sec.
10.000%     0.146 sec.
20.000%     0.146 sec.
30.000%     0.146 sec.
40.000%     0.147 sec.
50.000%     0.148 sec.
60.000%     0.148 sec.
70.000%     0.148 sec.
80.000%     0.149 sec.
90.000%     0.150 sec.
95.000%     0.150 sec.
99.000%     0.150 sec.
99.900%     0.150 sec.
99.990%     0.150 sec.
```

Raporda bulabilirsiniz:

-   Sorgu sayısı `Queries executed:` alan.

-   İçeren durum dizesi (sırayla):

    -   ClickHouse sunucusunun bitiş noktası.
    -   İşlenen sorgu sayısı.
    -   QPS: qps: kaç sorgu sunucusu saniyede belirtilen bir süre boyunca gerçekleştirilen `--delay` tartışma.
    -   RPS: kaç satır sunucu saniyede belirtilen bir süre boyunca okuma `--delay` tartışma.
    -   MıB / s: kaç mebibytes sunucu saniyede belirtilen bir süre boyunca okuma `--delay` tartışma.
    -   sonuç RPS: sunucu tarafından belirtilen bir süre boyunca saniyede bir sorgunun sonucuna kaç satır yerleştirilir `--delay` tartışma.
    -   sonuç MıB / s. kaç mebibytes sunucu tarafından belirtilen bir dönemde saniyede bir sorgu sonucu yerleştirilir `--delay` tartışma.

-   Sorgu yürütme süresi yüzdelik.

## Karşılaştırma Modu {#clickhouse-benchmark-comparison-mode}

`clickhouse-benchmark` iki çalışan ClickHouse sunucuları için performansları karşılaştırabilirsiniz.

Karşılaştırma modunu kullanmak için, her iki sunucunun bitiş noktalarını iki çift `--host`, `--port` anahtarlar. Anahtarlar argüman listesindeki konuma göre eşleşti, ilk `--host` ilk ile eşleştirilir `--port` ve böyle devam eder. `clickhouse-benchmark` her iki sunucuya da bağlantılar kurar, sonra sorgular gönderir. Her sorgu rastgele seçilen bir sunucuya gönderilir. Sonuçlar her sunucu için ayrı ayrı gösterilir.

## Örnek {#clickhouse-benchmark-example}

``` bash
$ echo "SELECT * FROM system.numbers LIMIT 10000000 OFFSET 10000000" | clickhouse-benchmark -i 10
```

``` text
Loaded 1 queries.

Queries executed: 6.

localhost:9000, queries 6, QPS: 6.153, RPS: 123398340.957, MiB/s: 941.455, result RPS: 61532982.200, result MiB/s: 469.459.

0.000%      0.159 sec.
10.000%     0.159 sec.
20.000%     0.159 sec.
30.000%     0.160 sec.
40.000%     0.160 sec.
50.000%     0.162 sec.
60.000%     0.164 sec.
70.000%     0.165 sec.
80.000%     0.166 sec.
90.000%     0.166 sec.
95.000%     0.167 sec.
99.000%     0.167 sec.
99.900%     0.167 sec.
99.990%     0.167 sec.



Queries executed: 10.

localhost:9000, queries 10, QPS: 6.082, RPS: 121959604.568, MiB/s: 930.478, result RPS: 60815551.642, result MiB/s: 463.986.

0.000%      0.159 sec.
10.000%     0.159 sec.
20.000%     0.160 sec.
30.000%     0.163 sec.
40.000%     0.164 sec.
50.000%     0.165 sec.
60.000%     0.166 sec.
70.000%     0.166 sec.
80.000%     0.167 sec.
90.000%     0.167 sec.
95.000%     0.170 sec.
99.000%     0.172 sec.
99.900%     0.172 sec.
99.990%     0.172 sec.
```
