---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: "Donan\u0131m Test"
---

# Donanımınızı ClickHouse ile Test etme {#how-to-test-your-hardware-with-clickhouse}

Bu talimat ile ClickHouse paketlerinin kurulumu olmadan herhangi bir sunucuda temel ClickHouse performans testi çalıştırabilirsiniz.

1.  Gitmek “commits” sayfa: https://github.com/ClickHouse/ClickHouse/commits/master

2.  Yeşil ile ilk yeşil onay işareti veya kırmızı Haç tıklayın “ClickHouse Build Check” ve tıklayın “Details” link yakın “ClickHouse Build Check”. Bazı taahhütlerde böyle bir bağlantı yoktur, örneğin belgelerle taahhüt eder. Bu durumda, bu bağlantıya sahip en yakın taahhüt seçin.

3.  Bağlantıyı kopyala “clickhouse” amd64 veya aarch64 için ikili.

4.  sunucuya ssh ve wget ile indirin:

<!-- -->

      # For amd64:
      wget https://clickhouse-builds.s3.yandex.net/0/00ba767f5d2a929394ea3be193b1f79074a1c4bc/1578163263_binary/clickhouse
      # For aarch64:
      wget https://clickhouse-builds.s3.yandex.net/0/00ba767f5d2a929394ea3be193b1f79074a1c4bc/1578161264_binary/clickhouse
      # Then do:
      chmod a+x clickhouse

1.  İndir yapılandırmaları:

<!-- -->

      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.xml
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/users.xml
      mkdir config.d
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.d/path.xml -O config.d/path.xml
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.d/log_to_console.xml -O config.d/log_to_console.xml

1.  Ben benchmarkch filesmark dosyaları indir:

<!-- -->

      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/benchmark-new.sh
      chmod a+x benchmark-new.sh
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/queries.sql

1.  İndir göre test verileri [Üye.Metrica veri kümesi](../getting-started/example-datasets/metrica.md) talimat (“hits” 100 milyon satır içeren tablo).

<!-- -->

      wget https://clickhouse-datasets.s3.yandex.net/hits/partitions/hits_100m_obfuscated_v1.tar.xz
      tar xvf hits_100m_obfuscated_v1.tar.xz -C .
      mv hits_100m_obfuscated_v1/* .

1.  Sunucuyu Çalıştır:

<!-- -->

      ./clickhouse server

1.  Verileri kontrol edin: başka bir terminaldeki sunucuya ssh

<!-- -->

      ./clickhouse client --query "SELECT count() FROM hits_100m_obfuscated"
      100000000

1.  Edit the benchmark-new.sh, değişim `clickhouse-client` -e doğru `./clickhouse client` ve Ekle `–-max_memory_usage 100000000000` parametre.

<!-- -->

      mcedit benchmark-new.sh

1.  Ben benchmarkch runmark Çalıştır:

<!-- -->

      ./benchmark-new.sh hits_100m_obfuscated

1.  Donanım yapılandırmanız hakkındaki numaraları ve bilgileri şu adrese gönderin clickhouse-feedback@yandex-team.com

Tüm sonuçlar burada yayınlanmaktadır: https://clickhouse.teknoloji / ben benchmarkch /mark / donanım/
