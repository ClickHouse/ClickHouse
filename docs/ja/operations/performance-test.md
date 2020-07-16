---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: "\u30CF\u30FC\u30C9\u30A6\u30A7\u30A2\u8A66\u9A13"
---

# ClickHouseでハードウェアをテストする方法 {#how-to-test-your-hardware-with-clickhouse}

この命令を実行できますが基本的なClickHouse性能試験はサーバーなしでの設置ClickHouseパッケージ。

1.  に行く “commits” ページ:https://github.com/ClickHouse/ClickHouse/commits/master

2.  最初の緑色のチェックマークまたは緑色の赤十字をクリックします “ClickHouse Build Check” をクリックして “Details” 近くのリンク “ClickHouse Build Check”. いくつかのコミットにはそのようなリンクはありません。 この場合、このリンクを持つ最も近いコミットを選択します。

3.  リンクをコピーする “clickhouse” amd64またはaarch64のバイナリ。

4.  サーバーにsshし、wgetでダウンロードします:

<!-- -->

      # For amd64:
      wget https://clickhouse-builds.s3.yandex.net/0/00ba767f5d2a929394ea3be193b1f79074a1c4bc/1578163263_binary/clickhouse
      # For aarch64:
      wget https://clickhouse-builds.s3.yandex.net/0/00ba767f5d2a929394ea3be193b1f79074a1c4bc/1578161264_binary/clickhouse
      # Then do:
      chmod a+x clickhouse

1.  ダウンロードconfigs:

<!-- -->

      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.xml
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/users.xml
      mkdir config.d
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.d/path.xml -O config.d/path.xml
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.d/log_to_console.xml -O config.d/log_to_console.xml

1.  ダウンロードファイルのベンチマーク:

<!-- -->

      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/benchmark-new.sh
      chmod a+x benchmark-new.sh
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/queries.sql

1.  に従うダウンロードテストデータ [Yandex.メトリカデータセット](../getting-started/example-datasets/metrica.md) 命令 (“hits” 100万行を含むテーブル）。

<!-- -->

      wget https://clickhouse-datasets.s3.yandex.net/hits/partitions/hits_100m_obfuscated_v1.tar.xz
      tar xvf hits_100m_obfuscated_v1.tar.xz -C .
      mv hits_100m_obfuscated_v1/* .

1.  サーバーの実行:

<!-- -->

      ./clickhouse server

1.  データを確認する：別の端末のサーバーへのssh

<!-- -->

      ./clickhouse client --query "SELECT count() FROM hits_100m_obfuscated"
      100000000

1.  編集benchmark-new.sh,変更 `clickhouse-client` に `./clickhouse client` と追加 `–-max_memory_usage 100000000000` パラメータ。

<!-- -->

      mcedit benchmark-new.sh

1.  ベンチマークの実行:

<!-- -->

      ./benchmark-new.sh hits_100m_obfuscated

1.  ハードウェア構成に関する番号と情報を次の宛先に送信しますclickhouse-feedback@yandex-team.com

すべての結果をこちらに発表します:https://clickhouse.技術/基準/ハードウェア/
