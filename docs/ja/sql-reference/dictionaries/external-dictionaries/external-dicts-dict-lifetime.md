---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 42
toc_title: "\u8F9E\u66F8\u306E\u66F4\u65B0"
---

# 辞書の更新 {#dictionary-updates}

ClickHouseは定期的に辞書を更新します。 完全にダウンロードされた辞書の更新間隔と、キャッシュされた辞書の無効化間隔は、 `<lifetime>` 秒単位のタグ。

辞書の更新(最初の使用のための読み込み以外)は、クエリをブロックしません。 更新時には、古いバージョンの辞書が使用されます。 更新中にエラーが発生すると、エラーはサーバーログに書き込まれ、クエリは古いバージョンの辞書を使用し続けます。

設定例:

``` xml
<dictionary>
    ...
    <lifetime>300</lifetime>
    ...
</dictionary>
```

``` sql
CREATE DICTIONARY (...)
...
LIFETIME(300)
...
```

設定 `<lifetime>0</lifetime>` (`LIFETIME(0)`)辞書の更新を防ぎます。

しかしながら、セッションの時間間隔のアップグレード、ClickHouseを選定しランダム均一に時間以内であることが判明した。 これは、多数のサーバーでアップグレードするときに辞書ソースの負荷を分散するために必要です。

設定例:

``` xml
<dictionary>
    ...
    <lifetime>
        <min>300</min>
        <max>360</max>
    </lifetime>
    ...
</dictionary>
```

または

``` sql
LIFETIME(MIN 300 MAX 360)
```

もし `<min>0</min>` と `<max>0</max>`,ClickHouseはタイムアウトによって辞書をリロードしません。
この場合、clickhouseは辞書設定ファイルが変更された場合、または `SYSTEM RELOAD DICTIONARY` コマンドが実行された。

ップする場合には辞書にClickHouseサーバーに適用の異なるロジックの種類によって [ソース](external-dicts-dict-sources.md):

ップする場合には辞書にClickHouseサーバーに適用の異なるロジックの種類によって [ソース](external-dicts-dict-sources.md):

-   テキストファイルの場合、変更の時刻をチェックします。 時刻が以前に記録された時刻と異なる場合、辞書が更新されます。
-   MyISAMテーブルの場合、変更時刻は `SHOW TABLE STATUS` クエリ。
-   他のソースからの辞書は、デフォルトで毎回updatedされます。

MySQL(InnoDB)、ODBC、およびClickHouseソースでは、毎回ではなく、実際に変更された場合にのみ辞書を更新するクエリを設定できます。 これを行うには、次の手順に従います:

-   辞書テーブルには、ソースデータの更新時に常に変更されるフィールドが必要です。
-   ソースの設定では、変更フィールドを取得するクエリを指定する必要があります。 ClickHouseサーバーは、クエリ結果を行として解釈し、この行が以前の状態に対して相対的に変更された場合、辞書が更新されます。 のクエリを指定します。 `<invalidate_query>` の設定のフィールド [ソース](external-dicts-dict-sources.md).

設定例:

``` xml
<dictionary>
    ...
    <odbc>
      ...
      <invalidate_query>SELECT update_time FROM dictionary_source where id = 1</invalidate_query>
    </odbc>
    ...
</dictionary>
```

または

``` sql
...
SOURCE(ODBC(... invalidate_query 'SELECT update_time FROM dictionary_source where id = 1'))
...
```

[元の記事](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_lifetime/) <!--hide-->
