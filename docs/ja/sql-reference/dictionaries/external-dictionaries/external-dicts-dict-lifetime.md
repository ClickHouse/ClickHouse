---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 42
toc_title: "\u8F9E\u66F8\u306E\u66F4\u65B0"
---

# 辞書の更新 {#dictionary-updates}

ClickHouseは定期的に辞書を更新します。 完全にダウンロードされたディクショナリの更新間隔とキャッシュされたディクショナ `<lifetime>` 秒の札。

辞書の更新（最初の使用のための読み込み以外）は、クエリをブロックしません。 更新時には、古いバージョンの辞書が使用されます。 更新中にエラーが発生すると、エラーがサーバーログに書き込まれ、古いバージョンの辞書が引き続き使用されます。

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

設定 `<lifetime>0</lifetime>` (`LIFETIME(0)`)辞書が更新されないようにします。

アップグレードの時間間隔を設定することができ、clickhouseはこの範囲内で一様にランダムな時間を選択します。 これは、多数のサーバーでアップグレードするときに、ディクショナリソースに負荷を分散するために必要です。

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

ップする場合には辞書にclickhouseサーバーに適用の異なるロジックの種類によって [ソース](external-dicts-dict-sources.md):

-   テキストファイルの場合は、変更の時間をチェックします。 時間が以前に記録された時間と異なる場合、辞書は更新されます。
-   MyISAMテーブルの場合、変更時刻は次のようにしてチェックされます `SHOW TABLE STATUS` クエリ。
-   他のソースからの辞書は、デフォルトで毎回updatedされます。

MySQL（InnoDB）、ODBC、ClickHouseのソースでは、辞書が実際に変更された場合にのみ、毎回ではなく、辞書を更新するクエリを設定できます。 これを行うには、次の手順に従います:

-   辞書に表れてい分野に常に変化するソースデータを更新しました。
-   ソースの設定では、変更フィールドを取得するクエリを指定する必要があります。 クリックハウスサーバーは、クエリ結果を行として解釈し、この行が以前の状態に対して変更されている場合は、辞書が更新されます。 クエリを指定します。 `<invalidate_query>` の設定のフィールド [ソース](external-dicts-dict-sources.md).

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
