---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: "\u5185\u90E8\u8F9E\u66F8"
---

# 内部辞書 {#internal_dicts}

ClickHouseには、ジオベースを操作するための組み込み機能が含まれています。

これにより、:

-   地域のIDを使用して、目的の言語でその名前を取得します。
-   地域のIDを使用して、都市、地域、連邦区、国、または大陸のIDを取得します。
-   ある地域が別の地域の一部であるかどうかを確認します。
-   親領域のチェーンを取得します。

すべての機能サポート “translocality,” 地域の所有権に関する異なる視点を同時に使用する能力。 詳細については “Functions for working with Yandex.Metrica dictionaries”.

既定のパッケージでは、内部辞書は無効になっています。
よって、strncasecmpのパラメータ `path_to_regions_hierarchy_file` と `path_to_regions_names_files` サーバー設定ファイル内。

Geobaseはテキストファイルから読み込まれます。

を置く `regions_hierarchy*.txt` にファイル `path_to_regions_hierarchy_file` ディレクトリ。 この構成パラメータには、 `regions_hierarchy.txt` ファイル(既定の地域階層)、およびその他のファイル (`regions_hierarchy_ua.txt`)同じディレクトリにある必要があります。

を置く `regions_names_*.txt` のファイル `path_to_regions_names_files` ディレクトリ。

を作ることもできますこれらのファイル。 ファイル形式は次のとおりです:

`regions_hierarchy*.txt`:TabSeparated(ヘッダーなし),列:

-   地域ID (`UInt32`)
-   親リージョンID (`UInt32`)
-   地域タイプ (`UInt8`):1大陸、3国、4連邦区、5地域、6都市。
-   人口 (`UInt32`) — optional column

`regions_names_*.txt`:TabSeparated(ヘッダーなし),列:

-   地域ID (`UInt32`)
-   地域名 (`String`) — Can't contain tabs or line feeds, even escaped ones.

フラットアレイは、RAMに格納するために使用されます。 このため、Idは百万を超えるべきではありません。

辞書は、サーバーを再起動せずに更新できます。 ただし、使用可能な辞書のセットは更新されません。
更新の場合、ファイルの変更時間がチェックされます。 ファイルが変更された場合は、辞書が更新されます。
変更をチェックする間隔は、 `builtin_dictionaries_reload_interval` パラメータ。
辞書の更新（最初の使用時の読み込み以外）は、クエリをブロックしません。 更新中、クエリは古いバージョンの辞書を使用します。 更新中にエラーが発生すると、エラーはサーバーログに書き込まれ、クエリは古いバージョンの辞書を使用し続けます。

Geobaseで辞書を定期的に更新することをお勧めします。 更新中に、新しいファイルを生成し、別の場所に書き込みます。 すべての準備ができたら、サーバーが使用するファイルに名前を変更します。

OS識別子とYandexを操作するための機能もあります。Metricaの調査エンジン、しかしそれらは使用されるべきではない。

[元の記事](https://clickhouse.tech/docs/en/query_language/dicts/internal_dicts/) <!--hide-->
