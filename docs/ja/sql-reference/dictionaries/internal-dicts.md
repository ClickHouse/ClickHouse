---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 39
toc_title: "\u5185\u90E8\u8F9E\u66F8"
---

# 内部辞書 {#internal_dicts}

ClickHouseには、ジオベースを操作するための組み込み機能が含まれています。

ことができ:

-   地域のidを使用して、目的の言語でその名前を取得します。
-   地域のidを使用して、都市、地域、連邦区、国、または大陸のidを取得します。
-   領域が別の領域の一部であるかどうかを確認します。
-   親領域のチェーンを取得します。

すべての機能サポート “translocality,” 同時に地域の所有権に異なる視点を使用する機能。 詳細については、以下を参照してください “Functions for working with Yandex.Metrica dictionaries”.

内部辞書は、既定のパッケージでは無効になっています。
よって、strncasecmpのパラメータ `path_to_regions_hierarchy_file` と `path_to_regions_names_files` サーバー設定ファイルで。

Geobaseはテキストファイルからロードされます。

場所は `regions_hierarchy*.txt` へのファイル `path_to_regions_hierarchy_file` ディレクトリ。 この構成パラ `regions_hierarchy.txt` ファイル(デフォルトの地域階層)、およびその他のファイル (`regions_hierarchy_ua.txt`)同じディレクトリに配置する必要があります。

を置く `regions_names_*.txt` のファイル `path_to_regions_names_files` ディレクトリ。

を作ることもできますこれらのファイル。 ファイルフォーマットは以下:

`regions_hierarchy*.txt`:TabSeparated(ヘッダーなし)、列:

-   地域ID (`UInt32`)
-   親リージョンID (`UInt32`)
-   地域タイプ (`UInt8`):1-大陸,3-国,4-連邦区,5-地域,6-都市;その他のタイプには値がありません
-   人口 (`UInt32`) — optional column

`regions_names_*.txt`:TabSeparated(ヘッダーなし)、列:

-   地域ID (`UInt32`)
-   地域名 (`String`) — Can’t contain tabs or line feeds, even escaped ones.

フラット配列は、ramに格納するために使用されます。 このため、idは百万を超えてはいけません。

辞書は、サーバーを再起動せずに更新できます。 ただし、使用可能な辞書のセットは更新されません。
更新の場合、ファイルの修正時刻がチェックされます。 ファイルが変更された場合は、辞書が更新されます。
変更をチェックする間隔は、 `builtin_dictionaries_reload_interval` パラメータ。
辞書updates（最初の使用時の読み込み以外）は、クエリをブロックしません。 更新時には、クエリは古いバージョンの辞書を使用します。 更新中にエラーが発生すると、エラーがサーバーログに書き込まれ、古いバージョンの辞書が引き続き使用されます。

Geobaseで辞書を定期的に更新することをお勧めします。 更新中に、新しいファイルを生成し、別の場所に書き込みます。 すべての準備ができたら、サーバーが使用するファイルに名前を変更します。

また、os識別子とyandexを操作するための機能もあります。metricaの調査エンジン、しかしそれらは使用されるべきではない。

[元の記事](https://clickhouse.tech/docs/en/query_language/dicts/internal_dicts/) <!--hide-->
