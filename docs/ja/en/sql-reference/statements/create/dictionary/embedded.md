---
description: 'ClickHouse に組み込まれている geobase Dictionary'
sidebar_label: '埋め込み Dictionary'
sidebar_position: 6
slug: /sql-reference/statements/create/dictionary/embedded
title: '埋め込み（geobase）Dictionary'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

ClickHouse には、geobase を扱うための組み込み機能があります。

これにより、次のことが可能です。

* リージョンの ID を使用して、指定した言語でその名前を取得する。
* リージョンの ID を使用して、都市、地域、連邦管区、国、または大陸の ID を取得する。
* あるリージョンが別のリージョンに含まれているかどうかを確認する。
* 親リージョンのチェーンを取得する。

すべての関数は「translocality」、つまりリージョンの帰属について異なる観点を同時に扱える機能をサポートしています。詳細については、「web analytics dictionaries を扱う関数」のセクションを参照してください。

内部 Dictionary はデフォルトパッケージでは無効になっています。
有効にするには、サーバー設定ファイル内の `path_to_regions_hierarchy_file` および `path_to_regions_names_files` のパラメータをアンコメントします。

geobase はテキストファイルから読み込まれます。

`regions_hierarchy*.txt` ファイルを `path_to_regions_hierarchy_file` ディレクトリに配置します。この設定パラメータには `regions_hierarchy.txt` ファイル (デフォルトのリージョン階層) へのパスを指定する必要があり、その他のファイル (`regions_hierarchy_ua.txt`) も同じディレクトリに配置する必要があります。

`regions_names_*.txt` ファイルを `path_to_regions_names_files` ディレクトリに配置します。

これらのファイルは自分で作成することもできます。ファイルのフォーマットは次のとおりです。

`regions_hierarchy*.txt`: TabSeparated (ヘッダーなし) 、カラム:

* region ID (`UInt32`)
* 親 region ID (`UInt32`)
* region type (`UInt8`): 1 - continent、3 - country、4 - federal district、5 - region、6 - city。その他の type には値がありません
* population (`UInt32`) — 任意のカラム

`regions_names_*.txt`: TabSeparated (ヘッダーなし) 、カラム:

* region ID (`UInt32`)
* region name (`String`) — エスケープされたものも含め、タブや line feed を含めることはできません。

RAM 内での保存にはフラットな配列が使用されます。このため、ID は 100 万を超えないようにする必要があります。

server を再起動せずに Dictionary を更新できます。ただし、利用可能な Dictionary のセットは更新されません。
更新時には、ファイルの最終更新時刻が確認されます。ファイルに変更があれば、Dictionary が更新されます。
変更を確認する間隔は、`builtin_dictionaries_reload_interval` パラメータで設定します。
Dictionary の更新 (初回使用時の読み込みを除く) では queries はブロックされません。更新中、queries は古いバージョンの Dictionary を使用します。更新中に error が発生した場合、その error は server log に書き込まれ、queries は引き続き古いバージョンの Dictionary を使用します。

geobase を使って定期的に Dictionary を更新することを推奨します。更新時には、新しいファイルを生成して別の場所に書き込みます。すべての準備が整ったら、それらを server が使用するファイルにリネームします。

OS 識別子や search engines を扱う関数もありますが、使用すべきではありません。