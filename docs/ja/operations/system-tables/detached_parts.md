---
slug: /ja/operations/system-tables/detached_parts
---
# detached_parts

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) テーブルの分離されたパーツに関する情報を含みます。`reason`カラムは、パーツが分離された理由を指定します。

ユーザーによって分離されたパーツの場合、理由は空です。そのようなパーツは、[ALTER TABLE ATTACH PARTITION\|PART](../../sql-reference/statements/alter/partition.md#alter_attach-partition) コマンドでアタッチできます。

他のカラムの説明については、[system.parts](../../operations/system-tables/parts.md#system_tables-parts)を参照してください。

パーツ名が無効な場合、一部のカラムの値は`NULL`になることがあります。そのようなパーツは、[ALTER TABLE DROP DETACHED PART](../../sql-reference/statements/alter/partition.md#alter_drop-detached)で削除できます。
