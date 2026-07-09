---
description: 'ClickHouseのデータ型に関するドキュメント'
sidebar_label: 'データ型の一覧'
sidebar_position: 1
slug: /sql-reference/data-types/
title: 'ClickHouseのデータ型'
doc_type: 'reference'
---

このセクションでは、ClickHouseでサポートされているデータ型について説明します。たとえば、[整数型](int-uint.md)、[浮動小数点型](float.md)、[文字列型](string.md) などがあります。

システムテーブル [system.data&#95;type&#95;families](/ja/operations/system-tables/data_type_families) では、
利用可能なすべてのデータ型の
概要を確認できます。
また、あるデータ型が別のデータ型のエイリアスであるかどうかや、その名前で大文字と小文字が区別されるかどうか (たとえば `bool` と `BOOL`) も示されます。