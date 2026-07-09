---
description: 'ClickHouse 中 String 类型的文档'
sidebar_label: 'String'
sidebar_position: 8
slug: /sql-reference/data-types/string
title: 'String'
doc_type: 'reference'
---

任意长度的字符串，长度不受限制。其值可以包含任意字节序列，包括空字节。
String 类型可替代其他 DBMS 中的 VARCHAR、BLOB、CLOB 等类型。

创建表时，可以为字符串字段设置数值参数 (例如 `VARCHAR(255)`) ，但 ClickHouse 会忽略这些参数。

别名：

* `String` — `LONGTEXT`, `MEDIUMTEXT`, `TINYTEXT`, `TEXT`, `LONGBLOB`, `MEDIUMBLOB`, `TINYBLOB`, `BLOB`, `VARCHAR`, `CHAR`, `CHAR LARGE OBJECT`, `CHAR VARYING`, `CHARACTER LARGE OBJECT`, `CHARACTER VARYING`, `NCHAR LARGE OBJECT`, `NCHAR VARYING`, `NATIONAL CHARACTER LARGE OBJECT`, `NATIONAL CHARACTER VARYING`, `NATIONAL CHAR VARYING`, `NATIONAL CHARACTER`, `NATIONAL CHAR`, `BINARY LARGE OBJECT`, `BINARY VARYING`,

<div id="encodings">
  ## 编码
</div>

ClickHouse 没有“编码”这一概念。String 可以包含任意字节序列，并按原样存储和输出。
如果你需要存储文本，我们建议使用 UTF-8 编码。至少在你的终端使用 UTF-8 (这是推荐做法) 时，你可以直接读取和写入值，而无需进行转换。
同样，某些用于处理字符串的函数也提供了单独的变体，这些变体会假定字符串包含表示 UTF-8 编码文本的字节序列。
例如，[length](/zh/sql-reference/functions/array-functions#length) 函数按字节计算字符串长度，而 [lengthUTF8](../functions/string-functions.md#lengthUTF8) 函数则在假定值采用 UTF-8 编码的前提下，按 Unicode 码点计算字符串长度。