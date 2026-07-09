---
description: '向 `Null` 表写入时，数据会被忽略。从 `Null` 表读取时，
  返回结果为空。'
sidebar_label: 'Null'
sidebar_position: 50
slug: /engines/table-engines/special/null
title: 'Null 表引擎'
doc_type: 'reference'
---

向 `Null` 表写入数据时，数据会被忽略。
从 `Null` 表读取时，返回结果为空。

`Null` 表引擎适用于这类数据转换场景：数据转换完成后，你不再需要原始数据。
为此，你可以在 `Null` 表上创建 materialized view。
写入该表的数据会被该视图消费，但原始数据会被丢弃。