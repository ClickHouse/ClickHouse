---
description: 'Документация по типам данных в ClickHouse'
sidebar_label: 'Список типов данных'
sidebar_position: 1
slug: /sql-reference/data-types/
title: 'Типы данных в ClickHouse'
doc_type: 'reference'
---

В этом разделе описаны типы данных, поддерживаемые ClickHouse, например [целые числа](int-uint.md), [числа с плавающей запятой](float.md) и [строки](string.md).

Системная таблица [system.data&#95;type&#95;families](/ru/operations/system-tables/data_type_families) содержит
обзор всех доступных типов данных.
Она также показывает, является ли тип данных алиасом другого типа данных и зависит ли его имя от регистра (например, `bool` и `BOOL`).