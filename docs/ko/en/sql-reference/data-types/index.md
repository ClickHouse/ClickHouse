---
description: 'ClickHouse의 데이터 타입 문서'
sidebar_label: '데이터 타입 목록'
sidebar_position: 1
slug: /sql-reference/data-types/
title: 'ClickHouse의 데이터 타입'
doc_type: 'reference'
---

이 섹션에서는 ClickHouse에서 지원하는 데이터 타입을 설명합니다. 예를 들어 [integers](int-uint.md), [floats](float.md), [strings](string.md)가 있습니다.

시스템 테이블(system table) [system.data&#95;type&#95;families](/ko/operations/system-tables/data_type_families)는
사용 가능한 모든 데이터 타입의 개요를 제공합니다.
또한 각 데이터 타입이 다른 데이터 타입의 별칭인지, 그리고 이름이 대소문자를 구분하는지도 보여줍니다(예: `bool`과 `BOOL`).