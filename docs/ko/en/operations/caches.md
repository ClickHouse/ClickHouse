---
description: '쿼리를 수행할 때 ClickHouse는 다양한 캐시를 사용합니다.'
sidebar_label: '캐시'
sidebar_position: 65
slug: /operations/caches
title: '캐시 유형'
keywords: ['cache']
doc_type: 'reference'
---

쿼리를 수행할 때 ClickHouse는 쿼리 속도를 높이고
디스크 읽기 및 쓰기 필요를 줄이기 위해 다양한 캐시를 사용합니다.

주요 캐시 유형은 다음과 같습니다.

* `mark_cache` — [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md) 계열의 테이블 엔진에서 사용하는 [마크](/ko/development/architecture#merge-tree) 캐시입니다.
* `uncompressed_cache` — [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md) 계열의 테이블 엔진에서 사용하는 비압축 데이터 캐시입니다.
* 운영 체제 페이지 캐시(실제 데이터가 저장된 파일에 간접적으로 사용됨)입니다.

이 밖에도 다양한 캐시 유형이 있습니다.

* DNS 캐시.
* [Regexp](/ko/interfaces/formats/Regexp) 캐시.
* 컴파일된 표현식 캐시.
* [Vector similarity index](../engines/table-engines/mergetree-family/annindexes.md) 캐시.
* [Text index](../engines/table-engines/mergetree-family/textindexes.md#caching) 캐시.
* [Avro format](/ko/interfaces/formats/Avro) 스키마 캐시.
* [Dictionaries](../sql-reference/statements/create/dictionary/overview.md) 데이터 캐시.
* 스키마 추론 캐시.
* S3, Azure, Local 및 기타 디스크용 [파일 시스템 캐시](storing-data.md).
* [사용자 공간 페이지 캐시](/ko/operations/userspace-page-cache)
* [쿼리 캐시](query-cache.md).
* [쿼리 조건 캐시](query-condition-cache.md).
* 포맷 스키마 캐시.

성능 튜닝, 문제 해결 또는 데이터 일관성 등의 이유로 캐시 중 하나를 비우려면
[`SYSTEM CLEAR ... CACHE`](../sql-reference/statements/system.md) SQL 문을 사용할 수 있습니다.