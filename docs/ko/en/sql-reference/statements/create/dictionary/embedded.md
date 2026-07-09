---
description: 'ClickHouse의 내장 지오베이스 딕셔너리'
sidebar_label: '내장 딕셔너리'
sidebar_position: 6
slug: /sql-reference/statements/create/dictionary/embedded
title: '내장(지오베이스) 딕셔너리'
doc_type: '참고'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

ClickHouse에는 지오베이스를 다루기 위한 내장 기능이 있습니다.

이를 통해 다음 작업을 수행할 수 있습니다:

* 지역 ID를 사용해 원하는 언어로 지역 이름을 가져옵니다.
* 지역 ID를 사용해 도시, 구역, 연방 지구, 국가 또는 대륙의 ID를 가져옵니다.
* 한 지역이 다른 지역에 속하는지 확인합니다.
* 상위 지역 체인을 가져옵니다.

모든 함수는 지역 소속에 대한 서로 다른 관점을 동시에 사용할 수 있는 &quot;translocality&quot;를 지원합니다. 자세한 내용은 &quot;웹 분석 딕셔너리 작업용 함수&quot; 섹션을 참조하십시오.

내부 딕셔너리는 기본 패키지에서는 비활성화되어 있습니다.
활성화하려면 서버 구성 파일에서 `path_to_regions_hierarchy_file` 및 `path_to_regions_names_files` 매개변수의 주석 처리를 해제하십시오.

지오베이스는 텍스트 파일에서 로드됩니다.

`regions_hierarchy*.txt` 파일은 `path_to_regions_hierarchy_file` 디렉터리에 두십시오. 이 구성 매개변수에는 `regions_hierarchy.txt` 파일(기본 지역 계층 구조)의 경로가 포함되어야 하며, 다른 파일(`regions_hierarchy_ua.txt`)도 같은 디렉터리에 있어야 합니다.

`regions_names_*.txt` 파일은 `path_to_regions_names_files` 디렉터리에 두십시오.

이 파일들은 직접 생성할 수도 있습니다. 파일 포맷은 다음과 같습니다:

`regions_hierarchy*.txt`: TabSeparated (헤더 없음), 컬럼:

* 지역 ID (`UInt32`)
* 상위 지역 ID (`UInt32`)
* 지역 유형 (`UInt8`): 1 - 대륙, 3 - 국가, 4 - 연방 지구, 5 - 지역, 6 - 도시; 그 외 유형에는 값이 없습니다
* 인구 (`UInt32`) — 선택적 컬럼

`regions_names_*.txt`: TabSeparated (헤더 없음), 컬럼:

* 지역 ID (`UInt32`)
* 지역 이름 (`String`) — 이스케이프된 경우를 포함해 탭이나 줄 바꿈(line feed) 문자를 포함할 수 없습니다.

RAM에 저장할 때는 평면 배열을 사용합니다. 따라서 ID는 100만을 넘지 않아야 합니다.

딕셔너리는 서버를 재시작하지 않고도 업데이트할 수 있습니다. 하지만 사용 가능한 딕셔너리 집합은 갱신되지 않습니다.
업데이트 시에는 파일 수정 시간을 확인합니다. 파일이 변경되면 딕셔너리가 업데이트됩니다.
변경 사항 확인 주기는 `builtin_dictionaries_reload_interval` 매개변수에서 구성합니다.
딕셔너리 업데이트(처음 사용할 때 로드하는 경우 제외)는 쿼리를 차단하지 않습니다. 업데이트 중에는 쿼리가 이전 버전의 딕셔너리를 사용합니다. 업데이트 중 오류가 발생하면 해당 오류가 서버 로그에 기록되며, 쿼리는 계속 이전 버전의 딕셔너리를 사용합니다.

지오베이스에 맞춰 딕셔너리를 주기적으로 업데이트하는 것을 권장합니다. 업데이트 시에는 새 파일을 생성해 별도의 위치에 기록하십시오. 모든 준비가 완료되면 서버에서 사용하는 파일 이름으로 바꾸십시오.

OS 식별자 및 검색 엔진을 다루는 함수도 있지만, 사용하지 않아야 합니다.