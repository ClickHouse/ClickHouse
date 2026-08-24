export const kbIndex = {
  categories: [
    "Cloud",
    "구성 및 설정",
    "데이터 가져오기 및 내보내기",
    "데이터 관리",
    "일반 및 FAQ",
    "통합 및 클라이언트 라이브러리",
    "Materialized view 및 프로젝션",
    "모니터링 및 디버깅",
    "성능 및 최적화",
    "쿼리 및 SQL",
    "보안 및 접근 제어",
    "Setup 및 설치",
    "테이블 및 스키마",
    "문제 해결 및 오류"
  ],
  tags: [
    "모범 사례",
    "커뮤니티",
    "개념",
    "핵심 데이터 개념",
    "데이터 내보내기",
    "데이터 포맷",
    "데이터 수집",
    "데이터 모델링",
    "데이터 소스",
    "배포 및 확장",
    "오류 및 예외",
    "함수",
    "언어 클라이언트",
    "Cloud 관리",
    "데이터 관리",
    "네이티브 클라이언트 및 인터페이스",
    "성능 및 최적화",
    "보안 및 인증",
    "서버 관리",
    "설정",
    "시스템 테이블",
    "도구 및 유틸리티",
    "문제 해결",
    "활용 사례"
  ],
  articles: [
    {
      id: "integrations/python-clickhouse-connect-example",
      title: "ClickHouse Cloud Service 연결을 위한 Python 클라이언트 실습 예시",
      description: "clickhouse-connect 드라이버를 사용하여 Python으로 ClickHouse Cloud Service에 연결하는 방법을 단계별 예시로 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/integrations/python-clickhouse-connect-example",
      category: "통합 및 클라이언트 라이브러리",
      tags: ["언어 클라이언트"]
    },
    {
      id: "configuration-settings/about-quotas-and-query-complexity",
      title: "쿼터 및 쿼리 복잡도 개요",
      description:
        "쿼터(Quota)와 쿼리 복잡도(Query Complexity)는 ClickHouse에서 사용자 작업을 제한하는 강력한 수단입니다. 이 KB 문서에서는 두 가지 접근 방식을 적용하는 예시를 소개합니다.",
      href: "/ko/resources/support-center/knowledge-base/configuration-settings/about-quotas-and-query-complexity",
      category: "구성 및 설정",
      tags: ["Cloud 관리"]
    },
    {
      id: "data-import-export/achieving-atomic-inserts",
      title: "ClickHouse Cloud에서 원자적 삽입 및 다중 테이블 일관성 확보",
      description: "스테이징 테이블과 파티션 수준 작업을 활용하여 멀티 구문 트랜잭션 없이 ClickHouse Cloud에서 데이터를 원자적으로 로드하고 여러 테이블의 일관성을 유지하는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/achieving-atomic-inserts",
      category: "데이터 가져오기 및 내보내기",
      tags: ["데이터 수집", "모범 사례"]
    },
    {
      id: "tables-schema/add-column",
      title: "테이블에 컬럼 추가하기",
      description: "이 가이드에서는 기존 테이블에 컬럼을 추가하는 방법을 알아봅니다.",
      href: "/ko/resources/support-center/knowledge-base/tables-schema/add-column",
      category: "테이블 및 스키마",
      tags: ["데이터 모델링"]
    },
    {
      id: "configuration-settings/alter-user-settings-exception",
      title: "사용자 설정 변경 시 예외 처리",
      description: "사용자 설정 변경 시 발생하는 예외를 처리하는 방법",
      href: "/ko/resources/support-center/knowledge-base/configuration-settings/alter-user-settings-exception",
      category: "구성 및 설정",
      tags: ["설정", "오류 및 예외"]
    },
    {
      id: "materialized-views/are-materialized-views-inserted-asynchronously",
      title: "Materialized View는 동기적으로 삽입되나요?",
      description: "이 KB 문서에서는 Materialized View가 동기적으로 삽입되는지 여부를 살펴봅니다.",
      href: "/ko/resources/support-center/knowledge-base/materialized-views/are-materialized-views-inserted-asynchronously",
      category: "Materialized view 및 프로젝션",
      tags: ["데이터 모델링"]
    },
    {
      id: "tables-schema/schema-migration-tools",
      title: "ClickHouse용 자동 스키마 마이그레이션 도구",
      description: "ClickHouse용 자동 스키마 마이그레이션 도구와 시간이 지남에 따라 변화하는 데이터베이스 스키마를 관리하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/tables-schema/schema-migration-tools",
      category: "테이블 및 스키마",
      tags: ["도구 및 유틸리티"]
    },
    {
      id: "cloud-services/aws-privatelink-setup-for-msk-clickpipes",
      title: "ClickPipes용 MSK 노출을 위한 AWS PrivateLink 설정",
      description: "MSK 멀티-VPC 연결을 통해 프라이빗 MSK를 ClickPipes에 노출하는 설정 단계입니다.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/aws-privatelink-setup-for-msk-clickpipes",
      category: "Cloud",
      tags: ["보안 및 인증", "Cloud 관리"]
    },
    {
      id: "cloud-services/aws-privatelink-setup-for-clickpipes",
      title: "ClickPipes용 프라이빗 RDS 노출을 위한 AWS PrivateLink 설정",
      description: "AWS PrivateLink를 통해 프라이빗 RDS를 ClickPipes에 노출하는 설정 단계입니다.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/aws-privatelink-setup-for-clickpipes",
      category: "Cloud",
      tags: ["보안 및 인증", "Cloud 관리"]
    },
    {
      id: "data-management/backing-up-a-specific-partition",
      title: "특정 파티션 백업하기",
      description: "ClickHouse에서 특정 파티션을 백업하는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/data-management/backing-up-a-specific-partition",
      category: "데이터 관리",
      tags: ["데이터 관리"]
    },
    {
      id: "general-faqs/key-value",
      title: "ClickHouse를 키-값 저장소로 사용할 수 있나요?",
      description: "ClickHouse를 키-값 저장소로 사용할 수 있는지에 대한 자주 묻는 질문에 답변합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/key-value",
      category: "일반 및 FAQ",
      tags: []
    },
    {
      id: "general-faqs/time-series",
      title: "ClickHouse를 시계열 데이터베이스로 사용할 수 있나요?",
      description: "ClickHouse를 시계열 데이터베이스로 사용하는 방법을 설명하는 페이지",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/time-series",
      category: "일반 및 FAQ",
      tags: []
    },
    {
      id: "queries-sql/pivot",
      title: "ClickHouse에서 PIVOT을 사용할 수 있나요?",
      description:
        "ClickHouse에는 PIVOT 절이 없지만, 집계 함수 컴비네이터를 사용하여 유사한 기능을 구현할 수 있습니다. 영국 주택 가격 데이터셋을 활용하여 구현하는 방법을 알아보겠습니다.",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/pivot",
      category: "쿼리 및 SQL",
      tags: ["데이터 모델링", "핵심 데이터 개념"]
    },
    {
      id: "general-faqs/vector-search",
      title: "ClickHouse를 벡터 검색에 사용할 수 있나요?",
      description: "임베딩 저장 및 코사인 유사도와 같은 거리 함수를 활용한 검색 등 ClickHouse를 벡터 검색에 활용하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/vector-search",
      category: "일반 및 FAQ",
      tags: ["활용 사례", "개념"]
    },
    {
      id: "monitoring-debugging/send-logs-level",
      title: "클라이언트에서 쿼리 서버 로그 캡처하기",
      description: "`send_logs_level` 클라이언트 설정을 사용하여 로그 설정이 다른 환경에서도 클라이언트 수준에서 서버 로그를 캡처하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/send-logs-level",
      category: "모니터링 및 디버깅",
      tags: ["서버 관리"]
    },
    {
      id: "configuration-settings/change-the-prompt-in-clickhouse-client",
      title: "clickhouse-client 프롬프트 변경하기",
      description: "이 문서에서는 ClickHouse 클라이언트 및 clickhouse-local 터미널 창의 프롬프트를 :)에서 접두사가 붙은 :)로 변경하는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/configuration-settings/change-the-prompt-in-clickhouse-client",
      category: "구성 및 설정",
      tags: ["설정", "네이티브 클라이언트 및 인터페이스"]
    },
    {
      id: "security/common-rbac-queries",
      title: "자주 사용하는 RBAC 쿼리",
      description: "사용자에게 특정 권한을 부여하는 데 활용할 수 있는 쿼리 모음입니다.",
      href: "/ko/resources/support-center/knowledge-base/security/common-rbac-queries",
      category: "보안 및 접근 제어",
      tags: ["보안 및 인증", "Cloud 관리"]
    },
    {
      id: "queries-sql/comparing-metrics-between-queries",
      title: "데시벨 단위로 쿼리 간 메트릭 비교하기",
      description: "ClickHouse에서 두 쿼리 간의 메트릭을 비교하는 쿼리입니다.",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/comparing-metrics-between-queries",
      category: "쿼리 & SQL",
      tags: ["성능 및 최적화"]
    },
    {
      id: "configuration-settings/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      title: "Docker에서 CAP_IPC_LOCK 및 CAP_SYS_NICE 기능 구성하기",
      description: "컨테이너에서 ClickHouse 실행 시 발생하는 `CAP_IPC_LOCK` 및 `CAP_SYS_NICE` Docker 기능 경고를 해결하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/configuration-settings/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      category: "구성 및 설정",
      tags: ["오류 및 예외"]
    },
    {
      id: "troubleshooting/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      title: "Docker에서 CAP_IPC_LOCK 및 CAP_SYS_NICE 기능 구성하기",
      description: "컨테이너에서 ClickHouse 실행 시 발생하는 `CAP_IPC_LOCK` 및 `CAP_SYS_NICE` Docker 기능 경고를 해결하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/troubleshooting/configure-cap-ipc-lock-and-cap-sys-nice-in-docker",
      category: "문제 해결 및 오류",
      tags: ["오류 및 예외"]
    },
    {
      id: "cloud-services/custom-dns-alias-for-instance",
      title: "리버스 프록시를 설정하여 사용자 지정 DNS 별칭 생성하기",
      description: "리버스 프록시를 사용하여 인스턴스에 사용자 지정 DNS 별칭을 설정하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/custom-dns-alias-for-instance",
      category: "Cloud",
      tags: ["서버 관리", "보안 및 인증"]
    },
    {
      id: "troubleshooting/part-intersects-previous-part",
      title: "DB::Exception: Part XXXXX intersects previous part YYYYY. It is a bug or a result of manual intervention in the ZooKeeper data.",
      description:
        "이 문서에서는 ClickHouse에서 파트 교차와 관련된 DB::Exception 오류를 해결하는 방법을 설명합니다. 이 오류는 경쟁 조건(race condition) 또는 ZooKeeper 데이터에 대한 수동 개입으로 인해 발생하는 경우가 많습니다.",
      href: "/ko/resources/support-center/knowledge-base/troubleshooting/part-intersects-previous-part",
      category: "문제 해결 및 오류",
      tags: ["오류 및 예외", "시스템 테이블"]
    },
    {
      id: "setup-installation/difference-between-official-builds-and-3rd-party",
      title: "공식 ClickHouse 빌드와 서드파티 빌드의 차이점",
      description: "업데이트, 호환성, 보안 고려 사항 등 공식 ClickHouse 빌드와 서드파티 빌드 간의 주요 차이점을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/setup-installation/difference-between-official-builds-and-3rd-party",
      category: "설치 및 셋업",
      tags: ["개념"]
    },
    {
      id: "general-faqs/cost-based",
      title: "ClickHouse에 비용 기반 옵티마이저가 있나요?",
      description: "ClickHouse에는 특정 비용 기반 최적화 메커니즘이 있습니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/cost-based",
      category: "일반 및 FAQ",
      tags: []
    },
    {
      id: "general-faqs/datalake",
      title: "ClickHouse는 데이터 레이크를 지원하나요?",
      description: "ClickHouse는 Iceberg, Delta Lake, Apache Hudi, Apache Paimon, Hive를 포함한 데이터 레이크를 지원합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/datalake",
      category: "일반 및 FAQ",
      tags: []
    },
    {
      id: "general-faqs/distributed-join",
      title: "ClickHouse는 분산 JOIN을 지원하나요?",
      description: "ClickHouse는 분산 JOIN을 지원합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/distributed-join",
      category: "일반 및 FAQ",
      tags: []
    },
    {
      id: "general-faqs/federated",
      title: "ClickHouse는 페더레이션 쿼리를 지원하나요?",
      description: "ClickHouse는 다양한 페더레이션 및 하이브리드 쿼리를 지원합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/federated",
      category: "일반 및 FAQ",
      tags: []
    },
    {
      id: "general-faqs/concurrency",
      title: "ClickHouse는 빈번한 동시 쿼리를 지원하나요?",
      description: "ClickHouse는 높은 QPS와 높은 동시성을 지원합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/concurrency",
      category: "일반 및 FAQ",
      tags: []
    },
    {
      id: "cloud-services/multi-region-replication",
      title: "ClickHouse는 멀티 리전 복제를 지원하나요?",
      description: "이 페이지에서는 ClickHouse의 멀티 리전 복제 지원 여부를 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/multi-region-replication",
      category: "Cloud",
      tags: []
    },
    {
      id: "general-faqs/updates",
      title: "ClickHouse는 실시간 업데이트를 지원하나요?",
      description: "ClickHouse는 경량 실시간 업데이트를 지원합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/updates",
      category: "일반 및 FAQ",
      tags: []
    },
    {
      id: "security/row-column-policy",
      title: "ClickHouse는 행 수준 및 컬럼 수준 보안을 지원하나요?",
      description: "ClickHouse 및 ClickHouse Cloud에서의 행 수준 및 컬럼 수준 접근 제한과 정책을 통한 역할 기반 접근 제어(RBAC) 구현 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/security/row-column-policy",
      category: "보안 및 접근 제어",
      tags: ["보안 및 인증"]
    },
    {
      id: "cloud-services/execute-system-queries-in-cloud",
      title: "ClickHouse Cloud의 모든 노드에서 SYSTEM 구문 실행하기",
      description: "`ON CLUSTER` 및 `clusterAllReplicas`를 사용하여 ClickHouse Cloud 서비스의 모든 노드에서 SYSTEM 구문과 쿼리를 실행하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/execute-system-queries-in-cloud",
      category: "Cloud",
      tags: ["배포 및 확장"]
    },
    {
      id: "troubleshooting/count-parts-by-type",
      title: "와이드 또는 컴팩트 파트의 수와 크기 확인하기",
      description: "이 지식 베이스 문서에서는 파트 유형(와이드 또는 컴팩트)별 파트 수를 확인하는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/troubleshooting/count-parts-by-type",
      category: "문제 해결 및 오류",
      tags: ["문제 해결"]
    },
    {
      id: "troubleshooting/fix-developer-verification-error-in-macos",
      title: "MacOS에서 개발자 확인 오류 해결하기",
      description: "시스템 설정 또는 터미널을 사용하여 ClickHouse 명령 실행 시 발생하는 MacOS 개발자 확인 오류를 해결하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/troubleshooting/fix-developer-verification-error-in-macos",
      category: "문제 해결 및 오류",
      tags: ["오류 및 예외"]
    },
    {
      id: "data-import-export/s3-export-data-year-month-folders",
      title: "S3에서 연도 및 월별로 파티션 쓰기를 수행하려면 어떻게 해야 하나요?",
      description: "사용자 지정 경로 구조를 사용하여 ClickHouse에서 S3 버킷에 연도 및 월별로 파티션된 데이터를 쓰는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/s3-export-data-year-month-folders",
      category: "데이터 가져오기 및 내보내기",
      tags: ["데이터 내보내기", "네이티브 클라이언트 및 인터페이스"]
    },
    {
      id: "data-import-export/kafka-clickhouse-json",
      title: "Kafka에서 새로운 JSON 데이터 타입을 사용하려면 어떻게 해야 하나요?",
      description: "Kafka 테이블 엔진과 JSON 데이터 타입을 사용하여 Apache Kafka의 JSON 메시지를 ClickHouse의 단일 JSON 컬럼에 직접 로드하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/kafka-clickhouse-json",
      category: "데이터 가져오기 및 내보내기",
      tags: ["데이터 포맷", "데이터 수집"]
    },
    {
      id: "cloud-services/change-billing-email",
      title: "ClickHouse Cloud에서 청구 담당자를 변경하려면 어떻게 해야 하나요?",
      description: "ClickHouse Cloud에서 청구 주소를 변경하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/change-billing-email",
      category: "Cloud",
      tags: ["Cloud 관리"]
    },
    {
      id: "general-faqs/how-do-i-contribute-code-to-clickhouse",
      title: "ClickHouse에 코드를 기여하려면 어떻게 해야 하나요?",
      description: "ClickHouse는 GitHub에서 개발 중인 오픈 소스 프로젝트입니다. 관례에 따라 기여 지침은 소스 코드 리포지토리의 루트에 있는 CONTRIBUTING 파일에 명시되어 있습니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/how-do-i-contribute-code-to-clickhouse",
      category: "일반 및 FAQ",
      tags: ["커뮤니티"]
    },
    {
      id: "data-import-export/parquet-to-csv-json",
      title: "Parquet 파일을 CSV 또는 JSON으로 변환하는 방법",
      description: "ClickHouse의 `clickhouse-local` 도구를 사용하여 Parquet 파일을 CSV 또는 JSON 포맷으로 쉽게 변환하는 방법을 알아봅니다.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/parquet-to-csv-json",
      category: "데이터 가져오기 및 내보내기",
      tags: ["데이터 소스", "데이터 포맷"]
    },
    {
      id: "data-import-export/mysql-to-parquet-csv-json",
      title: "ClickHouse를 사용하여 MySQL 데이터를 Parquet, CSV 또는 JSON으로 내보내는 방법",
      description: "`clickhouse-local` 도구를 사용하여 MySQL 데이터를 Parquet, CSV 또는 JSON과 같은 포맷으로 빠르고 효율적으로 내보내는 방법을 알아봅니다.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/mysql-to-parquet-csv-json",
      category: "데이터 가져오기 및 내보내기",
      tags: ["데이터 포맷", "데이터 내보내기"]
    },
    {
      id: "data-import-export/postgresql-to-parquet-csv-json",
      title: "PostgreSQL 데이터를 Parquet, CSV 또는 JSON으로 내보내는 방법",
      description: "다양한 예시와 함께 `clickhouse-local`을 사용하여 PostgreSQL 데이터를 Parquet, CSV 또는 JSON 포맷으로 내보내는 방법을 알아봅니다.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/postgresql-to-parquet-csv-json",
      category: "데이터 가져오기 및 내보내기",
      tags: ["데이터 내보내기", "데이터 포맷"]
    },
    {
      id: "setup-installation/install-clickhouse-windows10",
      title: "Windows 10에 ClickHouse를 설치하는 방법",
      description: "WSL 2를 사용하여 Windows 10에 ClickHouse를 설치하고 테스트하는 방법을 알아봅니다. 설정, 문제 해결 및 테스트 환경 실행이 포함됩니다.",
      href: "/ko/resources/support-center/knowledge-base/setup-installation/install-clickhouse-windows10",
      category: "설정 및 설치",
      tags: ["도구 및 유틸리티"]
    },
    {
      id: "security/remove-default-user",
      title: "기본 사용자를 제거하는 방법",
      description: "ClickHouse 서버를 실행할 때 기본 사용자를 제거하는 방법을 알아봅니다.",
      href: "/ko/resources/support-center/knowledge-base/security/remove-default-user",
      category: "보안 및 접근 제어",
      tags: ["서버 관리"]
    },
    {
      id: "cloud-services/ingest-failures-23-9-release",
      title: "ClickHouse 23.9 릴리스 이후 수집 실패를 해결하는 방법",
      description: "ClickHouse 23.9에서 `async_inserts`를 사용하는 테이블에 도입된 더 엄격한 권한 확인으로 인해 발생하는 수집 실패를 해결하는 방법을 알아봅니다. 오류를 수정하려면 권한을 업데이트하십시오.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/ingest-failures-23-9-release",
      category: "Cloud",
      tags: ["오류 및 예외"]
    },
    {
      id: "performance-optimization/insert-select-settings-tuning",
      title: "INSERT...SELECT 중 TOO MANY PARTS 오류를 해결하는 방법",
      description: "더 큰 블록을 처리하도록 전문가 수준의 설정을 조정하고 파티션 임계값을 늘려 `INSERT...SELECT` 실행 중 발생하는 ClickHouse의 TOO_MANY_PARTS 오류를 해결하십시오.",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/insert-select-settings-tuning",
      category: "성능 및 최적화",
      tags: ["설정", "오류 및 예외"]
    },
    {
      id: "integrations/node-js-example",
      title: "NodeJS에서 @clickhouse/client를 사용하는 방법",
      description: "Node.js 애플리케이션에서 @clickhouse/client를 사용하여 ClickHouse와 상호 작용하고 쿼리를 수행하는 방법을 알아봅니다.",
      href: "/ko/resources/support-center/knowledge-base/integrations/node-js-example",
      category: "통합 및 클라이언트 라이브러리",
      tags: ["언어 클라이언트"]
    },
    {
      id: "monitoring-debugging/view-number-of-active-mutations",
      title: "활성 또는 대기 중인 뮤테이션 수를 확인하는 방법",
      description:
        "특히 `ALTER` 또는 `UPDATE` 작업을 수행할 때 ClickHouse에서 활성 또는 대기 중인 뮤테이션 수를 모니터링하십시오. 뮤테이션을 추적하려면 `system.mutations` 테이블을 사용하십시오.",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/view-number-of-active-mutations",
      category: "모니터링 및 디버깅",
      tags: ["시스템 테이블"]
    },
    {
      id: "data-management/read-consistency",
      title: "ClickHouse에서 데이터 읽기 일관성을 달성하는 방법",
      description: "ClickHouse에서 데이터를 읽을 때 동일한 노드에 연결되어 있든 임의의 노드에 연결되어 있든 데이터 일관성을 보장하는 방법을 알아봅니다.",
      href: "/ko/resources/support-center/knowledge-base/data-management/read-consistency",
      category: "데이터 관리",
      tags: ["성능 및 최적화"]
    },
    {
      id: "setup-installation/llvm-clang-up-to-date",
      title: "Linux에서 LLVM 및 clang을 빌드하는 방법",
      description: "Linux에서 LLVM 및 clang을 빌드하는 명령어입니다.",
      href: "/ko/resources/support-center/knowledge-base/setup-installation/llvm-clang-up-to-date",
      category: "설정 및 설치",
      tags: ["커뮤니티", "도구 및 유틸리티"]
    },
    {
      id: "data-management/calculate-ratio-of-zero-sparse-serialization",
      title: "테이블의 모든 컬럼에서 빈 값 또는 0 값의 비율을 계산하는 방법",
      description: "희소 컬럼 직렬화를 최적화하기 위해 ClickHouse 테이블의 모든 컬럼에서 빈 값 또는 0 값의 비율을 계산하는 방법을 알아봅니다.",
      href: "/ko/resources/support-center/knowledge-base/data-management/calculate-ratio-of-zero-sparse-serialization",
      category: "데이터 관리",
      tags: ["성능 및 최적화"]
    },
    {
      id: "security/check-users-roles",
      title: "역할에 할당된 사용자 및 반대의 경우를 확인하는 방법",
      description: "ClickHouse의 `system.role_grants`를 쿼리하여 역할에 할당된 사용자와 특정 사용자에게 할당된 역할을 찾는 방법을 알아봅니다.",
      href: "/ko/resources/support-center/knowledge-base/security/check-users-roles",
      category: "보안 및 접근 제어",
      tags: ["서버 관리", "시스템 테이블", "Cloud 관리"]
    },
    {
      id: "monitoring-debugging/which-processes-are-currently-running",
      title: "서버에서 현재 실행 중인 코드를 확인하는 방법",
      description:
        "ClickHouse는 각 서버 스레드에서 현재 실행 중인 코드를 검사하기 위한 `system.stack_trace`와 같은 인트로스펙션(introspection) 도구를 제공하여 디버깅 및 성능 모니터링을 돕습니다.",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/which-processes-are-currently-running",
      category: "모니터링 및 디버깅",
      tags: ["서버 관리"]
    },
    {
      id: "cloud-services/how-to-check-my-clickhouse-cloud-sevice-state",
      title: "ClickHouse Cloud 서비스 상태를 확인하는 방법",
      description: "ClickHouse Cloud API를 사용하여 서비스를 활성화하지 않고 서비스가 중지되었는지, 유휴 상태인지 또는 실행 중인지 확인하는 방법을 알아봅니다.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/how-to-check-my-clickhouse-cloud-sevice-state",
      category: "Cloud",
      tags: ["Cloud 관리"]
    },
    {
      id: "configuration-settings/configure-a-user-setting",
      title: "ClickHouse에서 사용자 설정을 구성하는 방법",
      description: "`SET` 및 `ALTER USER` 명령을 사용하여 개별 쿼리, 클라이언트 세션 또는 특정 사용자에 대한 ClickHouse의 설정을 정의하는 방법을 알아봅니다.",
      href: "/ko/resources/support-center/knowledge-base/configuration-settings/configure-a-user-setting",
      category: "구성 및 설정",
      tags: ["설정"]
    },
    {
      id: "materialized-views/projection-example",
      title: "쿼리에서 프로젝션이 사용되는지 확인하는 방법",
      description: "샘플 데이터로 테스트하고 EXPLAIN을 사용하여 ClickHouse 쿼리에서 프로젝션이 사용되는지 확인하는 방법을 알아봅니다.",
      href: "/ko/resources/support-center/knowledge-base/materialized-views/projection-example",
      category: "구체화된 뷰(Materialized View) 및 프로젝션",
      tags: ["데이터 모델링"]
    },
    {
      id: "cloud-services/how-to-connect-to-ch-cloud-using-ssh-keys",
      title: "SSH 키를 사용하여 ClickHouse에 연결하는 방법",
      description: "SSH 키를 사용하여 ClickHouse 및 ClickHouse Cloud에 연결하는 방법",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/how-to-connect-to-ch-cloud-using-ssh-keys",
      category: "Cloud",
      tags: ["Managing Cloud", "Security and Authentication"]
    },
    {
      id: "data-management/dictionary-using-strings",
      title: "문자열 키와 값을 사용하여 ClickHouse 딕셔너리를 생성하는 방법",
      description: "MergeTree 테이블을 소스로 사용하여 문자열 키와 값으로 ClickHouse 딕셔너리를 생성하는 방법을 설명합니다. 설정 및 사용 예시를 포함합니다.",
      href: "/ko/resources/support-center/knowledge-base/data-management/dictionary-using-strings",
      category: "데이터 관리",
      tags: ["Data Modelling"]
    },
    {
      id: "tables-schema/how-to-create-table-to-query-multiple-remote-clusters",
      title: "여러 원격 클러스터를 쿼리할 수 있는 테이블을 생성하는 방법",
      description: "여러 원격 클러스터를 쿼리할 수 있는 테이블을 생성하는 방법",
      href: "/ko/resources/support-center/knowledge-base/tables-schema/how-to-create-table-to-query-multiple-remote-clusters",
      category: "테이블 및 스키마",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "setup-installation/enabling-ssl-with-lets-encrypt",
      title: "단일 ClickHouse 서버에서 Let's Encrypt로 SSL을 활성화하는 방법",
      description: "Let's Encrypt를 사용하여 단일 ClickHouse 서버에 SSL을 설정하는 방법을 설명합니다. 인증서 발급, 구성 및 검증 과정을 포함합니다.",
      href: "/ko/resources/support-center/knowledge-base/setup-installation/enabling-ssl-with-lets-encrypt",
      category: "설정 및 설치",
      tags: ["Security and Authentication"]
    },
    {
      id: "data-import-export/file-export",
      title: "ClickHouse에서 파일로 데이터를 내보내는 방법",
      description: "ClickHouse에서 데이터를 내보내는 다양한 방법을 설명합니다. `INTO OUTFILE`, File 테이블 엔진, 커맨드라인 리디렉션 등을 포함합니다.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/file-export",
      category: "데이터 가져오기 및 내보내기",
      tags: ["Data Export"]
    },
    {
      id: "queries-sql/how-to-filter-a-clickhouse-table-by-an-array-column",
      title: "배열 컬럼으로 ClickHouse 테이블을 필터링하는 방법",
      description: "배열 컬럼으로 ClickHouse 테이블을 필터링하는 방법에 대한 기술 자료 문서입니다.",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/how-to-filter-a-clickhouse-table-by-an-array-column",
      category: "쿼리 및 SQL",
      tags: ["Data Modelling", "Functions"]
    },
    {
      id: "monitoring-debugging/generate-har-file",
      title: "지원을 위한 HAR 파일 생성 방법",
      description: "HAR(HTTP Archive) 파일은 브라우저의 네트워크 활동을 캡처합니다. 페이지 로딩 지연, 요청 실패 또는 기타 네트워크 문제를 진단하는 데 지원팀에 도움이 됩니다.",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/generate-har-file",
      category: "모니터링 및 디버깅",
      tags: ["Tools and Utilities"]
    },
    {
      id: "materialized-views/how-to-display-queries-using-mv",
      title: "ClickHouse에서 Materialized View를 사용하는 쿼리를 식별하는 방법",
      description: "ClickHouse 로그를 쿼리하여 지정된 시간 범위 내에서 Materialized View와 관련된 모든 쿼리를 식별하는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/materialized-views/how-to-display-queries-using-mv",
      category: "Materialized view 및 프로젝션",
      tags: ["System Tables"]
    },
    {
      id: "performance-optimization/find-expensive-queries",
      title: "ClickHouse에서 리소스 소비가 가장 큰 쿼리를 식별하는 방법",
      description: "ClickHouse의 `query_log` 테이블을 사용하여 분산 노드 전반에서 메모리 및 CPU 사용량이 가장 높은 쿼리를 식별하는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/find-expensive-queries",
      category: "성능 및 최적화",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "configuration-settings/ignoring-incorrect-settings",
      title: "ClickHouse에서 잘못된 설정을 무시하는 방법",
      description: "`skip_check_for_incorrect_settings` 옵션을 사용하여 사용자 수준 설정이 잘못 지정된 경우에도 ClickHouse가 정상적으로 시작되도록 하는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/configuration-settings/ignoring-incorrect-settings",
      category: "구성 및 설정",
      tags: ["Settings"]
    },
    {
      id: "data-import-export/json-import",
      title: "ClickHouse에 JSON을 가져오는 방법",
      description: "ClickHouse에 JSON을 가져오는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/json-import",
      category: "데이터 가져오기 및 내보내기",
      tags: []
    },
    {
      id: "setup-installation/how-to-increase-thread-pool-size",
      title: "ClickHouse에서 스레드 수를 늘리는 방법",
      description: "`max_thread_pool_size`, `thread_pool_queue_size`, `max_thread_pool_free_size` 등의 설정을 조정하여 ClickHouse의 글로벌 스레드 풀을 구성하는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/setup-installation/how-to-increase-thread-pool-size",
      category: "설정 및 설치",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "data-import-export/kafka-to-clickhouse-setup",
      title: "Kafka에서 ClickHouse로 데이터를 수집하는 방법",
      description: "Kafka 테이블 엔진, materialized view, MergeTree 테이블을 사용하여 Kafka 토픽에서 ClickHouse로 데이터를 수집하는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/kafka-to-clickhouse-setup",
      category: "데이터 가져오기 및 내보내기",
      tags: ["Data Ingestion"]
    },
    {
      id: "data-import-export/ingest-parquet-files-in-s3",
      title: "S3 버킷에서 Parquet 파일을 수집하는 방법",
      description: "ClickHouse의 S3 테이블 엔진을 사용하여 S3 버킷에서 Parquet 파일을 수집하고 쿼리하는 기본 방법을 설명합니다. 설정, 접근 권한, 데이터 가져오기 예시를 포함합니다.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/ingest-parquet-files-in-s3",
      category: "데이터 가져오기 및 내보내기",
      tags: ["Data Ingestion"]
    },
    {
      id: "queries-sql/how-to-insert-all-rows-from-another-table",
      title: "한 테이블의 모든 행을 다른 테이블에 삽입하는 방법",
      description: "한 테이블의 모든 행을 다른 테이블에 삽입하는 방법에 대한 기술 자료 문서입니다.",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/how-to-insert-all-rows-from-another-table",
      category: "쿼리 및 SQL",
      tags: ["Data Ingestion"]
    },
    {
      id: "performance-optimization/check-query-processing-time-only",
      title: "행을 반환하지 않고 쿼리 처리 시간을 측정하는 방법",
      description: "ClickHouse의 `FORMAT Null` 옵션을 사용하여 클라이언트에 행을 반환하지 않고 쿼리 처리 시간을 측정하는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/check-query-processing-time-only",
      category: "성능 및 최적화",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "monitoring-debugging/outputSendLogsLevelTracesToFile",
      title: "clickhouse-client를 사용하여 로그 수준 트레이스를 파일로 출력하는 방법",
      description: "clickhouse-client를 사용하여 로그 수준 트레이스를 파일로 출력하는 방법",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/outputSendLogsLevelTracesToFile",
      category: "모니터링 및 디버깅",
      tags: ["Data Export"]
    },
    {
      id: "tables-schema/recreate-table-across-terminals",
      title: "서로 다른 터미널에서 소규모 테이블을 빠르게 재생성하는 방법",
      description: "개발 환경에서 복사/붙여넣기를 사용하여 서로 다른 터미널에서 소규모 테이블과 데이터를 빠르게 재생성하는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/tables-schema/recreate-table-across-terminals",
      category: "테이블 및 스키마",
      tags: ["Tools and Utilities"]
    },
    {
      id: "integrations/how-to-set-up-ch-on-docker-odbc-connect-mssql",
      title: "Docker에서 ODBC를 사용하여 Microsoft SQL Server(MSSQL) 데이터베이스에 연결하도록 ClickHouse를 설정하는 방법",
      description: "Docker에서 ODBC를 사용하여 Microsoft SQL Server(MSSQL) 데이터베이스에 연결하도록 ClickHouse를 설정하는 방법",
      href: "/ko/resources/support-center/knowledge-base/integrations/how-to-set-up-ch-on-docker-odbc-connect-mssql",
      category: "통합 및 클라이언트 라이브러리",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "queries-sql/using-array-join-to-extract-and-query-attributes",
      title: "배열 조인을 사용하여 맵 키와 값으로 다양한 속성을 추출하고 쿼리하는 방법",
      description: "맵 키와 값을 사용하여 다양한 속성을 추출하고 쿼리하기 위해 배열 조인을 사용하는 방법을 설명하는 간단한 예시",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/using-array-join-to-extract-and-query-attributes",
      category: "쿼리 & SQL",
      tags: ["Functions"]
    },
    {
      id: "materialized-views/how-to-use-parametrised-views",
      title: "ClickHouse에서 매개변수화된 뷰 사용 방법",
      description: "쿼리 시점 매개변수를 기반으로 동적 데이터 슬라이싱을 위해 ClickHouse에서 매개변수화된 뷰를 생성하고 쿼리하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/materialized-views/how-to-use-parametrised-views",
      category: "Materialized view 및 프로젝션",
      tags: ["Use Cases"]
    },
    {
      id: "tables-schema/exchangeStatementToSwitchTables",
      title: "exchange 명령어로 테이블 전환하는 방법",
      description: "exchange 명령어로 테이블 전환하는 방법",
      href: "/ko/resources/support-center/knowledge-base/tables-schema/exchangeStatementToSwitchTables",
      category: "테이블 & 스키마",
      tags: ["Managing Data"]
    },
    {
      id: "queries-sql/compare-resultsets",
      title: "두 쿼리가 동일한 결과 집합을 반환하는지 검증하는 방법",
      description: "해시 함수와 비교 기법을 사용하여 두 ClickHouse 쿼리가 동일한 결과 집합을 반환하는지 검증하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/compare-resultsets",
      category: "쿼리 & SQL",
      tags: ["Functions"]
    },
    {
      id: "monitoring-debugging/check-query-cache-in-use",
      title: "ClickHouse에서 쿼리 캐시 사용 여부 확인 방법",
      description: "`clickhouse-client` 추적 로그 또는 SQL 명령어를 사용하여 ClickHouse에서 쿼리 캐시가 활용되고 있는지 확인하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/check-query-cache-in-use",
      category: "모니터링 & 디버깅",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "cloud-services/unable-to-access-cloud-service",
      title: "ClickHouse Cloud 서비스에 접근할 수 없는 경우",
      description: "IP 액세스 목록 구성을 포함한 ClickHouse Cloud 서비스 접근 문제 해결",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/unable-to-access-cloud-service",
      category: "Cloud",
      tags: ["오류 및 예외", "Managing Cloud"]
    },
    {
      id: "performance-optimization/finding-expensive-queries-by-memory-usage",
      title: "ClickHouse에서 메모리 사용량 기준으로 고비용 쿼리 식별하기",
      description: "`system.query_log` 테이블을 사용하여 ClickHouse에서 메모리를 가장 많이 사용하는 쿼리를 찾는 방법을 클러스터 및 단독 실행 환경 예시와 함께 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/finding-expensive-queries-by-memory-usage",
      category: "성능 & 최적화",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "data-import-export/importing-and-working-with-json-array-objects",
      title: "ClickHouse에서 JSON 배열 객체 가져오기 및 쿼리하기",
      description: "JSON 함수와 배열 연산을 사용하여 ClickHouse에 JSON 배열 객체를 가져오고 고급 쿼리를 수행하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/importing-and-working-with-json-array-objects",
      category: "데이터 가져오기 & 내보내기",
      tags: ["Data Formats"]
    },
    {
      id: "data-import-export/importing-geojason-with-nested-object-array",
      title: "깊게 중첩된 객체 배열이 포함된 GeoJSON 가져오기",
      description: "깊게 중첩된 객체 배열이 포함된 GeoJSON 가져오기",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/importing-geojason-with-nested-object-array",
      category: "데이터 가져오기 & 내보내기",
      tags: ["Data Formats"]
    },
    {
      id: "performance-optimization/improve-map-performance",
      title: "ClickHouse에서 맵 조회 성능 개선하기",
      description: "특정 키를 독립 컬럼으로 구체화하여 ClickHouse의 맵 컬럼 조회를 최적화하고 쿼리 성능을 향상시키는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/improve-map-performance",
      category: "성능 & 최적화",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "tables-schema/delete-old-data",
      title: "ClickHouse 테이블에서 오래된 레코드를 삭제할 수 있나요?",
      description: "ClickHouse 테이블에서 오래된 레코드를 삭제할 수 있는지에 대한 답변을 제공합니다.",
      href: "/ko/resources/support-center/knowledge-base/tables-schema/delete-old-data",
      category: "테이블 & 스키마",
      tags: []
    },
    {
      id: "general-faqs/separate-storage",
      title: "ClickHouse를 스토리지와 컴퓨팅을 분리하여 배포할 수 있나요?",
      description: "ClickHouse를 스토리지와 컴퓨팅을 분리하여 배포할 수 있는지에 대한 답변을 제공합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/separate-storage",
      category: "일반 & FAQ",
      tags: []
    },
    {
      id: "data-import-export/json-extract-example",
      title: "JSON 추출 예시",
      description: "JSON에서 기본 타입을 추출하는 방법에 대한 간단한 예시",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/json-extract-example",
      category: "데이터 가져오기 & 내보내기",
      tags: ["Data Formats"]
    },
    {
      id: "queries-sql/calculate-pi-using-sql",
      title: "SQL로 파이(π) 계산하기",
      description: "파이 데이(Pi Day)입니다! ClickHouse SQL로 파이(π)를 계산해 보세요.",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/calculate-pi-using-sql",
      category: "쿼리 & SQL",
      tags: ["Use Cases"]
    },
    {
      id: "cloud-services/clickhouse-cloud-api-usage",
      title: "API와 cURL로 ClickHouse Cloud 서비스 관리하기",
      description: "API 엔드포인트와 cURL 명령어를 사용하여 ClickHouse Cloud 서비스를 시작, 중지 및 재개하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/cloud-services/clickhouse-cloud-api-usage",
      category: "Cloud",
      tags: ["Managing Cloud", "도구 및 유틸리티"]
    },
    {
      id: "monitoring-debugging/mapping-of-system-metrics-to-prometheus-metrics",
      title: "system.dashboards의 메트릭과 `system.custom_metrics`의 Prometheus 메트릭 간 매핑",
      description: "system.dashboards의 메트릭과 system.custom_metrics의 Prometheus 메트릭 간 매핑",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/mapping-of-system-metrics-to-prometheus-metrics",
      category: "모니터링 & 디버깅",
      tags: ["System Tables"]
    },
    {
      id: "security/windows-active-directory-to-ch-roles",
      title: "Windows Active Directory 보안 그룹을 ClickHouse 역할에 매핑하기",
      description: "Windows Active Directory 보안 그룹을 ClickHouse 역할에 매핑하는 예시",
      href: "/ko/resources/support-center/knowledge-base/security/windows-active-directory-to-ch-roles",
      category: "보안 & 접근 제어",
      tags: ["도구 및 유틸리티"]
    },
    {
      id: "performance-optimization/memory-limit-exceeded-for-query",
      title: "쿼리 메모리 한도 초과",
      description: "쿼리 메모리 한도 초과 오류 해결",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/memory-limit-exceeded-for-query",
      category: "성능 & 최적화",
      tags: ["오류 및 예외"]
    },
    {
      id: "integrations/ODBC-authentication-failed-error-using-PowerBI-CH-connector",
      title: "Power BI ClickHouse 커넥터 사용 시 ODBC 인증 실패 오류",
      description: "Power BI ClickHouse 커넥터 사용 시 ODBC 인증 실패 오류",
      href: "/ko/resources/support-center/knowledge-base/integrations/ODBC-authentication-failed-error-using-PowerBI-CH-connector",
      category: "통합 & 클라이언트 라이브러리",
      tags: ["네이티브 클라이언트 및 인터페이스", "오류 및 예외"]
    },
    {
      id: "monitoring-debugging/profiling-clickhouse-with-llvm-xray",
      title: "LLVM XRay로 ClickHouse 프로파일링하기",
      description: "LLVM의 XRay 계측 프로파일러를 사용하여 ClickHouse를 프로파일링하고, 추적을 시각화하며, 성능을 분석하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/profiling-clickhouse-with-llvm-xray",
      category: "모니터링 & 디버깅",
      tags: ["Performance and Optimizations", "도구 및 유틸리티"]
    },
    {
      id: "integrations/python-http-requests",
      title: "Python quick example using HTTP requests module",
      description: "An example using Python and requests module to write and read to ClickHouse",
      href: "/ko/resources/support-center/knowledge-base/integrations/python-http-requests",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "configuration-settings/maximum-number-of-tables-and-databases",
      title: "Recommended Maximum Databases, Tables, Partitions, and Parts in ClickHouse",
      description: "Learn the recommended maximum limits for databases, tables, partitions, and parts in a ClickHouse cluster to ensure optimal performance.",
      href: "/ko/resources/support-center/knowledge-base/configuration-settings/maximum-number-of-tables-and-databases",
      category: "Configuration & settings",
      tags: ["Performance and Optimizations", "Deployments and Scaling"]
    },
    {
      id: "data-import-export/cannot-append-data-to-parquet-format",
      title: 'Resolving "Cannot Append Data in Parquet Format" Error in ClickHouse',
      description: 'Are you getting the error "Cannot append data in format Parquet to file" error in ClickHouse? Let\'s take a look at how to resolve it.',
      href: "/ko/resources/support-center/knowledge-base/data-import-export/cannot-append-data-to-parquet-format",
      category: "데이터 가져오기 및 내보내기",
      tags: ["Errors and Exceptions", "Data Formats"]
    },
    {
      id: "troubleshooting/exception-too-many-parts",
      title: 'Resolving "Too Many Parts" Error in ClickHouse',
      description: 'Learn how to address the "Too many parts" error in ClickHouse by optimizing insert rates, configuring MergeTree settings, and managing partitions effectively.',
      href: "/ko/resources/support-center/knowledge-base/troubleshooting/exception-too-many-parts",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "troubleshooting/certificate-verify-failed-error",
      title: "Resolving SSL Certificate Verify Error in ClickHouse",
      description: "Learn how to resolve the SSL Exception CERTIFICATE_VERIFY_FAILED error.",
      href: "/ko/resources/support-center/knowledge-base/troubleshooting/certificate-verify-failed-error",
      category: "Troubleshooting & errors",
      tags: ["Security and Authentication", "Errors and Exceptions"]
    },
    {
      id: "troubleshooting/connection-timeout-remote-remoteSecure",
      title: "Resolving Timeout Errors with `remote` and `remoteSecure` Table Functions",
      description: "Learn how to fix timeout errors when using `remote` or `remoteSecure` table functions in ClickHouse by adjusting the connection timeout settings.",
      href: "/ko/resources/support-center/knowledge-base/troubleshooting/connection-timeout-remote-remoteSecure",
      category: "Troubleshooting & errors",
      tags: ["Errors and Exceptions"]
    },
    {
      id: "tables-schema/search-across-node-for-tables-with-a-wildcard",
      title: "Searching across nodes for tables with a wildcard",
      description: "Learn how to search across nodes for tables with a wildcard.",
      href: "/ko/resources/support-center/knowledge-base/tables-schema/search-across-node-for-tables-with-a-wildcard",
      category: "Tables & schema",
      tags: ["Deployments and Scaling"]
    },
    {
      id: "performance-optimization/query-max-execution-time",
      title: "Setting a limit on query execution time",
      description: "How to enforce limit on max query execution time",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/query-max-execution-time",
      category: "Performance & optimization",
      tags: ["Managing Cloud", "Settings"]
    },
    {
      id: "data-import-export/json-simple-example",
      title: "Simple example flow for extracting JSON data using a landing table with a Materialized View",
      description: "Simple example flow for extracting JSON data using a landing table with a Materialized View",
      href: "/ko/resources/support-center/knowledge-base/data-import-export/json-simple-example",
      category: "데이터 가져오기 및 내보내기",
      tags: ["Data Formats"]
    },
    {
      id: "performance-optimization/async-vs-optimize-read-in-order",
      title: "Synchronous data reading",
      description:
        "The new setting `allow_asynchronous_read_from_io_pool_for_merge_tree` allows the number of reading threads (streams) to be higher than the number of threads in the rest of the query execution pipeline.",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/async-vs-optimize-read-in-order",
      category: "Performance & optimization",
      tags: ["Settings", "Performance and Optimizations"]
    },
    {
      id: "integrations/terraform-example",
      title: "Terraform example on how to use Cloud API",
      description: "This covers an example of how you can use terraform to create/delete clusters using the API",
      href: "/ko/resources/support-center/knowledge-base/integrations/terraform-example",
      category: "Integrations & client libraries",
      tags: ["Native Clients and Interfaces"]
    },
    {
      id: "performance-optimization/tips-tricks-optimizing-basic-data-types-in-clickhouse",
      title: "Tips and tricks on optimizing basic data types in ClickHouse",
      description: "Tips and tricks on optimizing basic data types in ClickHouse",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/tips-tricks-optimizing-basic-data-types-in-clickhouse",
      category: "Performance & optimization",
      tags: ["Performance and Optimizations"]
    },
    {
      id: "queries-sql/useful-queries-for-troubleshooting",
      title: "Useful queries for troubleshooting",
      description: "A collection of handy queries for troubleshooting ClickHouse, including monitoring table sizes, long-running queries, and errors.",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/useful-queries-for-troubleshooting",
      category: "쿼리 및 SQL",
      tags: ["Settings"]
    },
    {
      id: "general-faqs/use-clickhouse-for-log-analytics",
      title: "Using ClickHouse for Log Analytics",
      description: "ClickHouse is popular for logs and metrics analysis because of the real-time analytics capabilities provided. Ready to find out more?",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/use-clickhouse-for-log-analytics",
      category: "General & FAQs",
      tags: ["Use Cases"]
    },
    {
      id: "queries-sql/filtered-aggregates",
      title: "Using Filtered Aggregates in ClickHouse",
      description: "Learn how to use filtered aggregates in ClickHouse with `-If` and `-Distinct` aggregate combinators to simplify query syntax and enhance analytics.",
      href: "/ko/resources/support-center/knowledge-base/queries-sql/filtered-aggregates",
      category: "쿼리 및 SQL",
      tags: ["Functions"]
    },
    {
      id: "general-faqs/dependencies",
      title: "What are the 3rd-party dependencies for running ClickHouse?",
      description: "ClickHouse is self-contained and has no runtime dependencies",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/dependencies",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/dbms-naming",
      title: 'What does "ClickHouse" mean?',
      description: 'Learn about What does "ClickHouse" mean?',
      href: "/ko/resources/support-center/knowledge-base/general-faqs/dbms-naming",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/ne-tormozit",
      title: "What does “не тормозит” mean?",
      description: 'This page explains what "Не тормозит" means',
      href: "/ko/resources/support-center/knowledge-base/general-faqs/ne-tormozit",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "integrations/oracle-odbc",
      title: "What if I have a problem with encodings when using Oracle via ODBC?",
      description: "This page provides guidance on what to do if you have a problem with encodings when using Oracle via ODBC",
      href: "/ko/resources/support-center/knowledge-base/integrations/oracle-odbc",
      category: "Integrations & client libraries",
      tags: []
    },
    {
      id: "general-faqs/columnar-database",
      title: "What is a columnar database?",
      description: "This page describes what a columnar database is",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/columnar-database",
      category: "General & FAQs",
      tags: []
    },
    {
      id: "general-faqs/olap",
      title: "OLAP란 무엇입니까?",
      description: "온라인 분석 처리(Online Analytical Processing)에 대한 설명입니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/olap",
      category: "일반 및 FAQ",
      tags: []
    },
    {
      id: "performance-optimization/optimize-final-vs-final",
      title: "OPTIMIZE FINAL과 FINAL의 차이점은 무엇입니까?",
      description: "OPTIMIZE FINAL과 FINAL의 차이점, 그리고 각각을 언제 사용하고 언제 피해야 하는지 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/optimize-final-vs-final",
      category: "성능 및 최적화",
      tags: ["핵심 데이터 개념"]
    },
    {
      id: "general-faqs/sql",
      title: "ClickHouse는 어떤 SQL 문법을 지원합니까?",
      description: "ClickHouse는 SQL 문법을 100% 지원합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/sql",
      category: "일반 및 FAQ",
      tags: []
    },
    {
      id: "data-management/when-is-ttl-applied",
      title: "TTL 규칙은 언제 적용되며, 이를 제어할 수 있습니까?",
      description:
        "ClickHouse의 TTL 규칙은 최종적으로 적용되며, `merge_with_ttl_timeout` 설정을 사용하여 실행 시점을 제어할 수 있습니다. TTL을 강제로 적용하는 방법과 TTL 실행을 위한 백그라운드 스레드를 관리하는 방법을 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/data-management/when-is-ttl-applied",
      category: "데이터 관리",
      tags: ["핵심 데이터 개념"]
    },
    {
      id: "setup-installation/production",
      title: "프로덕션 환경에서 어떤 ClickHouse 버전을 사용해야 합니까?",
      description: "프로덕션 환경에서 사용할 ClickHouse 버전 선택에 대한 지침을 제공합니다.",
      href: "/ko/resources/support-center/knowledge-base/setup-installation/production",
      category: "설정 및 설치",
      tags: []
    },
    {
      id: "general-faqs/who-is-using-clickhouse",
      title: "ClickHouse를 사용하는 곳은 어디입니까?",
      description: "ClickHouse 사용 사례를 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/who-is-using-clickhouse",
      category: "일반 및 FAQ",
      tags: []
    },
    {
      id: "data-management/dictionaries-consistent-state",
      title: "ClickHouse Cloud의 딕셔너리에서 데이터가 보이지 않는 이유는 무엇입니까?",
      description: "딕셔너리 생성 직후 데이터가 즉시 표시되지 않는 문제가 있습니다.",
      href: "/ko/resources/support-center/knowledge-base/data-management/dictionaries-consistent-state",
      category: "데이터 관리",
      tags: ["Cloud 관리", "데이터 모델링"]
    },
    {
      id: "general-faqs/why-recommend-clickhouse-keeper-over-zookeeper",
      title: "ZooKeeper 대신 ClickHouse Keeper가 권장되는 이유는 무엇입니까?",
      description:
        "ClickHouse Keeper는 디스크 공간 사용량 감소, 빠른 복구, 낮은 메모리 소비 등의 개선 사항을 통해 ClickHouse 클러스터에서 ZooKeeper보다 더 나은 성능을 제공합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/why-recommend-clickhouse-keeper-over-zookeeper",
      category: "일반 및 FAQ",
      tags: ["핵심 데이터 개념"]
    },
    {
      id: "monitoring-debugging/why-default-logging-verbose",
      title: "ClickHouse의 기본 로깅이 왜 이렇게 상세합니까?",
      description: "ClickHouse 개발자들이 기본 로깅 수준을 상세하게 설정한 이유를 알아보세요.",
      href: "/ko/resources/support-center/knowledge-base/monitoring-debugging/why-default-logging-verbose",
      category: "모니터링 및 디버깅",
      tags: ["설정"]
    },
    {
      id: "performance-optimization/why-is-my-primary-key-not-used",
      title: "기본 키가 사용되지 않는 이유는 무엇입니까? 어떻게 확인할 수 있습니까?",
      description: "정렬 시 기본 키가 사용되지 않는 일반적인 원인과 이를 확인하는 방법을 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/performance-optimization/why-is-my-primary-key-not-used",
      category: "성능 및 최적화",
      tags: ["성능 및 최적화"]
    },
    {
      id: "general-faqs/mapreduce",
      title: "MapReduce 같은 것을 사용하지 않는 이유는 무엇입니까?",
      description: "MapReduce 대신 ClickHouse를 사용해야 하는 이유를 설명합니다.",
      href: "/ko/resources/support-center/knowledge-base/general-faqs/mapreduce",
      category: "일반 및 FAQ",
      tags: []
    }
  ]
}