export const quickStartsData = [
  {
    id: "connect-your-iceberg-catalog",
    title: "ClickHouse Cloud에서 Iceberg 카탈로그 연결하기",
    description: "ClickHouse Cloud를 데이터 카탈로그에 연결하고 Iceberg 테이블을 쿼리하는 방법을 알아보세요.",
    href: "/get-started/quickstarts/connect-your-iceberg-catalog",
    useCases: ["data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-materialized-view",
    title: "첫 번째 materialized view 만들기",
    description:
      "ClickHouse에서 materialized view를 활용해 다른 정렬 순서로 쿼리 결과를 사전 계산하고 저장하는 방법을 알아보세요. 이를 통해 기본 키(primary key)에 포함되지 않은 컬럼에 대한 빠른 조회가 가능합니다.",
    href: "/get-started/quickstarts/create-your-first-materialized-view",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-mergetree-table",
    title: "첫 번째 MergeTree 테이블 만들기",
    description:
      "MergeTree 테이블을 생성하고 영국 부동산 가격 데이터를 로드하면서, 파트(part)와 머지(merge)가 스토리지 및 쿼리 성능에 미치는 영향을 살펴보며 ClickHouse의 기본 테이블 엔진 동작 방식을 이해해 보세요.",
    href: "/get-started/quickstarts/create-your-first-mergetree-table",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-projection",
    title: "첫 번째 프로젝션 만들기",
    description: "ClickHouse에서 프로젝션을 활용해 동일한 테이블 내에 추가적인 정렬 복사본을 저장하는 방법을 알아보세요. 이를 통해 기본 키(primary key)에 포함되지 않은 컬럼에 대한 빠른 조회가 가능합니다.",
    href: "/get-started/quickstarts/create-your-first-projection",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-service-on-cloud",
    title: "첫 번째 Cloud 서비스 생성 및 예시 데이터 로드",
    description: "ClickHouse Cloud 서비스를 생성하고 SQL 콘솔을 살펴본 후 예시 데이터셋을 로드하여 몇 분 안에 실제 데이터 쿼리를 시작해 보세요.",
    href: "/get-started/quickstarts/create-your-first-service-on-cloud",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "creating-tables",
    title: "ClickHouse에서 테이블 생성하기",
    description: "ClickHouse에서 테이블을 생성하는 방법을 알아보세요.",
    href: "/get-started/quickstarts/creating-tables",
    useCases: ["all"],
    products: ["self-managed"]
  },
  {
    id: "insert-data-using-clickhouse-client",
    title: "clickhouse-client를 사용하여 ClickHouse Cloud에 데이터 삽입하기",
    description: "clickhouse-client를 사용해 로컬 CSV 및 Parquet 파일의 데이터를 커맨드 라인에서 ClickHouse Cloud 서비스에 삽입하는 방법을 알아보세요.",
    href: "/get-started/quickstarts/insert-data-using-clickhouse-client",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "mutations",
    title: "ClickHouse 데이터 업데이트 및 삭제",
    description: "ClickHouse에서 업데이트 및 삭제 작업을 수행하는 방법을 설명합니다.",
    href: "/get-started/quickstarts/mutations",
    useCases: ["data-warehousing"],
    products: ["self-managed"]
  },
  {
    id: "obtain-your-cloud-connection-details",
    title: "Cloud 연결 정보 확인하기",
    description: "외부 클라이언트, CLI, 애플리케이션에서 연결할 수 있도록 ClickHouse Cloud 서비스의 호스트명, 포트, 자격 증명을 확인하는 방법을 알아보세요.",
    href: "/get-started/quickstarts/obtain-your-cloud-connection-details",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "tutorial",
    title: "고급 튜토리얼",
    description: "뉴욕시 택시 예시 데이터셋을 활용하여 ClickHouse에서 데이터를 수집하고 쿼리하는 방법을 알아보세요.",
    href: "/get-started/quickstarts/tutorial",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud", "self-managed"]
  },
  {
    id: "working-with-the-map-type",
    title: "ClickHouse에서 맵(Map) 타입 활용하기",
    description: "OTel 리소스 속성을 실습 예시로 활용하여 ClickHouse에서 맵(Map) 타입으로 동적 키-값 데이터를 저장, 쿼리, 집계하는 방법을 알아보세요.",
    href: "/get-started/quickstarts/working-with-the-map-type",
    useCases: ["observability"],
    products: ["self-managed"]
  },
  {
    id: "writing-queries",
    title: "ClickHouse 데이터 조회하기",
    description: "ClickHouse 데이터를 조회하는 방법을 알아보세요.",
    href: "/get-started/quickstarts/writing-queries",
    useCases: ["all"],
    products: ["self-managed"]
  }
]