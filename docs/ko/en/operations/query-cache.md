---
description: 'ClickHouse에서 쿼리 캐시 기능을 사용하고 구성하는 방법에 대한 가이드'
sidebar_label: '쿼리 캐시'
sidebar_position: 65
slug: /operations/query-cache
title: '쿼리 캐시'
doc_type: 'guide'
---

쿼리 캐시를 사용하면 `SELECT` 쿼리를 한 번만 실행하고, 이후 동일한 쿼리가 다시 실행되면 캐시에서 직접 결과를 반환할 수 있습니다.
쿼리 유형에 따라 ClickHouse 서버의 지연 시간과 리소스 사용량을 크게 줄일 수 있습니다.

<div id="background-design-and-limitations">
  ## 배경, 설계 및 제한 사항
</div>

쿼리 캐시는 일반적으로 트랜잭션 일관성이 있는 경우와 없는 경우로 나눌 수 있습니다.

* 트랜잭션 일관성이 있는 캐시에서는 `SELECT` 쿼리 결과가 변경되었거나
  변경되었을 가능성이 있으면 데이터베이스가 캐시된 쿼리 결과를 무효화(폐기)합니다. ClickHouse에서 데이터를 변경하는 작업에는 테이블에 대한 삽입/업데이트/삭제나 축약
  머지가 포함됩니다. 트랜잭션 일관성 캐싱은 특히 OLTP 데이터베이스에 적합하며, 대표적인 예로
  [MySQL](https://dev.mysql.com/doc/refman/5.6/en/query-cache.html) (v8.0 이후 쿼리 캐시 제거)과
  [Oracle](https://docs.oracle.com/database/121/TGDBA/tune_result_cache.htm)이 있습니다.
* 트랜잭션 일관성이 없는 캐시에서는 모든 엔트리에
  유효 기간이 할당되고 해당 기간이 지나면 만료되며(예: 1분), 그 기간 동안 기반 데이터가 거의 변하지 않는다는 가정하에 쿼리 결과에 약간의 부정확성이 있는 것을 허용합니다.
  이러한 방식은 전반적으로 OLAP 데이터베이스에 더 적합합니다. 트랜잭션 일관성이 없는 캐싱으로도 충분한 예시로는,
  여러 사용자가 동시에 접근하는 리포팅 도구의 시간별 매출 보고서를 들 수 있습니다. 일반적으로 매출 데이터는
  변화 속도가 충분히 느리므로 데이터베이스는 보고서를 한 번만 컴퓨트하면 됩니다(첫 번째 `SELECT` 쿼리). 이후 쿼리는
  쿼리 캐시에서 직접 제공할 수 있습니다. 이 예시에서 합리적인 유효 기간은 30분입니다.

트랜잭션 일관성이 없는 캐싱은 전통적으로 데이터베이스와 상호작용하는 클라이언트 도구나 프록시 패키지(예:
[chproxy](https://www.chproxy.org/configuration/caching/))에서 제공되었습니다. 그 결과, 동일한 캐싱 로직과
구성이 중복되는 경우가 많았습니다. ClickHouse의 쿼리 캐시를 사용하면 캐싱 로직이 서버 측으로 이동합니다. 이는 유지 관리
부담을 줄이고 중복을 방지합니다.

<div id="configuration-settings-and-usage">
  ## 구성 설정 및 사용
</div>

:::note
ClickHouse Cloud에서는 쿼리 캐시 설정을 수정하려면 [쿼리 수준 설정](/ko/operations/settings/query-level)을 사용해야 합니다. [구성 수준 설정](/ko/operations/configuration-files) 수정은 현재 지원되지 않습니다.
:::

:::note
[clickhouse-local](utilities/clickhouse-local.md)은 한 번에 쿼리 하나만 실행합니다. 쿼리 결과를 캐싱해도 의미가 없으므로 clickhouse-local에서는 쿼리
결과 캐시가 비활성화되어 있습니다.
:::

[use&#95;query&#95;cache](/ko/operations/settings/settings#use_query_cache) 설정을 사용하면 특정 쿼리 또는 현재 세션의 모든 쿼리에서
쿼리 캐시를 사용할지 제어할 수 있습니다. 예를 들어, 쿼리를 처음 실행할 때

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true;
```

쿼리 결과를 쿼리 캐시에 저장합니다. 이후 동일한 쿼리를 다시 실행하면(이 경우에도 매개변수 `use_query_cache = true` 사용) 계산된 결과를 캐시에서
읽어 즉시 반환합니다.

:::note
설정 `use_query_cache`와 그 밖의 모든 쿼리 캐시 관련 설정은 독립 실행형 `SELECT` SQL 문에만 적용됩니다. 특히,
`CREATE VIEW AS SELECT [...] SETTINGS use_query_cache = true`로 생성된 뷰에 대한 `SELECT`
결과는 해당 `SELECT` SQL 문을 `SETTINGS use_query_cache = true`와 함께 실행하지 않는 한 캐시되지 않습니다.
:::

캐시 활용 방식은 설정 [enable&#95;writes&#95;to&#95;query&#95;cache](/ko/operations/settings/settings#enable_writes_to_query_cache)
및 [enable&#95;reads&#95;from&#95;query&#95;cache](/ko/operations/settings/settings#enable_reads_from_query_cache) (둘 다 기본값은 `true`)을 사용해 더 세부적으로 구성할 수 있습니다. 전자의 설정은
쿼리 결과를 캐시에 저장할지 제어하고, 후자의 설정은 데이터베이스가 캐시에서 쿼리 결과를 가져오려고 시도할지 결정합니다. 예를 들어, 다음 쿼리는 캐시를
수동적으로만 사용합니다. 즉, 캐시에서 읽기만 시도하고 그 결과를 캐시에 저장하지는 않습니다:

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, enable_writes_to_query_cache = false;
```

최대한 세밀하게 제어하려면 일반적으로 `use_query_cache`, `enable_writes_to_query_cache`,
`enable_reads_from_query_cache` 설정은 특정 쿼리에만 지정하는 것이 좋습니다. 사용자 수준이나 프로필 수준(예: `SET
use_query_cache = true`)에서 캐싱을 활성화할 수도 있지만, 이 경우 모든 `SELECT` 쿼리가 캐시된 결과를 반환할 수
있다는 점을 유의해야 합니다.

쿼리 캐시는 `SYSTEM CLEAR QUERY CACHE` 문으로 비울 수 있습니다. 쿼리 캐시의 내용은 시스템 테이블
[system.query&#95;cache](system-tables/query_cache.md)에 표시됩니다. 데이터베이스 시작 이후의 쿼리 캐시 적중 수와 미적중 수는 시스템 테이블
[system.events](system-tables/events.md)의 이벤트 &quot;QueryCacheHits&quot; 및 &quot;QueryCacheMisses&quot;로 확인할 수 있습니다. 두 카운터는
`use_query_cache = true` 설정으로 실행되는 `SELECT` 쿼리에 대해서만 갱신되며, 다른 쿼리는 &quot;QueryCacheMisses&quot;에 영향을 주지 않습니다. 시스템 테이블
[system.query&#95;log](system-tables/query_log.md)의 필드 `query_cache_usage`는 실행된 각 쿼리에 대해 쿼리 결과가
쿼리 캐시에 기록되었는지, 또는 쿼리 캐시에서 읽혔는지를 보여줍니다. 시스템 테이블
[system.metrics](system-tables/metrics.md)의 메트릭 `QueryCacheEntries` 및 `QueryCacheBytes`는 현재 쿼리 캐시에
포함된 엔트리 수 / 바이트 수를 보여줍니다.

쿼리 캐시는 ClickHouse 서버 프로세스마다 하나씩 존재합니다. 다만 기본적으로 캐시 결과는 사용자 간에 공유되지 않습니다. 이 동작은
변경할 수 있지만(아래 참조), 보안상 권장되지 않습니다.

쿼리 결과는 해당 쿼리의 [Abstract Syntax Tree (AST)](https://en.wikipedia.org/wiki/Abstract_syntax_tree)를 기준으로
쿼리 캐시에서 참조됩니다. 즉, 캐싱은 대문자/소문자를 구분하지 않으므로 예를 들어 `SELECT 1`과 `select 1`은 동일한 쿼리로 처리됩니다. 더
자연스럽게 일치시키기 위해 쿼리 캐시 및 [출력 형식](settings/settings-formats.md))과 관련된 모든 쿼리 수준 설정은 AST에서
제거됩니다.

예외가 발생하거나 사용자가 취소하여 쿼리가 중단된 경우에는 쿼리 캐시에 엔트리가 기록되지 않습니다.

쿼리 캐시의 바이트 단위 크기, 캐시 엔트리의 최대 개수, 개별 캐시 엔트리의 최대 크기(바이트 및
레코드 기준)는 여러 [서버 구성 옵션](/ko/operations/server-configuration-parameters/settings#query_cache)으로 구성할 수 있습니다.

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

또한 [설정 프로필](settings/settings-profiles.md)과 [설정
제약 조건](settings/constraints-on-settings.md)을 사용해 개별 사용자의 캐시 사용량을 제한할 수도 있습니다. 구체적으로는 사용자가
쿼리 캐시에서 할당할 수 있는 최대 메모리 양(바이트 단위)과 저장할 수 있는 최대 쿼리 결과 수를 제한할 수 있습니다. 이를 위해 먼저 `users.xml`의 사용자 프로필에
[query&#95;cache&#95;max&#95;size&#95;in&#95;bytes](/ko/operations/settings/settings#query_cache_max_size_in_bytes) 및
[query&#95;cache&#95;max&#95;entries](/ko/operations/settings/settings#query_cache_max_entries) 구성을 지정한 다음, 두 설정을 모두
readonly로 설정합니다:

```xml
<profiles>
    <default>
        <!-- The maximum cache size in bytes for user/profile 'default' -->
        <query_cache_max_size_in_bytes>10000</query_cache_max_size_in_bytes>
        <!-- The maximum number of SELECT query results stored in the cache for user/profile 'default' -->
        <query_cache_max_entries>100</query_cache_max_entries>
        <!-- Make both settings read-only so the user cannot change them -->
        <constraints>
            <query_cache_max_size_in_bytes>
                <readonly/>
            </query_cache_max_size_in_bytes>
            <query_cache_max_entries>
                <readonly/>
            <query_cache_max_entries>
        </constraints>
    </default>
</profiles>
```

쿼리 결과를 캐시할 수 있도록 쿼리가 최소 얼마 동안 실행되어야 하는지 지정하려면
[query&#95;cache&#95;min&#95;query&#95;duration](/ko/operations/settings/settings#query_cache_min_query_duration) 설정을 사용할 수 있습니다. 예를 들어, 다음 쿼리의 결과는

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, query_cache_min_query_duration = 5000;
```

는 쿼리가 5초를 초과하여 실행되는 경우에만 캐시됩니다. 또한 결과가
캐시되기까지 쿼리가 몇 번 실행되어야 하는지도 지정할 수 있습니다. 이때는 설정 [query&#95;cache&#95;min&#95;query&#95;runs](/ko/operations/settings/settings#query_cache_min_query_runs)를 사용합니다.

쿼리 캐시의 엔트리는 일정 시간이 지나면 오래된 상태가 됩니다(time-to-live). 기본적으로 이
주기는 60초이지만, 설정 [query&#95;cache&#95;ttl](/ko/operations/settings/settings#query_cache_ttl)을 사용하여 세션(session), 프로필(profile) 또는 쿼리 수준에서 다른
값을 지정할 수 있습니다. 쿼리 캐시는 엔트리를 &quot;지연 방식으로&quot; 제거합니다. 즉, 엔트리가 오래된 상태가 되어도 즉시 캐시에서 제거되지는 않습니다. 대신 새 엔트리를
쿼리 캐시에 삽입하려고 할 때 데이터베이스는 새 엔트리를 위한 충분한 여유 공간이 캐시에 있는지 확인합니다. 그렇지 않은
경우 데이터베이스는 오래된 엔트리를 모두 제거하려고 시도합니다. 그래도 캐시에 여유 공간이 충분하지 않으면 새 엔트리는 삽입되지 않습니다.

쿼리가 HTTP를 통해 실행되면 ClickHouse는 캐시된 엔트리의 경과 시간(초 단위)과 만료 타임스탬프를 담은 `Age` 및 `Expires` 헤더를 설정합니다.

쿼리 캐시의 엔트리는 기본적으로 압축됩니다. 이렇게 하면 전체 메모리 사용량이 줄어드는 대신 쿼리 캐시에 대한 쓰기 / 읽기
속도가 느려집니다. 압축을 비활성화하려면 설정 [query&#95;cache&#95;compress&#95;entries](/ko/operations/settings/settings#query_cache_compress_entries)를 사용합니다.

경우에 따라 동일한 쿼리에 대해 여러 결과를 캐시된 상태로 유지하는 것이 유용할 수 있습니다. 이는 설정
[query&#95;cache&#95;tag](/ko/operations/settings/settings#query_cache_tag)를 사용해 구현할 수 있으며, 이 설정은 쿼리 캐시 엔트리의 레이블(또는 네임스페이스) 역할을 합니다. 쿼리 캐시는
동일한 쿼리라도 태그가 다르면 서로 다른 결과로 간주합니다.

동일한 쿼리에 대해 서로 다른 3개의 쿼리 캐시 엔트리를 생성하는 예시:

```sql
SELECT 1 SETTINGS use_query_cache = true; -- query_cache_tag is implicitly '' (empty string)
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 1';
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 2';
```

쿼리 캐시에서 `tag` 태그가 지정된 엔트리만 제거하려면 `SYSTEM CLEAR QUERY CACHE TAG 'tag'` 구문을 사용할 수 있습니다.

<div id="subquery-caching">
  ## 서브쿼리 캐싱
</div>

기본적으로 외부 쿼리에서 `use_query_cache`를 사용하도록 설정해도 그 설정은 서브쿼리로 전파되지 않습니다. 즉, 각 서브쿼리에서 캐싱을 명시적으로 활성화해야 합니다:

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000 SETTINGS use_query_cache = true)
WHERE number > 500;
```

이 예시에서는 내부 서브쿼리의 결과만 캐시됩니다. 외부 쿼리는 캐시되지 않습니다.

모든 서브쿼리에 대한 캐싱을 한 번에 활성화하려면 `query_cache_for_subqueries` 설정을 사용하십시오:

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000)
WHERE number > 500
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
```

일괄 전파가 활성화된 상태에서 특정 서브쿼리의 캐싱을 명시적으로 비활성화하려면 해당 서브쿼리에 `use_query_cache = false`를 설정하세요:

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000 SETTINGS use_query_cache = false)
WHERE number > 500
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
```

서브쿼리 캐시 엔트리는 `is_subquery = 1`인 경우 [system.query&#95;cache](system-tables/query_cache.md)에서 확인할 수 있습니다. `query_cache_ttl` 설정은 서브쿼리 캐시 엔트리에도 적용되며, 서브쿼리별로 설정할 수 있습니다.

ClickHouse는 테이블 데이터를 [max&#95;block&#95;size](/ko/operations/settings/settings#max_block_size)개 행 단위의 블록으로 읽습니다. 필터링, 집계
등으로 인해 결과 블록은 일반적으로 &#39;max&#95;block&#95;size&#39;보다 훨씬 작지만, 반대로 훨씬 큰 경우도 있습니다. 설정
[query&#95;cache&#95;squash&#95;partial&#95;results](/ko/operations/settings/settings#query_cache_squash_partial_results) (기본적으로 활성화됨)는 결과 블록을
쿼리 결과 캐시에 삽입하기 전에 &#39;max&#95;block&#95;size&#39; 크기의 블록으로 합칠지(매우 작은 경우) 또는 분할할지(큰 경우)를 제어합니다.
이 설정을 사용하면 쿼리 캐시에 쓰는 성능은 저하되지만, 캐시 엔트리의 압축률은 향상되며 이후 쿼리 캐시에서 쿼리 결과를 제공할 때 더 자연스러운
블록 세분화 수준을 제공합니다.

그 결과, 쿼리 캐시는 각 쿼리에 대해 여러 개의 (부분)
결과 블록을 저장합니다. 이 동작은 기본 설정으로는 적절하지만, 설정
[query&#95;cache&#95;squash&#95;partial&#95;results](/ko/operations/settings/settings#query_cache_squash_partial_results)를 사용하여 비활성화할 수 있습니다.

또한 비결정적 함수를 포함하는 쿼리의 결과는 기본적으로 캐시되지 않습니다. 이러한 함수에는 다음이 포함됩니다.

* 사전에 접근하는 함수: [`dictGet()`](/ko/sql-reference/functions/ext-dict-functions) 등
* XML
  정의에 `<deterministic>true</deterministic>` 태그가 없는 [사용자 정의 함수](../sql-reference/statements/create/function.md),
* 현재 날짜 또는 시간을 반환하는 함수: [`now()`](../sql-reference/functions/date-time-functions.md#now),
  [`today()`](../sql-reference/functions/date-time-functions.md#today),
  [`yesterday()`](../sql-reference/functions/date-time-functions.md#yesterday) 등,
* 임의의 값을 반환하는 함수: [`randomString()`](../sql-reference/functions/random-functions.md#randomString),
  [`fuzzBits()`](../sql-reference/functions/random-functions.md#fuzzBits) 등,
* 쿼리 처리에 사용되는 내부 청크의 크기와 순서에 따라 결과가 달라지는 함수:
  [`nowInBlock()`](../sql-reference/functions/date-time-functions.md#nowInBlock) 등,
  [`rowNumberInBlock()`](../sql-reference/functions/other-functions.md#rowNumberInBlock),
  [`runningDifference()`](../sql-reference/functions/other-functions.md#runningDifference),
  [`blockSize()`](../sql-reference/functions/other-functions.md#blockSize) 등,
* 환경에 따라 달라지는 함수: [`currentUser()`](../sql-reference/functions/other-functions.md#currentUser),
  [`queryID()`](/ko/sql-reference/functions/other-functions#queryID),
  [`getMacro()`](../sql-reference/functions/other-functions.md#getMacro) 등.

비결정적 함수를 포함하는 쿼리 결과도 강제로 캐시하려면, 설정
[query&#95;cache&#95;nondeterministic&#95;function&#95;handling](/ko/operations/settings/settings#query_cache_nondeterministic_function_handling)을 사용하십시오.

시스템 테이블이 포함된 쿼리(예: [system.processes](system-tables/processes.md)&#96; 또는
[information&#95;schema.tables](system-tables/information_schema.md))의 결과도 기본적으로 캐시되지 않습니다. 시스템 테이블이 포함된 쿼리 결과도
강제로 캐시하려면, 설정 [query&#95;cache&#95;system&#95;table&#95;handling](/ko/operations/settings/settings#query_cache_system_table_handling)을 사용하십시오.

마지막으로, 보안상의 이유로 쿼리 캐시의 엔트리는 사용자 간에 공유되지 않습니다. 예를 들어, 사용자 A는
이러한 정책이 없는 다른 사용자 B와 동일한 쿼리를 실행해 테이블의
ROW POLICY를 우회할 수 없어야 합니다. 그러나 필요한 경우 설정
[query&#95;cache&#95;share&#95;between&#95;users](/ko/operations/settings/settings#query_cache_share_between_users)를 지정하여 캐시 엔트리를
다른 사용자가 접근할 수 있도록(즉, 공유되도록) 표시할 수 있습니다.

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse 쿼리 캐시 소개](https://clickhouse.com/blog/introduction-to-the-clickhouse-query-cache-and-design)