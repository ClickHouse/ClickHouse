---
description: 'WebAssembly 사용자 정의 함수에 대한 문서'
sidebar_label: 'WebAssembly UDFs'
slug: /sql-reference/functions/wasm_udf
title: 'WebAssembly 사용자 정의 함수'
doc_type: 'guide'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="webassembly-user-defined-functions">
  # WebAssembly 사용자 정의 함수
</div>

ClickHouse에서는 WebAssembly로 작성된 사용자 정의 함수(UDF)를 만들 수 있습니다. 이를 통해 Rust, C, C++ 등의 언어로 작성한 사용자 지정 로직을 WebAssembly 모듈로 컴파일해 실행할 수 있습니다.

<CloudNotSupportedBadge />

<ExperimentalBadge />

<div id="overview">
  ## 개요
</div>

WebAssembly 모듈은 ClickHouse에서 호출할 수 있는 하나 이상의 함수를 포함하는 컴파일된 바이너리 파일입니다.
모듈은 한 번 로드한 뒤 여러 번 재사용하는 라이브러리 또는 공유 객체라고 생각하면 됩니다.

UDF를 포함하는 WebAssembly 모듈은 Rust, C, C++처럼 WebAssembly로 컴파일할 수 있는 모든 언어로 작성할 수 있습니다.

WebAssembly로 컴파일된 코드(&quot;게스트&quot; 코드)와 ClickHouse에서 실행되는 코드(&quot;호스트&quot;)는 전용 메모리 공간에만 접근할 수 있는 샌드박스 환경에서 실행됩니다.

게스트 코드는 ClickHouse가 호출할 수 있는 함수를 내보냅니다. 여기에는 사용자 지정 로직을 구현하는 함수(UDF 정의에 사용됨)뿐 아니라 메모리 관리와 ClickHouse와 WebAssembly 코드 간 데이터 교환에 필요한 지원 함수도 포함됩니다.

코드는 운영 체제나 표준 라이브러리에 대한 의존성 없이 &quot;freestanding&quot; WebAssembly(즉, `wasm32-unknown-unknown`)로 컴파일되어야 합니다. 또한 기본 32비트 WebAssembly 대상만 지원됩니다(`wasm64` 확장 기능은 지원되지 않음).
모듈은 ClickHouse와 상호작용하기 위해 지원되는 통신 프로토콜(ABI) 중 하나를 따라야 합니다.

컴파일이 완료되면 모듈의 바이너리 코드를 `system.webassembly_modules` 테이블에 삽입하여 ClickHouse에 로드합니다.
그 후 `CREATE FUNCTION ... LANGUAGE WASM` 문를 사용하여 모듈이 내보낸 함수를 참조하는 UDF를 생성할 수 있습니다.

<div id="prerequisites">
  ## 사전 요구 사항
</div>

ClickHouse 구성에서 WebAssembly 지원을 활성화하세요:

```xml
<clickhouse>
    <allow_experimental_webassembly_udf>true</allow_experimental_webassembly_udf>
    <webassembly_udf_engine>wasmtime</webassembly_udf_engine>
</clickhouse>
```

사용 가능한 엔진 구현:

* `wasmtime` (기본값, 권장) — [WasmTime](https://github.com/bytecodealliance/wasmtime) 사용
* `wasmedge` — [WasmEdge](https://github.com/WasmEdge/WasmEdge) 사용

<div id="quick-start">
  ## 빠른 시작
</div>

이 예시는 [Collatz conjecture](https://en.wikipedia.org/wiki/Collatz_conjecture) 계산기를 구현하여 WebAssembly UDF를 만드는 전체 워크플로를 보여줍니다.

이 단계에서는 별도의 프로그래밍 언어가 필요하지 않으므로, WebAssembly를 사람이 읽을 수 있는 형태로 표현한 WebAssembly Text format (WAT)으로 코드를 작성합니다.
ClickHouse는 모듈이 바이너리 형식이어야 하므로, 트랜스파일러를 사용해 WAT를 WASM으로 변환합니다.
이 변환을 수행하려면 [WebAssembly Binary Toolkit (WABT)](https://github.com/WebAssembly/wabt)의 `wat2wasm` 또는 [wasm-tools](https://github.com/bytecodealliance/wasm-tools)의 `parse` 명령을 사용할 수 있습니다.

```bash
cat << 'EOF' | wasm-tools parse | clickhouse client -q "INSERT INTO system.webassembly_modules (name, code) SELECT 'collatz', code FROM input('code String') FORMAT RawBlob"
(module
  (func $next (param $n i32) (result i32)
    local.get $n i32.const 1 i32.and
    (if (result i32)
      (then local.get $n i32.const 3 i32.mul i32.const 1 i32.add)
      (else local.get $n i32.const 2 i32.div_u)))
  (func $steps (export "steps") (param $n i32) (result i32)
    (local $count i32)
    local.get $n i32.const 1 i32.lt_u
    (if (then i32.const 0 return))
    (block $done (loop $loop
      local.get $n i32.const 1 i32.eq br_if $done
      local.get $n call $next local.set $n
      local.get $count i32.const 1 i32.add local.set $count
      br $loop))
    local.get $count)
)
EOF
```

위 스니펫에서는 바이너리 WASM 코드를 `FORMAT RawBlob`으로 clickhouse client에 직접 파이프해 `system.webassembly_modules` 테이블(table)에 삽입합니다.

그런 다음 모듈에서 내보낸 `steps` 함수를 참조하는 UDF를 정의합니다:

```sql
CREATE FUNCTION collatz_steps LANGUAGE WASM ARGUMENTS (n UInt32) RETURNS UInt32 FROM 'collatz' :: 'steps';
```

함수 이름은 UDF 이름과 다르므로, `::` 뒤에는 모듈의 함수 이름을 지정한다는 점에 유의하십시오.

이제 쿼리에서 `collatz_steps` 함수를 사용할 수 있습니다:

```sql
SELECT groupArray(collatz_steps(number :: UInt32))
FROM numbers(1, 100)
FORMAT TSV
```

`number` 컬럼은 WebAssembly 함수가 `CREATE FUNCTION` 문에 지정된 시그니처의 타입과 정확히 일치하는 값을 기대하므로, 명시적으로 `UInt32`로 형변환됩니다.

그 결과 1부터 100까지의 숫자에 대한 Collatz 단계 시퀀스를 얻었으며, 이는 [OEIS의 A006577 시퀀스](https://oeis.org/A006577)에 해당합니다.

```text
[0,1,7,2,5,8,16,3,19,6,14,9,9,17,17,4,12,20,20,7,7,15,15,10,23,10,111,18,18,18,106,5,26,13,13,21,21,21,34,8,109,8,29,16,16,16,104,11,24,24,24,11,11,112,112,19,32,19,32,19,19,107,107,6,27,27,27,14,14,14,102,22,115,22,14,22,22,35,35,9,22,110,110,9,9,30,30,17,30,17,92,17,17,105,105,12,118,25,25,25]
```

<div id="manage-wasm-modules-via-system-table">
  ## 시스템 테이블을 통해 WASM 모듈 관리
</div>

WebAssembly 모듈은 다음과 같은 구조의 `system.webassembly_modules` 테이블에 저장됩니다:

* **컬럼**
  * `name` String — 모듈 이름입니다. 비어 있을 수 없고, 영문자, 숫자, 밑줄 등의 단어 문자만 사용할 수 있습니다.
  * `code` String — 원시 바이너리 WASM 코드입니다. 쓰기 전용이며, 읽기 시에는 빈 문자열이 반환됩니다.
  * `hash` UInt256 — 모듈 바이너리의 SHA256입니다(디스크에는 존재하지만 아직 로드되지 않은 경우에는 0).

모듈 관리는 이 테이블에 대한 표준 SQL 연산을 통해 수행됩니다:

<div id="insert-a-module">
  ### 모듈 삽입
</div>

```sql
INSERT INTO system.webassembly_modules (name, code)
SELECT 'my_module', base64Decode('AGFzbQEAAAA...');
```

선택 사항으로 무결성 해시를 제공할 수 있습니다:

```sql
INSERT INTO system.webassembly_modules (name, code, hash)
SELECT 'my_module', base64Decode('...'), reinterpretAsUInt256(unhex('369f...c57d'));
```

제공된 해시가 모듈 코드에서 계산한 SHA256과 일치하지 않으면 삽입이 실패합니다. S3 또는 HTTP와 같은 외부 소스에서 모듈을 로드할 때 유용할 수 있습니다.

<div id="distribute-a-module-across-a-cluster">
  ### 클러스터 전체에 모듈 배포
</div>

`system.webassembly_modules`는 인스턴스별 테이블(table)이므로 `INSERT`는 해당 연결(connection)을 처리하는 레플리카에만 반영됩니다. `INSERT` 구문에는 `ON CLUSTER` 형식이 없으므로, 이후 `CREATE FUNCTION ... ON CLUSTER`는 해당 모듈이 없는 레플리카에서 실패합니다:

```text
Code: 674. DB::Exception: WebAssembly module 'collatz' not found:
while adding user defined function `collatz_steps`. (RESOURCE_NOT_FOUND)
```

삽입 작업을 모든 노드에 분산하려면 로컬 `system.webassembly_modules` 테이블 대신 `cluster` 테이블 함수를 사용해 쓰기 작업을 수행하십시오:

```bash
cat collatz.wasm | clickhouse client -q "
  INSERT INTO FUNCTION cluster('default', 'system', 'webassembly_modules') (name, code)
  SELECT 'collatz', code FROM input('code String') FORMAT RawBlob"
```

:::note
이 패턴은 기본 분산 쓰기 경로가 각 세그먼트의 모든 레플리카를 거치는 것을 전제로 하며, 이는 클러스터가 `internal_replication=false`로 구성된 경우에만 발생합니다. `internal_replication=true`인 경우(`ReplicatedMergeTree`가 자체적으로 복제를 수행하도록 사용하는 클러스터의 기본값)에는 삽입이 세그먼트당 정상인 레플리카 1개에만 전달되고, `system.webassembly_modules`는 이 경로를 통해 복제되지 않으므로 일부 레플리카에는 여전히 모듈이 없을 수 있습니다. 이 구성에서는 각 레플리카에 개별적으로 삽입해야 합니다. 예를 들어 `system.clusters`를 순회하며 호스트별로 `remote(...)`를 통해 쓰거나, 모든 호스트의 `user_scripts/wasm/`에 바이너리를 복사하면 됩니다.

클러스터의 `internal_replication`은 `SELECT cluster, shard_num, internal_replication FROM system.clusters`로 확인할 수 있습니다.
:::

이렇게 fan-out 삽입을 수행한 후에는 모든 레플리카에 모듈이 존재하게 되며, `CREATE FUNCTION ... ON CLUSTER`가 성공합니다:

```sql
CREATE FUNCTION collatz_steps ON CLUSTER 'default'
LANGUAGE WASM FROM 'collatz' :: 'steps'
ARGUMENTS (n UInt32) RETURNS UInt32;
```

`clusterAllReplicas`를 사용하면 모든 레플리카에 모듈이 로드되었는지 확인할 수 있습니다.

```sql
SELECT hostName(), name FROM clusterAllReplicas('default', system.webassembly_modules) WHERE name = 'collatz';
```

`system.webassembly_modules`에 대한 삽입은 동일한 `(name, hash)` 쌍에 대해 멱등성을 보장하므로, 분산된 삽입을 다시 실행해도 안전하며 레플리카가 교체된 후 상태를 복구하는 현실적인 방법이 됩니다. 새로 추가된 서버는 기존 모듈을 소급 적용받지 않는다는 점에 유의하십시오. 업데이트된 클러스터를 대상으로 삽입을 다시 실행하거나, 새 호스트의 `user_scripts/wasm/` 디렉터리에 바이너리를 배치해야 합니다.

<div id="list-modules">
  ### 모듈 목록 조회
</div>

```sql
SELECT name, lower(hex(reinterpretAsFixedString(hash))) AS sha256 FROM system.webassembly_modules

   ┌─name────┬─sha256───────────────────────────────────────────────────────────┐
1. │ collatz │ a084a10b7b5cb07db198bc93bf1f3c1f8cb8ef279df7a4f6b66b1cdd55d79c48 │
   └─────────┴──────────────────────────────────────────────────────────────────┘
```

<div id="delete-a-module">
  ### 모듈 삭제
</div>

삭제는 `DELETE FROM system.webassembly_modules WHERE name = '...'` 문을 사용해 수행됩니다.
프레디케이트는 정확히 일치하는 `name = 'literal'` 또는 이름이 패턴과 일치하는 모든 모듈을 삭제하는 `name LIKE 'pattern'` 중 하나여야 하며, 그 외의 형태는 허용되지 않습니다.

```sql
DELETE FROM system.webassembly_modules WHERE name = 'collatz';

-- Bulk-delete every module whose name starts with `tmp_` (literal underscore is escaped as `\_`):
DELETE FROM system.webassembly_modules WHERE name LIKE 'tmp\_%';
```

기존 UDF 중 일치하는 모듈을 참조하는 항목이 있으면 삭제에 실패하므로, 먼저 해당 UDF를 삭제해야 합니다.

<div id="create-a-webassembly-udf">
  ## WebAssembly UDF 생성하기
</div>

**구문**:

```sql
CREATE [OR REPLACE] FUNCTION function_name
LANGUAGE WASM
FROM 'module_name' [:: 'source_function_name']
ARGUMENTS ( [name type[, ...]] | [type[, ...]] )
RETURNS return_type
[ABI ROW_DIRECT | ABI BUFFERED_V1 | ABI ASSEMBLYSCRIPT]
[DETERMINISTIC]
[SHA256_HASH 'hex']
[SETTINGS key = value[, ...]];
```

**매개변수**:

* `function_name`: ClickHouse에서의 함수 이름입니다. 모듈의 내보낸 함수 이름과 다를 수 있습니다.
* `FROM 'module_name' :: 'source_function_name'`: 사용할 로드된 WASM 모듈 이름과 WASM 모듈 내 함수 이름입니다(기본값은 function&#95;name).
* `ARGUMENTS`: 인수 이름과 타입 목록입니다(이름은 선택 사항이며, 이름 있는 필드를 지원하는 직렬화 포맷에서 사용됩니다).
* `ABI`: Application Binary Interface 버전
  * `ROW_DIRECT`: 직접 타입 매핑, 행 단위 처리
  * `BUFFERED_V1`: 직렬화를 사용하는 블록 기반 처리
  * `ASSEMBLYSCRIPT`: [AssemblyScript](https://www.assemblyscript.org) 컴파일러로 생성된 모듈용 행 단위 처리입니다. 숫자 타입은 AssemblyScript 기본 타입에 매핑되며, ClickHouse `String`은 AssemblyScript `string`에 매핑됩니다.
* `DETERMINISTIC`: 함수를 결정적으로 선언합니다 — 동일한 입력에 대해 항상 동일한 출력을 반환합니다. 지정하면 ClickHouse는 모든 인수가 상수인 호출을 상수 폴딩할 수 있습니다. 함수는 쿼리 분석 시점에 한 번 평가되며, 그 결과는 모든 행에 재사용됩니다.
* `SHA256_HASH`: 검증용으로 예상되는 모듈 해시입니다(생략하면 자동으로 채워짐). 서로 다른 레플리카 전반에서 올바른 WASM 모듈이 로드되었는지 확인하는 데 사용할 수 있습니다.
* `SETTINGS`: 함수별 설정
  * `serialization_format` String — ABI에 필요할 때 사용하는 직렬화 포맷입니다. 지원 값: `MsgPack`, `JSONEachRow`, `CSV`, `TSV`, `TSVRaw`, `RowBinary`, `Buffers`. 기본값: `MsgPack`. `Buffers`와 같은 블록 기반 포맷은 선언된 함수 시그니처와 타입이 일치하는 단일 컬럼을 반환해야 합니다.
  * `webassembly_udf_enable_fuel` Bool — 함수의 유한 연료 예산을 활성화합니다. 기본값: `true`. `false`이면 쿼리 수준 설정 `webassembly_udf_max_fuel`은 이 함수에 대해 무시됩니다. 연료 제한을 비활성화하면 `wasmtime` 엔진 사용 시 성능이 향상될 수 있습니다. 그러나 신뢰할 수 없거나 버그가 있는 게스트 코드의 경우 제어되지 않는 실행 위험이 커질 수 있습니다.

<div id="abis-versions">
  ## ABI 버전
</div>

ClickHouse와 상호 작용하려면 WebAssembly 모듈이 지원되는 ABI(Application Binary Interface) 중 하나를 따라야 합니다.

* `ROW_DIRECT`: 직접 타입 매핑(원시 타입 `Int32`, `UInt32`, `Int64`, `UInt64`, `Float32`, `Float64`만 지원)
* `BUFFERED_V1`: 직렬화를 사용하는 복합 타입
* `ASSEMBLYSCRIPT`: [AssemblyScript](https://www.assemblyscript.org) 모듈과 행별 상호 운용을 지원하며, 숫자 타입과 `String`을 지원합니다.

<div id="abi-row_direct">
  ### ABI ROW_DIRECT
</div>

각 행에 대해 내보낸 WASM 함수를 직접 호출합니다.

* 인수와 반환 타입은 숫자 타입 `Int32/UInt32/Int64/UInt64/Float32/Float64/Int128/UInt128`이어야 합니다.
* 이 ABI에서는 String 타입이 지원되지 않습니다.
* 시그니처는 WASM export(`i32/i64/f32/f64/v128`)와 일치해야 합니다.
* 모듈에서 내보내야 하는 보조 함수는 필요하지 않습니다.

예를 들어, 시그니처가 다음과 같은 함수:

```
(func (param i32 i64 f32) (result f64) ...)
```

다음과 같이 생성할 수 있습니다:

```sql
CREATE FUNCTION my_func ARGUMENTS (Int32, UInt64, Float32) RETURNS Float64 ...
```

WebAssembly는 부호 있는 인수와 부호 없는 인수를 구분하지 않으며, 대신 값을 해석할 때 서로 다른 명령어를 사용합니다. 따라서 인수의 크기는 정확히 일치해야 하고, 부호 여부는 함수 내부의 연산에 따라 결정됩니다.

<div id="abi-buffered_v1">
  ### ABI BUFFERED_V1
</div>

:::note
이 ABI는 실험적 기능이며 향후 릴리스에서 변경될 수 있습니다.
:::

WASM 메모리를 통한 (역)직렬화를 사용해 전체 블록을 한 번에 처리합니다. 모든 인수 및 반환 타입을 지원합니다.

직렬화된 데이터는 wasm 메모리에 복사되며, 이 메모리는 버퍼에 대한 포인터(데이터 포인터와 데이터 크기로 구성됨)와 입력 행 수와 함께 UDF 함수에 전달됩니다. 따라서 wasm 측의 사용자 정의 함수는 항상 두 개의 `i32` 인수를 받고, 하나의 `i32` 값을 반환합니다.
게스트 코드는 데이터를 처리한 뒤, 직렬화된 결과 데이터가 들어 있는 결과 버퍼에 대한 포인터를 반환합니다.

게스트 코드는 이러한 버퍼를 생성하고 해제하는 두 개의 함수를 제공해야 합니다.

```
(module
  ;; Allocate a new buffer of specified size
  ;; Returns: handle to Buffer structure (not direct data pointer!) with pointer to data and size
  (func (export "clickhouse_create_buffer")
    (param $size i32)    ;; Size of data to allocate
    (result i32))        ;; Returns buffer handle with enough space

  ;; Free a buffer by its handle
  (func (export "clickhouse_destroy_buffer")
    (param $handle i32)  ;; Buffer handle to free
    (result))            ;; No return value

    ;; User-defined function
    (func (export "user_defined_function1")
      (param $input_buffer_handle i32)  ;; Input buffer handle
      (param $n i32)                    ;; Number of rows in input
      (result i32))                     ;; Returns output buffer handle
)
```

C 정의 예시:

```c
typedef struct {
    uint8_t * data;
    uint32_t size;
} ClickhouseBuffer;

ClickhouseBuffer * clickhouse_create_buffer(uint32_t size) { /* ... */ }

void clickhouse_destroy_buffer(ClickhouseBuffer * data) { /* ... */ }

/// Example user-defined functions
ClickhouseBuffer * user_defined_function1(ClickhouseBuffer * span, uint32_t n) { /* ... */ }
ClickhouseBuffer * user_defined_function2(ClickhouseBuffer * span, uint32_t n) { /* ... */ }
```

<div id="abi-assemblyscript">
  ### ABI ASSEMBLYSCRIPT
</div>

[AssemblyScript](https://www.assemblyscript.org) 컴파일러로 생성된 모듈을 대상으로 합니다. 각 행마다 내보낸 함수가 한 번 호출되며, ClickHouse 값은 AssemblyScript 기본 타입과 string 객체에 매핑됩니다.

**지원되는 타입**:

* 숫자형: `Int8`/`UInt8`, `Int16`/`UInt16` (경계에서 `i32`로 확장), `Int32`/`UInt32`, `Int64`/`UInt64`, `Float32`, `Float64`

* `String` — AssemblyScript `string`에 매핑됩니다(WASM 메모리에서는 UTF-16). ClickHouse가 UTF-8 ↔ UTF-16 변환을 자동으로 처리합니다.

* 사용자 정의 AssemblyScript 클래스는 인수 또는 반환 타입으로 지원되지 않습니다. 해당 런타임 클래스 id는 컴파일마다 안정적이지 않습니다([AssemblyScript#2982](https://github.com/AssemblyScript/assemblyscript/issues/2982) 참조).

**모듈 요구 사항**:

모듈은 `__new`, `__pin`, `__unpin`이 내보내지도록 AssemblyScript 관리형 런타임으로 컴파일해야 합니다. 표준 문자열 입출력 처리는 이를 전제로 합니다. 권장 호출 방식은 다음과 같습니다:

```bash
asc src.ts --runtime incremental --exportRuntime -o src.wasm
```

AssemblyScript는 런타임 트랩(메모리 부족, 경계 검사 등)을 위해 `env.abort`도 import합니다. ClickHouse는 이 import를 자동으로 제공합니다. `abort`가 트리거되면 현재 실행 중인 쿼리는 디코딩된 AssemblyScript 메시지와 소스 위치가 포함된 `WASM_ERROR` 예외와 함께 실패합니다.

**예시**:

```typescript
// src.ts
export function add(a: u32, b: u32): u32 {
  return a + b;
}

export function greet(name: string): string {
  return "Hello, " + name + "!";
}
```

`asc`로 컴파일하고 생성된 `.wasm`을 `system.webassembly_modules`에 로드한 다음, UDF를 다음과 같이 선언합니다:

```sql
CREATE FUNCTION as_add
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_example' :: 'add'
    ARGUMENTS (a UInt32, b UInt32) RETURNS UInt32;

CREATE FUNCTION as_greet
    LANGUAGE WASM ABI ASSEMBLYSCRIPT
    FROM 'as_example' :: 'greet'
    ARGUMENTS (name String) RETURNS String;
```

<div id="note-for-developing-udfs-in-rust">
  ### Rust로 UDF를 개발할 때 참고 사항
</div>

Rust 프로그램의 경우 ClickHouse용 WebAssembly UDF 개발을 간소화할 수 있도록 보조 크레이트 [clickhouse-wasm-udf](https://crates.io/crates/clickhouse-wasm-udf)를 제공합니다. 이 크레이트는 메모리 관리를 위한 함수를 제공하므로 `clickhouse_create_buffer` 및 `clickhouse_destroy_buffer` 함수를 직접 구현할 필요 없이 크레이트를 종속성으로 추가하면 됩니다. 또한 일반 Rust 함수를 필요한 ABI 포맷으로 감싸는 매크로 `#[clickhouse_wasm_udf]`도 제공합니다.

이 크레이트를 사용하면 다음과 같이 UDF를 작성할 수 있습니다:

```rust

use clickhouse_wasm_udf_bindgen::clickhouse_udf;

#[clickhouse_udf]
pub fn some_udf(data: String) -> HashMap<String, String> {
    // Your implementation here
}

```

매크로는 버퍼 구조체를 인자로 받고 반환하는 래퍼 함수를 생성하며, `serde`를 사용해 직렬화와 역직렬화를 자동으로 처리합니다.

<div id="host-api-available-to-modules">
  ## 모듈에서 사용할 수 있는 호스트 API
</div>

다음 호스트 함수는 모듈에서 import하여 사용할 수 있습니다:

* `clickhouse_server_version() -> i64` — ClickHouse 서버 버전을 정수로 반환합니다(예: v25.11.1.1은 25011001).
* `clickhouse_throw(ptr: i32, size: i32)` — 지정된 메시지로 오류를 발생시킵니다. 오류 메시지 문자열이 저장된 메모리 위치의 포인터와 문자열 크기를 인자로 받습니다.
* `clickhouse_log(ptr: i32, size: i32)` — ClickHouse 서버 텍스트 로그에 메시지를 기록합니다.
* `clickhouse_random(ptr: i32, size: i32)` — 메모리를 임의의 바이트로 채웁니다.
* `env.abort(message: i32, fileName: i32, line: i32, column: i32)` — AssemblyScript 호환 모듈을 위해 제공됩니다. 이를 호출하면(또는 이를 호출하는 AssemblyScript 런타임 트랩이 트리거되면) 디코딩된 메시지와 소스 위치를 포함한 `WASM_ERROR` 예외와 함께 UDF가 종료됩니다. `env.abort`를 import하지 않는 모듈에는 영향을 주지 않습니다.

<div id="settings">
  ## 설정
</div>

다음 쿼리 수준 설정은 WebAssembly UDF 실행을 제어합니다.

* `webassembly_udf_max_fuel` — WebAssembly UDF 인스턴스 실행당 연료 한도입니다. 각 WebAssembly 명령어는 일정량의 연료를 소비합니다. 이 값은 런타임에 전달되기 전에 1024배로 조정되므로, `webassembly_udf_max_fuel = 1`은 대략 1024 연료 단위에 해당합니다. 제한을 두지 않으려면 0으로 설정합니다. 기본적으로 true인 함수별 설정 `webassembly_udf_enable_fuel`이 true인 함수에만 적용됩니다.

* `webassembly_udf_max_memory` — WebAssembly UDF 인스턴스당 바이트 단위 메모리 한도입니다.

* `webassembly_udf_max_input_block_size` — 단일 블록으로 WebAssembly UDF에 전달되는 최대 행 수입니다. 모든 행을 한 번에 처리하려면 0으로 설정합니다.

* `webassembly_udf_max_instances` — 함수별로 병렬 실행할 수 있는 WebAssembly UDF 인스턴스의 최대 개수입니다.

사용 예시:

```sql
SET webassembly_udf_max_fuel = 200000;
SELECT my_wasm_udf(column) FROM table;
```

<div id="see-also">
  ## 관련 항목
</div>

* [ClickHouse UDF 개요](/ko/sql-reference/functions/udf)