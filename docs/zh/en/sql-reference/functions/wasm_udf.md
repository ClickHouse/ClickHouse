---
description: '关于 WebAssembly 用户自定义函数的文档'
sidebar_label: 'WebAssembly UDFs'
slug: /sql-reference/functions/wasm_udf
title: 'WebAssembly 用户自定义函数'
doc_type: 'guide'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="webassembly-user-defined-functions">
  # WebAssembly 用户自定义函数
</div>

ClickHouse 支持创建以 WebAssembly 编写的用户自定义函数 (UDF) 。这样，您就可以执行用 Rust、C、C++ 或其他语言编写并编译为 WebAssembly 模块的自定义逻辑。

<CloudNotSupportedBadge />

<ExperimentalBadge />

<div id="overview">
  ## 概述
</div>

WebAssembly 模块是一个已编译的二进制文件，其中包含一个或多个可供 ClickHouse 调用的函数。
可以将模块视为一个库或共享对象：加载一次，重复使用多次。

包含 UDF 的 WebAssembly 模块可以使用任何能够编译为 WebAssembly 的语言编写，例如 Rust、C 或 C++。

被编译为 WebAssembly 的代码 (“guest”代码) 以及执行它的 ClickHouse (“host”) 运行在沙箱环境中，只能访问专用的内存空间。

guest 代码会导出供 ClickHouse 调用的函数——其中既包括实现自定义逻辑的函数 (用于定义 UDF) ，也包括 ClickHouse 与 WebAssembly 代码之间进行内存管理和数据交换所需的支持函数。

你的代码应编译为“独立运行”的 WebAssembly (即 `wasm32-unknown-unknown`) ，且不依赖任何操作系统或标准库。此外，仅支持默认的 32 位 WebAssembly 目标 (不支持 `wasm64` 扩展) 。
该模块必须遵循一种受支持的通信协议 (ABI) 来与 ClickHouse 交互。

编译完成后，需要通过将其插入 `system.webassembly_modules` 表，把模块的二进制代码加载到 ClickHouse 中。
之后，你可以使用 `CREATE FUNCTION ... LANGUAGE WASM` 语句 创建引用该模块导出函数的 UDF。

<div id="prerequisites">
  ## 前置条件
</div>

在 ClickHouse 配置中启用 WebAssembly 支持：

```xml
<clickhouse>
    <allow_experimental_webassembly_udf>true</allow_experimental_webassembly_udf>
    <webassembly_udf_engine>wasmtime</webassembly_udf_engine>
</clickhouse>
```

可用的引擎实现：

* `wasmtime` (默认，推荐) — 使用 [WasmTime](https://github.com/bytecodealliance/wasmtime)
* `wasmedge` — 使用 [WasmEdge](https://github.com/WasmEdge/WasmEdge)

<div id="quick-start">
  ## 快速入门
</div>

本示例通过实现 [Collatz conjecture](https://en.wikipedia.org/wiki/Collatz_conjecture) 计算器，演示了创建 WebAssembly UDF 的完整流程。

我们将使用 WebAssembly 文本格式 (WAT) 编写代码。它是 WebAssembly 的一种便于人阅读的表示形式，因此在这个阶段无需使用任何编程语言。
ClickHouse 要求模块采用二进制格式，因此我们将使用转译器将 WAT 转换为 WASM。
要执行此转换，你可以使用 [WebAssembly Binary Toolkit (WABT)](https://github.com/WebAssembly/wabt) 中的 `wat2wasm`，或 [wasm-tools](https://github.com/bytecodealliance/wasm-tools) 中的 `parse` 命令。

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

在上面的代码片段中，我们使用 `FORMAT RawBlob` 通过管道将二进制 WASM 代码直接传入 ClickHouse 客户端，并将其插入 `system.webassembly_modules` 表。

然后，我们定义引用该模块导出的 `steps` 函数的 UDF：

```sql
CREATE FUNCTION collatz_steps LANGUAGE WASM ARGUMENTS (n UInt32) RETURNS UInt32 FROM 'collatz' :: 'steps';
```

请注意，我们在 `::` 后指定的是模块中的函数名，因为它与 UDF 名称不同。

现在我们可以在查询中使用 `collatz_steps` 函数：

```sql
SELECT groupArray(collatz_steps(number :: UInt32))
FROM numbers(1, 100)
FORMAT TSV
```

`number` 列被显式转换为 `UInt32`，因为 WebAssembly 函数要求其类型必须与 `CREATE FUNCTION` 语句中指定签名的类型精确匹配。

结果中，我们得到了 1 到 100 各数字对应的 Collatz 步数序列，对应于 [OEIS 中的 A006577 序列](https://oeis.org/A006577)。

```text
[0,1,7,2,5,8,16,3,19,6,14,9,9,17,17,4,12,20,20,7,7,15,15,10,23,10,111,18,18,18,106,5,26,13,13,21,21,21,34,8,109,8,29,16,16,16,104,11,24,24,24,11,11,112,112,19,32,19,32,19,19,107,107,6,27,27,27,14,14,14,102,22,115,22,14,22,22,35,35,9,22,110,110,9,9,30,30,17,30,17,92,17,17,105,105,12,118,25,25,25]
```

<div id="manage-wasm-modules-via-system-table">
  ## 通过系统表管理 WASM 模块
</div>

WebAssembly 模块存储在 `system.webassembly_modules` 表中，其结构如下：

* **列**
  * `name` String — 模块名称。不能为空，且只能包含字母、数字和下划线。
  * `code` String — 原始 WASM 二进制代码。仅写入，读取时返回空字符串。
  * `hash` UInt256 — 模块二进制文件的 SHA256 (如果文件已存在于磁盘上但尚未加载，则为零) 。

模块管理通过对该表执行标准 SQL 操作进行：

<div id="insert-a-module">
  ### 插入模块
</div>

```sql
INSERT INTO system.webassembly_modules (name, code)
SELECT 'my_module', base64Decode('AGFzbQEAAAA...');
```

(可选) 提供完整性哈希值：

```sql
INSERT INTO system.webassembly_modules (name, code, hash)
SELECT 'my_module', base64Decode('...'), reinterpretAsUInt256(unhex('369f...c57d'));
```

如果提供的哈希与根据模块代码计算出的 SHA256 不匹配，插入会失败。在从 S3 或 HTTP 等外部来源加载模块时，这会很有用。

<div id="distribute-a-module-across-a-cluster">
  ### 在 cluster 中分发 模块
</div>

`system.webassembly_modules` 是一张按实例分隔的表——`INSERT` 只会写入处理该 connection 的副本。`INSERT` 语句 没有 `ON CLUSTER` 这种形式，因此后续执行 `CREATE FUNCTION ... ON CLUSTER` 时，会在没有该 模块 的副本上失败：

```text
Code: 674. DB::Exception: WebAssembly module 'collatz' not found:
while adding user defined function `collatz_steps`. (RESOURCE_NOT_FOUND)
```

要将一次 `insert` 操作分发到每个节点，请写入 `cluster` 表函数，而不是本地的 `system.webassembly_modules` 表：

```bash
cat collatz.wasm | clickhouse client -q "
  INSERT INTO FUNCTION cluster('default', 'system', 'webassembly_modules') (name, code)
  SELECT 'collatz', code FROM input('code String') FORMAT RawBlob"
```

:::note
这种方式依赖底层分布式写入路径访问每个分片中的所有副本，而这只会在集群配置为 `internal_replication=false` 时发生。启用 `internal_replication=true` 时 (对于使用 `ReplicatedMergeTree` 自行执行复制的集群，这是默认设置) ，插入只会发送到每个分片中一个健康的副本，而 `system.webassembly_modules` 不会通过这条路径复制——因此某些副本仍会缺少该模块。在这种配置下，你需要分别向每个副本执行插入，例如遍历 `system.clusters` 并按主机通过 `remote(...)` 写入，或者将可执行文件复制到每台主机上的 `user_scripts/wasm/` 中。

你可以使用 `SELECT cluster, shard_num, internal_replication FROM system.clusters` 查看集群的 `internal_replication`。
:::

扇出插入完成后，该模块就会出现在每个副本上，且 `CREATE FUNCTION ... ON CLUSTER` 将成功执行：

```sql
CREATE FUNCTION collatz_steps ON CLUSTER 'default'
LANGUAGE WASM FROM 'collatz' :: 'steps'
ARGUMENTS (n UInt32) RETURNS UInt32;
```

你可以使用 `clusterAllReplicas` 验证该模块是否已在所有副本上加载：

```sql
SELECT hostName(), name FROM clusterAllReplicas('default', system.webassembly_modules) WHERE name = 'collatz';
```

向 `system.webassembly_modules` 中 insert 时，对于相同的 `(name, hash)` 组合是幂等的，因此重新执行扇出 insert 是安全的，也是副本被替换后修复状态的合理方式。请注意，新加入的服务器不会自动接收已有模块——你必须针对更新后的 cluster 重新执行 insert，或者将 binary 放到新主机上的 `user_scripts/wasm/` 目录中。

<div id="list-modules">
  ### 查看模块列表
</div>

```sql
SELECT name, lower(hex(reinterpretAsFixedString(hash))) AS sha256 FROM system.webassembly_modules

   ┌─name────┬─sha256───────────────────────────────────────────────────────────┐
1. │ collatz │ a084a10b7b5cb07db198bc93bf1f3c1f8cb8ef279df7a4f6b66b1cdd55d79c48 │
   └─────────┴──────────────────────────────────────────────────────────────────┘
```

<div id="delete-a-module">
  ### 删除模块
</div>

删除操作通过 `DELETE FROM system.webassembly_modules WHERE name = '...'` 语句执行。
谓词必须为 `name = 'literal'` (用于精确匹配) 或 `name LIKE 'pattern'` (用于删除名称匹配该模式的所有模块) ；不接受其他形态。

```sql
DELETE FROM system.webassembly_modules WHERE name = 'collatz';

-- Bulk-delete every module whose name starts with `tmp_` (literal underscore is escaped as `\_`):
DELETE FROM system.webassembly_modules WHERE name LIKE 'tmp\_%';
```

如果现有 UDFs 中有任何一个引用了某个匹配的模块，删除操作就会失败，因此必须先删除这些 UDFs。

<div id="create-a-webassembly-udf">
  ## 创建 WebAssembly UDF
</div>

**语法**:

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

**参数**:

* `function_name`：ClickHouse 中的函数名称。可以与模块中导出的函数名不同。
* `FROM 'module_name' :: 'source_function_name'`：要使用的已加载 WASM 模块名称，以及该 WASM 模块中的函数名 (默认为 function&#95;name)
* `ARGUMENTS`：argument 名称和类型列表 (名称可选，用于支持命名字段的 serialization format)
* `ABI`：Application Binary Interface 版本
  * `ROW_DIRECT`：直接类型映射，逐行处理
  * `BUFFERED_V1`：基于块的处理，带 serialization
  * `ASSEMBLYSCRIPT`：适用于由 [AssemblyScript](https://www.assemblyscript.org) 编译器 生成的模块的逐行处理。数值类型映射为 AssemblyScript primitives；ClickHouse `String` 映射为 AssemblyScript `string`。
* `DETERMINISTIC`：将该函数声明为 deterministic——对于相同输入始终返回相同输出。指定后，ClickHouse 可能会对所有 argument 均为常量的调用执行常量折叠：函数会在查询分析阶段计算一次，结果随后复用于每一行。
* `SHA256_HASH`：用于校验的预期模块哈希值 (如果省略会自动填充) ，可用于确保在不同副本上加载的是正确的 WASM 模块。
* `SETTINGS`：函数级 settings
  * `serialization_format` String — ABI 所需的 serialization format。支持的值：`MsgPack`、`JSONEachRow`、`CSV`、`TSV`、`TSVRaw`、`RowBinary` 和 `Buffers`。默认值：`MsgPack`。`Buffers` 等基于块的 formats 必须返回单列，且其类型必须与声明的函数签名匹配。
  * `webassembly_udf_enable_fuel` Bool — 为该函数启用有限 fuel 预算。默认值：`true`。当为 `false` 时，此函数会忽略查询级 setting `webassembly_udf_max_fuel`。使用 `wasmtime` engine 时，禁用 fuel 限制可能会提升性能。但对于不受信任或存在 bug 的 guest 代码，这也可能增加失控执行的风险。

<div id="abis-versions">
  ## ABI 版本
</div>

要与 ClickHouse 交互，WebAssembly 模块必须遵循受支持的 ABI (应用二进制接口) 之一。

* `ROW_DIRECT`：直接类型映射 (仅支持基本类型 `Int32`、`UInt32`、`Int64`、`UInt64`、`Float32`、`Float64`)
* `BUFFERED_V1`：支持序列化的复杂类型
* `ASSEMBLYSCRIPT`：与 [AssemblyScript](https://www.assemblyscript.org) 模块按行互操作；支持数值类型和 `String`。

<div id="abi-row_direct">
  ### ABI ROW_DIRECT
</div>

按行直接调用导出的 WASM 函数。

* 参数和返回类型均为数值类型 `Int32/UInt32/Int64/UInt64/Float32/Float64/Int128/UInt128`。
* 此 ABI 不支持 String。
* 签名必须与 WASM 导出一致 (`i32/i64/f32/f64/v128`) 。
* 模块无需导出任何 支持函数。

例如，具有以下签名的函数：

```
(func (param i32 i64 f32) (result f64) ...)
```

可按如下方式创建：

```sql
CREATE FUNCTION my_func ARGUMENTS (Int32, UInt64, Float32) RETURNS Float64 ...
```

WebAssembly 不区分有符号参数和无符号参数，而是通过不同的指令来解释这些值。因此，参数的大小必须完全匹配，而符号属性则由函数内部的操作决定。

<div id="abi-buffered_v1">
  ### ABI BUFFERED_V1
</div>

:::note
该 ABI 仍处于 Experimental 阶段，并且可能会在未来的发行版中发生变化。
:::

通过 WASM 内存中的 (反) 序列化一次处理整个块。支持任意参数和返回类型。

序列化后的数据会被复制到 wasm 内存中，并以指向缓冲区的指针形式传递给 UDF 函数 (该缓冲区由数据指针和数据大小组成) ，同时还会传入输入中的行数。因此，wasm 侧的用户自定义函数始终接受两个 `i32` 参数，并返回单个 `i32` 值。
guest 代码处理这些数据后，会返回一个指向结果缓冲区的指针，其中包含序列化后的结果数据。

guest 代码必须提供两个函数来创建和销毁这些缓冲区。

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

C 语言定义示例：

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

适用于由 [AssemblyScript](https://www.assemblyscript.org) 编译器生成的模块。每一行都会触发一次对导出函数的调用，将 ClickHouse 的值映射为 AssemblyScript 基本类型和字符串对象。

**支持的类型**：

* 数值类型：`Int8`/`UInt8`、`Int16`/`UInt16` (在边界处扩展为 `i32`) 、`Int32`/`UInt32`、`Int64`/`UInt64`、`Float32`、`Float64`

* `String` — 映射为 AssemblyScript `string` (WASM 内存中的 UTF-16) 。ClickHouse 会自动处理 UTF-8 ↔ UTF-16 的转换。

* 不支持将自定义 AssemblyScript 类用作参数或返回类型——它们的运行时类 id 在不同编译产物之间并不稳定 (参见 [AssemblyScript#2982](https://github.com/AssemblyScript/assemblyscript/issues/2982)) 。

**模块要求**：

模块必须使用 AssemblyScript 托管运行时进行编译，以便导出 `__new`、`__pin` 和 `__unpin`。标准的输入/输出字符串处理依赖这些符号。推荐的调用方式如下：

```bash
asc src.ts --runtime incremental --exportRuntime -o src.wasm
```

AssemblyScript 还会导入用于运行时陷阱 (如内存不足、边界检查等) 的 `env.abort`。ClickHouse 会自动提供此导入：当触发 `abort` 时，当前查询会失败，并抛出 `WASM_ERROR` 异常，其中包含解码后的 AssemblyScript 消息和源位置。

**示例**:

```typescript
// src.ts
export function add(a: u32, b: u32): u32 {
  return a + b;
}

export function greet(name: string): string {
  return "Hello, " + name + "!";
}
```

使用 `asc` 编译并将生成的 `.wasm` 加载到 `system.webassembly_modules` 后，按如下方式声明 UDFs：

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
  ### 使用 Rust 开发 UDFs 的说明
</div>

对于 Rust 程序，我们提供了一个辅助 crate [clickhouse-wasm-udf](https://crates.io/crates/clickhouse-wasm-udf)，用于简化 ClickHouse WebAssembly UDFs 的开发。该 crate 提供了内存管理相关函数，因此你无需手动实现 `clickhouse_create_buffer` 和 `clickhouse_destroy_buffer`，只需将该 crate 添加为依赖即可。此外，还提供了宏 `#[clickhouse_wasm_udf]`，可将普通的 Rust 函数封装为所需的 ABI 格式。

借助这个 crate，你可以像这样编写 UDFs：

```rust

use clickhouse_wasm_udf_bindgen::clickhouse_udf;

#[clickhouse_udf]
pub fn some_udf(data: String) -> HashMap<String, String> {
    // Your implementation here
}

```

宏会生成包装函数，该函数接受并返回 buffer 结构，并使用 `serde` 自动处理序列化和反序列化。

<div id="host-api-available-to-modules">
  ## 模块可用的宿主 API
</div>

模块可导入并使用以下宿主函数：

* `clickhouse_server_version() -> i64` — 以整数形式返回 ClickHouse server 版本 (例如，v25.11.1.1 对应 25011001) 。
* `clickhouse_throw(ptr: i32, size: i32)` — 使用提供的消息抛出错误。接受指向包含错误消息字符串的内存位置的指针，以及该字符串的大小。
* `clickhouse_log(ptr: i32, size: i32)` — 将消息记录到 ClickHouse server 文本日志中。
* `clickhouse_random(ptr: i32, size: i32)` — 用随机字节填充内存。
* `env.abort(message: i32, fileName: i32, line: i32, column: i32)` — 为兼容 AssemblyScript 的模块提供。调用它 (或触发会调用它的 AssemblyScript 运行时陷阱) 会终止 UDF，并抛出一个包含已解码消息和源位置的 `WASM_ERROR` 异常。未导入 `env.abort` 的模块不受影响。

<div id="settings">
  ## 设置
</div>

以下查询级设置用于控制 WebAssembly UDF 的执行：

* `webassembly_udf_max_fuel` — 每个 WebAssembly UDF 实例执行时的 fuel 限额。每条 WebAssembly 指令都会消耗一定数量的 fuel。该值在传递给 runtime 之前会先乘以 1024，因此 `webassembly_udf_max_fuel = 1` 大致相当于 1024 个 fuel 单位。设置为 0 表示不设有限值。仅适用于每个函数的 `webassembly_udf_enable_fuel` 设置为 true 的函数；该设置默认为 true。

* `webassembly_udf_max_memory` — 每个 WebAssembly UDF 实例的内存限制，以字节为单位。

* `webassembly_udf_max_input_block_size` — 单个块中传递给 WebAssembly UDF 的最大行数。设置为 0 表示一次处理所有行。

* `webassembly_udf_max_instances` — 每个函数可并行运行的 WebAssembly UDF 实例最大数量。

示例用法：

```sql
SET webassembly_udf_max_fuel = 200000;
SELECT my_wasm_udf(column) FROM table;
```

<div id="see-also">
  ## 另见
</div>

* [ClickHouse UDF 概览](/zh/sql-reference/functions/udf)