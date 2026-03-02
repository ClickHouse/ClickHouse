---
description: 'Documentation for WebAssembly User Defined Functions'
sidebar_label: 'WebAssembly UDFs'
slug: /sql-reference/functions/wasm_udf
title: 'WebAssembly User Defined Functions'
doc_type: 'guide'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

# WebAssembly User-Defined Functions

ClickHouse supports creating user-defined functions (UDFs) written in WebAssembly. This allows you to execute custom logic written in languages like Rust, C, C++, or others by compiling them to WebAssembly modules.

<CloudNotSupportedBadge/>
<ExperimentalBadge/>

## Overview

A WebAssembly module is a compiled binary file that contains one or more functions that can be called from ClickHouse.
Think of a module as a library or shared object that you load once and reuse many times.

WebAssembly module containing UDFs can be written in any language that can compile to WebAssembly, such as Rust, C, or C++.

Code compiled to WebAssembly ("guest" code) and executed by ClickHouse  ("host") run in a sandboxed environment having access only to a dedicated memory space.

Guest code exports functions that ClickHouse can invoke - these include the functions that implement your custom logic (used to define UDFs) as well as support functions required for memory management and data exchange between ClickHouse and the WebAssembly code.

Your code should be compiled to "freestanding" WebAssembly (aka `wasm32-unknown-unknown`) without any dependencies on an operating system or standard library. Also only default 32-bit WebAssembly target is supported (no `wasm64` extension).
The module must follow one of the supported communication protocols (ABIs) for interacting with ClickHouse.

Once compiled, the module's binary code is loaded into ClickHouse by inserting it into the `system.webassembly_modules` table.
After that, you can create UDFs that reference functions exported by the module using the `CREATE FUNCTION ... LANGUAGE WASM` statement.

## Prerequisites

Enable WebAssembly support in your ClickHouse configuration:

```xml
<clickhouse>
    <allow_experimental_webassembly_udf>true</allow_experimental_webassembly_udf>
    <webassembly_udf_engine>wasmtime</webassembly_udf_engine>
</clickhouse>
```

Available Engine Implementations:

- `wasmtime` (default, recommended) — uses [WasmTime](https://github.com/bytecodealliance/wasmtime)
- `wasmedge` — uses [WasmEdge](https://github.com/WasmEdge/WasmEdge)


## Quick Start

This example demonstrates the complete workflow of creating a WebAssembly UDF by implementing the [Collatz conjecture](https://en.wikipedia.org/wiki/Collatz_conjecture) calculator.

We'll write the code in WebAssembly Text format (WAT), which is a human-readable representation of WebAssembly, so no any programming language is required at this stage.
ClickHouse requires the module to be in binary format, so we'll use the transpiler to convert WAT to WASM.
To perform this conversion you may use `wat2wasm` from the [WebAssembly Binary Toolkit (WABT)](https://github.com/WebAssembly/wabt) or `parse` command from the [wasm-tools](https://github.com/bytecodealliance/wasm-tools).

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

In snippet above we pipe binary WASM code directly into ClickHouse client using `FORMAT RawBlob` to insert it into `system.webassembly_modules` table.

Then we define the UDF that references the `steps` function exported by the module:

```sql
CREATE FUNCTION collatz_steps LANGUAGE WASM ARGUMENTS (n UInt32) RETURNS UInt32 FROM 'collatz' :: 'steps';
```

Note that we specify function name from the module after `::`, because it differs from the UDF name.

Now we can use the `collatz_steps` function in our queries:

```sql
SELECT groupArray(collatz_steps(number :: UInt32))
FROM numbers(1, 100)
FORMAT TSV
```

The `number` column is explicitly cast to `UInt32`, because WebAssembly functions expect exact type matches specified signature in `CREATE FUNCTION` statement.

In the result we got sequence of Collatz steps for numbers from 1 to 100, corresponding to sequence [A006577 from the OEIS](https://oeis.org/A006577).

```text
[0,1,7,2,5,8,16,3,19,6,14,9,9,17,17,4,12,20,20,7,7,15,15,10,23,10,111,18,18,18,106,5,26,13,13,21,21,21,34,8,109,8,29,16,16,16,104,11,24,24,24,11,11,112,112,19,32,19,32,19,19,107,107,6,27,27,27,14,14,14,102,22,115,22,14,22,22,35,35,9,22,110,110,9,9,30,30,17,30,17,92,17,17,105,105,12,118,25,25,25]
```

## Manage WASM modules via system table


WebAssembly modules are stored in the `system.webassembly_modules` table having the following structure:

- **Columns**
  - `name` String — Module name. Non-empty, word characters only.
  - `code` String — Raw binary WASM code. Write-only, reads return empty string.
  - `hash` UInt256 — SHA256 of the module binary (zero if present on disk but not yet loaded).

Module management happens through standard SQL operations on this table:

### Insert a module

```sql
INSERT INTO system.webassembly_modules (name, code)
SELECT 'my_module', base64Decode('AGFzbQEAAAA...');
```

Optionally, provide integrity hash:

```sql
INSERT INTO system.webassembly_modules (name, code, hash)
SELECT 'my_module', base64Decode('...'), reinterpretAsUInt256(unhex('369f...c57d'));
```

If the provided hash does not match the computed SHA256 of the module code, the insertion fails. It may be useful when loading modules from external sources such as S3 or HTTP.

### List modules

```sql
SELECT name, lower(hex(reinterpretAsFixedString(hash))) AS sha256 FROM system.webassembly_modules

   ┌─name────┬─sha256───────────────────────────────────────────────────────────┐
1. │ collatz │ a084a10b7b5cb07db198bc93bf1f3c1f8cb8ef279df7a4f6b66b1cdd55d79c48 │
   └─────────┴──────────────────────────────────────────────────────────────────┘
```

### Delete a module

Deletion performed by `DELETE FROM system.webassembly_modules WHERE name = '...'` statement.
Only deletion of single module by exact name per single statement is supported.

```sql
DELETE FROM system.webassembly_modules WHERE name = 'collatz';
```

If any existing UDFs reference the module, the deletion fails, so you must drop those UDFs first.

## Create a WebAssembly UDF

**Syntax**:

```sql
CREATE [OR REPLACE] FUNCTION function_name
LANGUAGE WASM
FROM 'module_name' [:: 'source_function_name']
ARGUMENTS ( [name type[, ...]] | [type[, ...]] )
RETURNS return_type
[ABI ROW_DIRECT | ABI BUFFERED_V1]
[SHA256_HASH 'hex']
[SETTINGS key = value[, ...]];
```

**Parameters**:

- `function_name`: Name of the function in ClickHouse. May be different from the exported function name in the module.
- `FROM 'module_name' :: 'source_function_name'`: Name of the loaded WASM module and function name in WASM module to use (defaults to function_name)
- `ARGUMENTS`: List of argument names and types (names optional and used for serialization formats that support named fields)
- `ABI`: Application Binary Interface version
  - `ROW_DIRECT`: Direct type mapping, row-by-row processing
  - `BUFFERED_V1`: Block-based processing with serialization
- `SHA256_HASH`: Expected module hash for verification (auto-filled if omitted), can be used to ensure the correct WASM module loaded across different replicas.
- `SETTINGS`: Per-function settings
    - `max_fuel` UInt64 — Instruction fuel per instance. Default: `100000`.
    - `max_memory` UInt64 — Max memory usage per instance, in bytes. Range: 64 KiB … 4 GiB. Default: `104857600` (100 MiB).
    - `serialization_format` String — Serialization format for ABI requires it. Default: `MsgPack`.
    - `max_input_block_size` UInt64 — If specified, limits the maximum input block size in rows for ABI using block-based processing. Default: `0` (unlimited).
    - `max_instances` UInt64 — Maximum parallel instances per function in single query. Default: `128`.

## ABIs Versions

To interact with ClickHouse, WebAssembly modules must adhere to one of the supported ABIs (Application Binary Interfaces).

- `ROW_DIRECT`: Direct type mapping (primitive types `Int32`, `UInt32`, `Int64`, `UInt64`, `Float32`, `Float64` only)
- `BUFFERED_V1`: Complex types with serialization

### ABI ROW_DIRECT

Calls an exported WASM function directly per row.

- Arguments and return types as numeric types `Int32/UInt32/Int64/UInt64/Float32/Float64/Int128/UInt128`.
- Strings are not supported in this ABI.
- Signatures must match the WASM export (`i32/i64/f32/f64/v128`).
- No support functions required to be exported by the module.

For example function with signature:

```
(func (param i32 i64 f32) (result f64) ...)
```

Can be created as:

```sql
CREATE FUNCTION my_func ARGUMENTS (Int32, UInt64, Float32) RETURNS Float64 ...
```

WebAssembly does not distinguish between signed and unsigned arguments, but rather uses different instructions to interpret the values. Thus, size of the argument should match exactly, while signedness is determined by the operations inside the function.


### ABI BUFFERED_V1

:::note
This ABI is experimental and subject to change in future releases.
:::

Processes entire blocks at once using a (de)serialization through WASM memory. Supports any argument and return types.

Serialized data is copied to wasm memory passed as pointer to buffer (which consists of pointer to data and size of the data) to the UDF function along with the number of rows in the input. Thus, user-defined function on wasm time always accepts two `i32` arguments and returns single `i32` value.
Guest code processes the data and returns a pointer to the result buffer with serialized result data.

The guest code must provide two functions to create and destroy these buffers.

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

Example C definitions:

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
<!--

!!! TODO: crate is not yet published

### Note for developing UDFs in Rust

For Rust programs we provide a helper crate [clickhouse-wasm-udf](https://crates.io/crates/clickhouse-wasm-udf) to simplify development of WebAssembly UDFs for ClickHouse. The crate provides function for memory management, so you don't need to implement `clickhouse_create_buffer` and `clickhouse_destroy_buffer` functions manually, but rather add the crate as a dependency. Also there are macros `#[clickhouse_wasm_udf]` to wrap your regular Rust functions into the required ABI format.

With the crate you can write UDFs like this:


```rust

use clickhouse_wasm_udf_bindgen::clickhouse_udf;

#[clickhouse_udf]
pub fn some_udf(data: String) -> HashMap<String, String> {
    // Your implementation here
}

```

Macros will generate wrapper function accepting and returning buffer structures and handle serialization/deserialization automatically using `serde`.

-->

## Host API available to modules

The following host functions may be imported and used by modules:

- `clickhouse_server_version() -> i64` — returns ClickHouse server version as integer (e.g. 25011001 for v25.11.1.1).
- `clickhouse_terminate(ptr: i32, size: i32)` — throws an error with the provided message. Accepts pointer to the memory location containing the error message string and size of the string.
- `clickhouse_log(ptr: i32, size: i32)` — logs a message to ClickHouse server text log.
- `clickhouse_random(ptr: i32, size: i32)` — fills memory with random bytes.

## See also

- [ClickHouse UDF overview](/sql-reference/functions/udf)
