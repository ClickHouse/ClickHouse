#pragma once

#include <Functions/UserDefined/UserDefinedWebAssembly.h>

namespace DB
{

/// Construct the AssemblyScript-flavoured ABI implementation for a WebAssembly UDF.
///
/// The resulting `UserDefinedWebAssemblyFunction` invokes the user's exported function once
/// per row, mapping ClickHouse `String` values to AssemblyScript `String` objects (rtId = 2,
/// UTF-16 payload, 20-byte object header) via the runtime exports `__new` / `__pin` / `__unpin`,
/// and ClickHouse numeric types to AssemblyScript primitives (i32 / i64 / f32 / f64) the same
/// way the simple ABI does. Custom AssemblyScript classes are intentionally out of scope —
/// their runtime class ids are not stable across builds, see
/// https://github.com/AssemblyScript/assemblyscript/issues/2982.
std::unique_ptr<UserDefinedWebAssemblyFunction> createUserDefinedWebAssemblyFunctionAssemblyScript(
    std::shared_ptr<WebAssembly::WasmModule> wasm_module,
    const String & function_name,
    const Strings & argument_names,
    const DataTypes & arguments,
    const DataTypePtr & result_type,
    WebAssemblyFunctionSettings function_settings,
    bool is_deterministic);

}
