#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/WebAssembly/WasmTypes.h>

namespace DB::WebAssembly
{

/// Maps ClickHouse numeric types to their WASM storage type.
/// Small integer types (Int8, UInt8, Int16, UInt16) are widened to uint32_t (i32).
/// All other supported types map 1:1 via NativeToWasmType.
template <typename T>
struct WasmStorageType
{
    using Type = typename NativeToWasmType<T>::Type;
};

template <> struct WasmStorageType<Int8>   { using Type = uint32_t; };
template <> struct WasmStorageType<UInt8>  { using Type = uint32_t; };
template <> struct WasmStorageType<Int16>  { using Type = uint32_t; };
template <> struct WasmStorageType<UInt16> { using Type = uint32_t; };

template <typename T>
constexpr WasmValKind wasmKindFor()
{
    return WasmValTypeToKind<typename WasmStorageType<T>::Type>::value;
}

/// Iterate over the numeric types we accept on the WASM ABI boundary, calling
/// `callable.template operator()<T>(args...)` for each. Stops on the first call that
/// returns true and propagates that.
template <typename Callable, typename... Args>
inline bool tryExecuteForNumericTypes(Callable && callable, Args &&... args)
{
    return (
        callable.template operator()<Int8>(args...)
        || callable.template operator()<UInt8>(args...)
        || callable.template operator()<Int16>(args...)
        || callable.template operator()<UInt16>(args...)
        || callable.template operator()<Int32>(args...)
        || callable.template operator()<UInt32>(args...)
        || callable.template operator()<Int64>(args...)
        || callable.template operator()<UInt64>(args...)
        || callable.template operator()<Float32>(args...)
        || callable.template operator()<Float64>(args...)
        || callable.template operator()<Int128>(args...)
        || callable.template operator()<UInt128>(args...)
    );
}

std::optional<WasmValKind> wasmKindForDataType(const IDataType * type);

/// Returns true when `from` can be implicitly coerced to `to`.
/// Allowed: same kind; i32→i64; any int→any float; f32→f64.
bool canCoerce(WasmValKind from, WasmValKind to);


}
