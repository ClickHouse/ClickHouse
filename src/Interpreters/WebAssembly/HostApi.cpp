
#include <Interpreters/WebAssembly/HostApi.h>
#include <Interpreters/WebAssembly/WasmEngine.h>

#include <Common/Exception.h>
#include <Common/config_version.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>
#include <random>


namespace DB::ErrorCodes
{
    extern const int WASM_ERROR;
    extern const int RESOURCE_NOT_FOUND;
}

namespace DB::WebAssembly
{

namespace
{

std::string_view getWasmString(WasmCompartment * compartment, WasmPtr ptr, WasmSizeT size)
{
    auto data = compartment->getMemory(ptr, size);
    return {reinterpret_cast<const char *>(data.data()), data.size()};
}

void wasmExportLog(WasmCompartment * compartment, UInt32 level, WasmPtr wasm_ptr, WasmSizeT size)
{
    /// Map level to Poco::Message::Priority.
    /// Poco priorities: 1 (FATAL) .. 8 (TRACE). WASM modules must not emit messages
    /// more severe than WARNING — those could trigger alerting systems and misrepresent
    /// ClickHouse health. Clamp as integers first to avoid signed overflow on large
    /// UInt32 values (e.g. 0xFFFFFFFF → -1 if cast first), then cast.
    UInt32 clamped_level = std::clamp(level,
        static_cast<UInt32>(Poco::Message::PRIO_WARNING),
        static_cast<UInt32>(Poco::Message::PRIO_TRACE));
    auto prio = static_cast<Poco::Message::Priority>(clamped_level);

    std::string_view message = getWasmString(compartment, wasm_ptr, size);

    auto logger = getLogger("WasmUdf");
    logger->log(Poco::Message(logger->name(), std::string(message), prio));
}

Int64 wasmExportServerVer(WasmCompartment *)
{
    return static_cast<UInt64>(VERSION_INTEGER);
}

[[noreturn]] void wasmExportThrow(WasmCompartment * compartment, WasmPtr wasm_ptr, WasmSizeT size)
{
    auto data = compartment->getMemory(wasm_ptr, size);
    std::string_view data_view(reinterpret_cast<const char *>(data.data()), data.size());
    throw Exception(ErrorCodes::WASM_ERROR, "WebAssembly UDF terminated with error: {}", data_view);
}

void wasmExportRandom(WasmCompartment * compartment, WasmPtr wasm_ptr, WasmSizeT size)
{
    auto data = compartment->getMemory(wasm_ptr, size);
    using ValueType = decltype(data)::value_type;
    std::uniform_int_distribution<> dist(std::numeric_limits<ValueType>::min(), std::numeric_limits<ValueType>::max());
    for (WasmSizeT i = 0; i < size; ++i)
        data[i] = static_cast<ValueType>(dist(thread_local_rng));
}

/// Decode an AssemblyScript string (UTF-16, length stored in the runtime header at -4
/// from the payload pointer) into a UTF-8 std::string suitable for inclusion in error messages.
/// Returns "<null>" for null pointers and tolerates broken / out-of-bounds pointers without throwing.
std::string decodeAssemblyScriptString(WasmCompartment * compartment, WasmPtr ptr)
{
    if (ptr == 0)
        return "";

    constexpr WasmSizeT header_size = 4; // rtSize is u32 at -4
    if (ptr < header_size)
        throw Exception(ErrorCodes::WASM_ERROR, "Illegal AssemblyScript string pointer {}", ptr);

    auto header = compartment->getMemory(ptr - header_size, header_size);
    uint32_t rt_size = loadFromWasmMemory<uint32_t>(header.data());

    /// rtSize is the byte length; AS strings are UTF-16, so length is rt_size / 2.
    if (rt_size == 0 || rt_size % 2 != 0 || rt_size > (1u << 24))
        return "";

    std::span<uint8_t> data = compartment->getMemory(ptr, rt_size);

    std::string out;
    out.reserve(rt_size);
    for (WasmSizeT i = 0; i + 1 < data.size(); i += 2)
    {
        uint32_t code_unit = static_cast<uint32_t>(data[i]) | (static_cast<uint32_t>(data[i + 1]) << 8);
        /// Naive transcoding: emit ASCII as-is, replace anything else with '?'.
        /// Good enough for human-readable error messages from AS abort() calls.
        if (code_unit < 0x80)
            out.push_back(static_cast<char>(code_unit));
        else
            out.push_back('?');
    }
    return out;
}

/// AssemblyScript runtime calls `env.abort(message, fileName, lineNumber, columnNumber)`
/// from generated trap code (e.g. on out-of-memory, bounds checks, runtime errors).
/// The two pointers refer to AS String objects (UTF-16, length in object header).
[[noreturn]] void wasmExportAssemblyScriptAbort(
    WasmCompartment * compartment,
    WasmPtr message_ptr,
    WasmPtr file_name_ptr,
    UInt32 line_number,
    UInt32 column_number)
{
    auto message = decodeAssemblyScriptString(compartment, message_ptr);
    auto file_name = decodeAssemblyScriptString(compartment, file_name_ptr);
    throw Exception(ErrorCodes::WASM_ERROR,
        "AssemblyScript module aborted: {} (at {}:{}:{})",
        message, file_name, line_number, column_number);
}

}

template <typename>
class WasmHostFunctionAdapter;

template <typename ReturnType, typename... Args>
class WasmHostFunctionAdapter<ReturnType (*)(WasmCompartment *, Args...)>
{
public:
    using HostFunctionPtr = ReturnType (*)(WasmCompartment *, Args...);

    explicit WasmHostFunctionAdapter(HostFunctionPtr host_function_, std::string_view function_name_)
        : host_function(host_function_), function_name(function_name_)
    {
    }

    std::optional<WasmVal> operator()(WasmCompartment * compartment, std::span<WasmVal> args) const
    {
        if (args.size() != sizeof...(Args))
            throw Exception(ErrorCodes::WASM_ERROR,
                "WebAssembly function '{}' expects {} arguments, got {}",
                function_name, sizeof...(Args), args.size());
        if constexpr (!std::is_void_v<ReturnType>)
            return callFunctionImpl(compartment, args.data(), std::make_index_sequence<sizeof...(Args)>());
        callFunctionImpl(compartment, args.data(), std::make_index_sequence<sizeof...(Args)>());
        return std::nullopt;
    }

    static std::optional<WasmValKind> getReturnType()
    {
        if constexpr (std::is_void_v<ReturnType>)
            return std::nullopt;
        else
            return WasmValTypeToKind<ReturnType>::value;
    }

    static std::vector<WasmValKind> getArgumentTypes() { return {WasmValTypeToKind<Args>::value...}; }

private:
    template <size_t... is>
    ReturnType callFunctionImpl(WasmCompartment * compartment, const WasmVal * params, std::index_sequence<is...>) const
    {
        return host_function(compartment, std::get<Args>(params[is])...);
    }

    HostFunctionPtr host_function;
    std::string_view function_name;
};

template <typename FuncPtr>
std::optional<WasmVal> invokeImpl(void * ptr, std::string_view function_name, WasmCompartment * c, std::span<WasmVal> args)
{
    return WasmHostFunctionAdapter<FuncPtr>(reinterpret_cast<FuncPtr>(ptr), function_name)(c, args);
}

template <typename ReturnType, typename... Args>
WasmHostFunction makeHostFunction(std::string_view function_name, ReturnType (*host_function)(WasmCompartment *, Args...))
{
    using FuncPtr = ReturnType (*)(WasmCompartment *, Args...);
    WasmFunctionDeclaration func_decl(
        "env",
        function_name,
        WasmHostFunctionAdapter<FuncPtr>::getArgumentTypes(),
        WasmHostFunctionAdapter<FuncPtr>::getReturnType());

    return WasmHostFunction(std::move(func_decl), reinterpret_cast<void *>(host_function), &invokeImpl<FuncPtr>);
}

/// API exported to guest WebAssembly code.
WasmHostFunction getHostFunction(std::string_view function_name)
{
    static const std::array exported_functions{
        makeHostFunction("clickhouse_server_version", wasmExportServerVer),
        makeHostFunction("clickhouse_throw", wasmExportThrow),
        makeHostFunction("clickhouse_log", wasmExportLog),
        makeHostFunction("clickhouse_random", wasmExportRandom),
        /// Mandatory import for any AssemblyScript module that uses runtime checks
        /// (memory allocation, bounds checks, etc). Linked only when the module
        /// declares `(import "env" "abort" (func ...))`, so non-AS modules are unaffected.
        makeHostFunction("abort", wasmExportAssemblyScriptAbort),
    };

    for (const auto & function : exported_functions)
    {
        if (function.getFunctionDeclaration().getName() == function_name)
            return function;
    }

    throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "Unknown WebAssembly host function '{}'", function_name);
}

}
