
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
/// API exported to guest WebAssembly code.

void wasmExportLog(WasmCompartment * compartment, WasmPtr wasm_ptr, WasmSizeT size)
{
    const auto * host_ptr = compartment->getMemory(wasm_ptr, size);
    LOG_TRACE(getLogger("WasmUdf"), "{}", std::string_view(reinterpret_cast<const char *>(host_ptr), size));
}

Int64 wasmExportServerVer(WasmCompartment *)
{
    return static_cast<UInt64>(VERSION_INTEGER);
}

[[noreturn]] void wasmExportThrow(WasmCompartment * compartment, WasmPtr wasm_ptr, WasmSizeT size)
{
    const auto * host_ptr = compartment->getMemory(wasm_ptr, size);
    std::string_view data(reinterpret_cast<const char *>(host_ptr), size);
    throw Exception(ErrorCodes::WASM_ERROR, "WebAssembly UDF terminated with error: {}", data);
}

void wasmExportRandom(WasmCompartment * compartment, WasmPtr wasm_ptr, WasmSizeT size)
{
    auto * host_ptr = compartment->getMemory(wasm_ptr, size);
    std::uniform_int_distribution<> dist(std::numeric_limits<UInt8>::min(), std::numeric_limits<UInt8>::max());
    for (WasmSizeT i = 0; i < size; ++i)
        host_ptr[i] = static_cast<UInt8>(dist(thread_local_rng));
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
        function_name,
        WasmHostFunctionAdapter<FuncPtr>::getArgumentTypes(),
        WasmHostFunctionAdapter<FuncPtr>::getReturnType());

    return WasmHostFunction(std::move(func_decl), reinterpret_cast<void *>(host_function), &invokeImpl<FuncPtr>);
}

WasmHostFunction getHostFunction(std::string_view function_name)
{
    static const std::array exported_functions{
        makeHostFunction("clickhouse_server_version", wasmExportServerVer),
        makeHostFunction("clickhouse_throw", wasmExportThrow),
        makeHostFunction("clickhouse_log", wasmExportLog),
        makeHostFunction("clickhouse_random", wasmExportRandom),
    };

    for (const auto & function : exported_functions)
    {
        if (function.getFunctionDeclaration().getName() == function_name)
            return function;
    }

    throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "Unknown WebAssembly host function '{}'", function_name);
}

}
