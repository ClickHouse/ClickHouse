
#include <Interpreters/WebAssembly/HostApi.h>
#include <Interpreters/WebAssembly/WasmEngine.h>

#include <Common/Exception.h>
#include <Common/config_version.h>
#include <Common/logger_useful.h>


namespace DB::ErrorCodes
{
extern const int WASM_ERROR;
}

namespace DB::WebAssembly
{

namespace
{
/// API exported to guest WebAssembly code.

void wasmExportAlert(WasmCompartment * compartment, WasmPtr wasm_ptr, WasmSizeT size)
{
    const auto * host_ptr = compartment->getMemory(wasm_ptr, size);
    LOG_DEBUG(getLogger("WasmUdf"), "{}", std::string_view(reinterpret_cast<const char *>(host_ptr), size));
}

Int64 wasmExportServerVer(WasmCompartment *)
{
    return static_cast<UInt64>(VERSION_INTEGER);
}

[[noreturn]] void wasmExportTerminate(WasmCompartment * compartment, WasmPtr wasm_ptr, WasmSizeT size)
{
    const auto * host_ptr = compartment->getMemory(wasm_ptr, size);
    std::string_view data(reinterpret_cast<const char *>(host_ptr), size);
    throw Exception(ErrorCodes::WASM_ERROR, "WebAssembly UDF terminated with error: {}", data);
}
}

template <typename ReturnType, typename... Args>
class WasmHostFunctionImpl final : public WasmHostFunction
{
public:
    using HostFunctionPtr = ReturnType (*)(WasmCompartment *, Args...);

    explicit WasmHostFunctionImpl(std::string function_name_, HostFunctionPtr host_function_)
        : function_name(std::move(function_name_))
        , host_function(host_function_)
    {
    }

    std::optional<WasmVal> operator()(WasmCompartment * compartment, std::span<WasmVal> args) const override
    {
        if (args.size() != sizeof...(Args))
            throw Exception(
                ErrorCodes::WASM_ERROR,
                "WebAssembly function '{}' expects {} arguments, got {}",
                function_name,
                sizeof...(Args),
                args.size());
        if constexpr (!std::is_void_v<ReturnType>)
            return callFunctionImpl(compartment, args.data(), std::make_index_sequence<sizeof...(Args)>());
        callFunctionImpl(compartment, args.data(), std::make_index_sequence<sizeof...(Args)>());
        return std::nullopt;
    }

    std::string_view getName() const override { return function_name; }

    std::optional<WasmValKind> getReturnType() const override
    {
        if constexpr (std::is_void_v<ReturnType>)
            return std::nullopt;
        else
            return WasmValTypeToKind<ReturnType>::value;
    }

    std::vector<WasmValKind> getArgumentTypes() const override { return {WasmValTypeToKind<Args>::value...}; }

    ~WasmHostFunctionImpl() override = default;

private:
    template <size_t... is>
    ReturnType callFunctionImpl(WasmCompartment * compartment, const WasmVal * params, std::index_sequence<is...>) const
    {
        return host_function(compartment, std::get<Args>(params[is])...);
    }

    std::string function_name;
    HostFunctionPtr host_function;
};


template <typename ReturnType, typename... Args>
std::unique_ptr<WasmHostFunction> makeHostFunction(std::string_view function_name, ReturnType (*host_function)(WasmCompartment *, Args...))
{
    return std::make_unique<WasmHostFunctionImpl<ReturnType, Args...>>(std::string(function_name), host_function);
}

std::unique_ptr<WasmHostFunction> getHostFunction(std::string_view function_name)
{
    for (auto && function : std::array{
             makeHostFunction("clickhouse_server_version", wasmExportServerVer),
             makeHostFunction("clickhouse_terminate", wasmExportTerminate),
             makeHostFunction("clickhouse_log", wasmExportAlert),
         })
    {
        if (function->getName() == function_name)
            return std::move(function);
    }
    return nullptr;
}

}
