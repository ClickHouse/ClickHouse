#include <Interpreters/WebAssembly/WasmEngine.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

namespace DB::ErrorCodes
{
extern const int WASM_ERROR;
}


namespace ProfileEvents
{
extern const Event WasmModuleInstatiate;
}

namespace DB::WebAssembly
{


WasmCompartment::WasmCompartment()
{
    ProfileEvents::increment(ProfileEvents::WasmModuleInstatiate);
}

template <typename ResultType>
ResultType WasmCompartment::invoke(std::string_view function_name, const std::vector<WasmVal> & params)
{
    auto returns = invokeImpl(function_name, params);

    if constexpr (std::is_same_v<ResultType, void>)
    {
        if (returns.empty())
            return;
        throw Exception(ErrorCodes::WASM_ERROR, "Unexpected return value from wasm function '{}', expected void", function_name);
    }
    else
    {
        if (returns.size() != 1)
        {
            throw Exception(ErrorCodes::WASM_ERROR,
                "Unexpected return values count from wasm function '{}', got {} values, expected single value",
                function_name, returns.size());
        }

        if (std::holds_alternative<ResultType>(returns[0]))
            return std::get<ResultType>(returns[0]);

        throw Exception(ErrorCodes::WASM_ERROR,
            "Unexpected return value from wasm function '{}', expected: {}, got {}",
            function_name, WasmValTypeToKind<ResultType>::value, getWasmValKind(returns[0]));
    }
}

template void WasmCompartment::invoke<>(std::string_view, const std::vector<WasmVal> &);

#define M(T) template std::variant_alternative_t<std::to_underlying(WasmValKind::T), WasmVal> WasmCompartment::invoke<>(std::string_view, const std::vector<WasmVal> &);
APPLY_FOR_WASM_TYPES(M)
#undef M


}
