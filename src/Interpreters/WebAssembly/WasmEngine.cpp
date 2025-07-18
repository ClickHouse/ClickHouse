#include <Interpreters/WebAssembly/WasmEngine.h>
#include <Common/Exception.h>


namespace DB::ErrorCodes
{
extern const int WASM_ERROR;
}

namespace DB::WebAssembly
{

template <typename ResultType>
ResultType WasmCompartment::invoke(std::string_view function_name, const std::vector<WasmVal> & params)
{
    std::vector<WasmVal> returns;
    invoke(function_name, params, returns);

    if constexpr (std::is_same_v<ResultType, void>)
    {
        if (returns.empty())
            return;
        throw Exception(ErrorCodes::WASM_ERROR, "Unexpected return value from wasm function '{}', expected void", function_name);
    }
    else if constexpr (std::is_same_v<ResultType, std::optional<WasmVal>>)
    {
        if (returns.empty())
            return {};
        if (returns.size() == 1)
            return returns[0];
        throw Exception(ErrorCodes::WASM_ERROR, "Unexpected return value from wasm function '{}', expected single value", function_name);
    }
    else if constexpr (std::is_same_v<ResultType, WasmVal>)
    {
        if (returns.size() == 1)
            return returns[0];
        throw Exception(ErrorCodes::WASM_ERROR, "Unexpected return value from wasm function '{}', expected single value", function_name);
    }
    else
    {
        if (returns.size() != 1)
        {
            throw Exception(
                ErrorCodes::WASM_ERROR,
                "Unexpected return values count from wasm function '{}', got {} values, expected single value",
                function_name,
                returns.size());
        }
        if (std::holds_alternative<ResultType>(returns[0]))
            return std::get<ResultType>(returns[0]);

        throw Exception(
            ErrorCodes::WASM_ERROR,
            "Unexpected return value from wasm function '{}', expected: {}, got {}",
            function_name,
            WasmValTypeToKind<ResultType>::value,
            getWasmValKind(returns[0]));
    }
}

template void WasmCompartment::invoke<>(std::string_view, const std::vector<WasmVal> &);
template WasmVal WasmCompartment::invoke<>(std::string_view, const std::vector<WasmVal> &);
template std::optional<WasmVal> WasmCompartment::invoke<>(std::string_view, const std::vector<WasmVal> &);
template std::variant_alternative_t<0, WasmVal> WasmCompartment::invoke<>(std::string_view, const std::vector<WasmVal> &);
template std::variant_alternative_t<1, WasmVal> WasmCompartment::invoke<>(std::string_view, const std::vector<WasmVal> &);
template std::variant_alternative_t<2, WasmVal> WasmCompartment::invoke<>(std::string_view, const std::vector<WasmVal> &);
template std::variant_alternative_t<3, WasmVal> WasmCompartment::invoke<>(std::string_view, const std::vector<WasmVal> &);

}
