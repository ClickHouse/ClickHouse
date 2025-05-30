#include <Interpreters/WebAssembly/WasmTypes.h>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <Common/Exception.h>


namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB::WebAssembly
{

WasmValKind getWasmValKind(const WasmVal & val)
{
    return std::visit([](auto && arg) -> WasmValKind
    {
        using T = std::decay_t<decltype(arg)>;
        return WasmValTypeToKind<T>::value;
    }, val);
}

String formatFunctionDeclaration(const IWasmFunctionDeclaration & wasm_func)
{
    auto result_type = wasm_func.getReturnType();
    return fmt::format("{}({}) -> {}",
        wasm_func.getName(),
        fmt::join(wasm_func.getArgumentTypes(), ", "),
        result_type ? toString(*result_type) : "void");
}

void checkFunctionDeclarationMatches(const IWasmFunctionDeclaration & actual, const IWasmFunctionDeclaration & expected)
{
    if (actual.getName() != expected.getName())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected function name: '{}', expected '{}'", actual.getName(), expected.getName());

    if (actual.getArgumentTypes() != expected.getArgumentTypes())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function have unexpected argument types: {}, expected {}",
            formatFunctionDeclaration(actual), formatFunctionDeclaration(expected));

    if (actual.getReturnType() != expected.getReturnType())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function have unexpected return type: {}, expected {}",
            formatFunctionDeclaration(actual), formatFunctionDeclaration(expected));
}

}

namespace DB
{

String toString(WebAssembly::WasmValKind kind)
{
    switch (kind)
    {
        case WebAssembly::WasmValKind::I32: return "I32";
        case WebAssembly::WasmValKind::I64: return "I64";
        case WebAssembly::WasmValKind::F32: return "F32";
        case WebAssembly::WasmValKind::F64: return "F64";
    }
    __builtin_unreachable();
}

}
