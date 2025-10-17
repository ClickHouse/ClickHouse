#pragma once

#include <Interpreters/WebAssembly/WasmTypes.h>

namespace DB::ErrorCodes
{
    extern const int WASM_ERROR;
}

namespace DB::WebAssembly
{

/// Functions below is basically compile time switch-case for WasmValKind,
/// so we don't need to maintain it in sync with enum options manually in in each function.
template <template <WasmValKind> typename Trait, std::underlying_type_t<WasmValKind> index = 0>
inline WasmValKind fromImplValueType(auto val_type)
{
    if constexpr (index < std::variant_size_v<WasmVal>)
    {
        constexpr auto kind = static_cast<WasmValKind>(index);
        if (Trait<kind>::is(val_type))
            return kind;
        return fromImplValueType<Trait, index + 1>(val_type);
    }
    throw Exception(ErrorCodes::WASM_ERROR, "Unsupported wasm implementation type");
}

template <template <WasmValKind> typename Trait, std::underlying_type_t<WasmValKind> index = 0>
WasmVal fromWasmImplValue(auto val, auto val_type)
{
    if constexpr (index < std::variant_size_v<WasmVal>)
    {
        constexpr auto kind = static_cast<WasmValKind>(index);
        if (Trait<kind>::is(val_type))
        {
            using WasmType = std::variant_alternative_t<index, WasmVal>;
            return std::bit_cast<WasmType>(Trait<kind>::from(val));
        }
        return fromWasmImplValue<Trait, index + 1>(val, val_type);
    }
    throw Exception(ErrorCodes::WASM_ERROR, "Unsupported wasm implementation type");
}

template <template <WasmValKind> typename Trait, std::underlying_type_t<WasmValKind> index = 0>
inline auto toWasmImplValueType(WasmValKind k)
{
    static_assert(index < std::variant_size_v<WasmVal>, "Index out of bounds");

    constexpr auto kind = static_cast<WasmValKind>(index);
    if (kind == k)
        return Trait<kind>::type();

    if constexpr (index + 1 < std::variant_size_v<WasmVal>)
        return toWasmImplValueType<Trait, index + 1>(k);
    else
        throw Exception(ErrorCodes::WASM_ERROR, "Unsupported wasm edge type");
}


template <template <WasmValKind> typename Trait>
auto toWasmImplValue(WasmVal val)
{
    return std::visit([](auto arg)
    {
        using T = std::decay_t<decltype(arg)>;
        return Trait<WasmValTypeToKind<T>::value>::to(arg);
    }, val);
}

}
