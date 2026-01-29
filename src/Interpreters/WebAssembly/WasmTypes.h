#pragma once

#include <variant>
#include <optional>
#include <vector>
#include <string>
#include <memory>

#include <base/extended_types.h>

namespace DB::WebAssembly
{

using WasmPtr = uint32_t;
using WasmSizeT = uint32_t;

using WasmVal = std::variant<uint32_t, int64_t, float, double, Int128>;

#define APPLY_FOR_WASM_TYPES(M) \
    M(I32)                      \
    M(I64)                      \
    M(F32)                      \
    M(F64)                      \
    M(V128)                     \

enum class WasmValKind : uint8_t
{
#define M(T) T,
    APPLY_FOR_WASM_TYPES(M)
#undef M
};


template <typename T, std::underlying_type_t<WasmValKind> index = 0>
struct WasmValTypeToIndex
{
    static_assert(index < std::variant_size_v<WasmVal>, "Type not found in WasmVal");
    using TVal = std::variant_alternative_t<index, WasmVal>;

    static constexpr std::underlying_type_t<WasmValKind> value = std::conditional_t<
        std::is_same_v<T, TVal> || (sizeof(T) == sizeof(TVal) && is_integer<T> && is_integer<TVal>),
        std::integral_constant<std::underlying_type_t<WasmValKind>, index>,
        WasmValTypeToIndex<T, index + 1>
    >::value;
};

template <typename T> struct WasmValTypeToKind { static constexpr WasmValKind value = static_cast<WasmValKind>(WasmValTypeToIndex<T>::value); };
template <typename T> struct NativeToWasmType { using Type = std::variant_alternative_t<WasmValTypeToIndex<T>::value, WasmVal>; };

WasmValKind getWasmValKind(const WasmVal & val);

class IWasmFunctionDeclaration
{
public:
    virtual std::vector<WasmValKind> getArgumentTypes() const = 0;
    virtual std::optional<WasmValKind> getReturnType() const = 0;
    virtual std::string_view getName() const = 0;

    IWasmFunctionDeclaration() = default;
    IWasmFunctionDeclaration(const IWasmFunctionDeclaration &) = delete;
    IWasmFunctionDeclaration & operator=(const IWasmFunctionDeclaration &) = delete;
    IWasmFunctionDeclaration(IWasmFunctionDeclaration &&) = default;
    IWasmFunctionDeclaration & operator=(IWasmFunctionDeclaration &&) = default;

    virtual ~IWasmFunctionDeclaration() = default;
};

String formatFunctionDeclaration(const IWasmFunctionDeclaration & wasm_func);
void checkFunctionDeclarationMatches(const IWasmFunctionDeclaration & actual, const IWasmFunctionDeclaration & expected);

class WasmFunctionDeclaration final : public IWasmFunctionDeclaration
{
public:
    WasmFunctionDeclaration(std::string_view function_name_, std::vector<WasmValKind> argument_types_, std::optional<WasmValKind> return_type_)
        : function_name(std::move(function_name_))
        , argument_types(std::move(argument_types_))
        , return_type(return_type_)
    {
    }

    std::string_view getName() const override { return function_name; }
    std::vector<WasmValKind> getArgumentTypes() const override { return argument_types; }
    std::optional<WasmValKind> getReturnType() const override { return return_type; }

private:
    std::string function_name;
    std::vector<WasmValKind> argument_types;
    std::optional<WasmValKind> return_type;
};

using WasmFunctionDeclarationPtr = std::unique_ptr<IWasmFunctionDeclaration>;

}

namespace DB
{
    String toString(WebAssembly::WasmValKind kind);
}
