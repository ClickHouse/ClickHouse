#pragma once

#include <memory>
#include <optional>
#include <Interpreters/Context_fwd.h>
#include <Core/ColumnWithTypeAndName.h>


namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<const IFunctionBase>;

enum class CastType : uint8_t
{
    nonAccurate,
    accurate,
    accurateOrNull
};

struct CastDiagnostic
{
    std::string column_from;
    std::string column_to;
};

/// If `fixed_string_to_string_strip_trailing_zeros_override` is set, it takes the place of the setting
/// `cast_fixed_string_to_string_strip_trailing_zeros`, which is taken from `context` otherwise (and is false without it).
FunctionBasePtr createInternalCast(
    ColumnWithTypeAndName from,
    DataTypePtr to,
    CastType cast_type,
    std::optional<CastDiagnostic> diagnostic,
    ContextPtr context,
    std::optional<bool> fixed_string_to_string_strip_trailing_zeros_override = {});

/// Whether the cast `function` removes the trailing zero bytes of a `FixedString` converted to `String`,
/// or nullopt if `function` is not a cast.
std::optional<bool> getCastFixedStringToStringStripTrailingZeros(const IFunctionBase & function);

/// Whether CastType::accurateOrNull accepts this target. Failure is reported by wrapping the target in
/// Nullable, so the target itself must be able to be inside Nullable; a nested type is also accepted
/// when it can carry a NULL of its own, as Dynamic and Variant do.
bool canBeAccurateCastOrNullTarget(const DataTypePtr & type);

FunctionOverloadResolverPtr createCastOverloadResolver(ContextPtr context, CastType cast_type, std::optional<CastDiagnostic> diagnostic);

}
