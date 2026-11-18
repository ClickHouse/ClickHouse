#pragma once

#include <memory>
#include <optional>
#include <Interpreters/Context_fwd.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

class DataTypeTuple;
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

/// The same for a cast or another function that captures the setting `cast_fixed_string_to_string_strip_trailing_zeros`
/// (see `IFunctionWithFixedStringToStringMode`), or nullopt if the result of `function` does not depend on it.
std::optional<bool> getFixedStringToStringStripTrailingZeros(const IFunctionBase & function);

/// The same for the functions that `resolver` builds: a resolver that implements `IFunctionWithFixedStringToStringMode`
/// itself (such as one delegating to a function created from the context), or the generic adaptor of such a function.
std::optional<bool> getFixedStringToStringStripTrailingZeros(const IFunctionOverloadResolver & resolver);

/// Whether CastType::accurateOrNull accepts this target. Failure is reported by wrapping the target in
/// Nullable, so the target itself must be able to be inside Nullable; a nested type is also accepted
/// when it can carry a NULL of its own, as Dynamic and Variant do.
bool canBeAccurateCastOrNullTarget(const DataTypePtr & type);

/// Map each destination tuple element to its source position, or no position for a defaulted field. For named
/// tuples with at least one element name in common, elements are matched by name: source elements without a
/// counterpart are dropped, and target elements without a counterpart are filled by default values (schema
/// evolution: adding, dropping and renaming elements of a `Tuple` or a `Nested` column with `ALTER`). For
/// named tuples with disjoint sets of element names, matching by name cannot be meant, because it would fill
/// the whole result by default values and lose all the data, so elements are matched by position, as for
/// unnamed tuples (e.g. the result of function `tuple` with `enable_named_columns_in_function_tuple` inserted
/// into a column whose tuple elements are named differently).
VectorWithMemoryTracking<std::optional<size_t>> getTupleCastElementPositions(const DataTypeTuple & from, const DataTypeTuple & to);

/// The same mapping, or no mapping when the tuples are matched by position and have different numbers of
/// elements, where `getTupleCastElementPositions` throws `TYPE_MISMATCH` because `CAST` cannot convert them.
std::optional<VectorWithMemoryTracking<std::optional<size_t>>> tryGetTupleCastElementPositions(
    const DataTypeTuple & from, const DataTypeTuple & to);

FunctionOverloadResolverPtr createCastOverloadResolver(ContextPtr context, CastType cast_type, std::optional<CastDiagnostic> diagnostic);

}
