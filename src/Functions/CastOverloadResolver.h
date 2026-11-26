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

/// Internal, for the input format readers: named tuples are matched by name even when they have no element name in common.
enum class TupleElementsMatching : uint8_t
{
    Default,
    ByName,
    /// As `DataTypeTuple::matchElementsByName` with `case_insensitive`.
    ByNameCaseInsensitive,
};

/// Internal, for the input format readers: a cast without a context takes these from the format settings.
struct InternalCastSettings
{
    TupleElementsMatching tuple_elements_matching = TupleElementsMatching::Default;
    bool check_conversion_from_numbers_to_enum = false;
};

struct CastDiagnostic
{
    std::string column_from;
    std::string column_to;
};

FunctionBasePtr createInternalCast(ColumnWithTypeAndName from, DataTypePtr to, CastType cast_type, std::optional<CastDiagnostic> diagnostic,
    ContextPtr context, const InternalCastSettings & internal_settings = {});

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
/// into a column whose tuple elements are named differently). The input format readers request matching by
/// name in this case too (`matching`), as a file is read by its schema.
VectorWithMemoryTracking<std::optional<size_t>> getTupleCastElementPositions(
    const DataTypeTuple & from, const DataTypeTuple & to, TupleElementsMatching matching = TupleElementsMatching::Default);

/// The same mapping, or no mapping when the tuples are matched by position and have different numbers of
/// elements, where `getTupleCastElementPositions` throws `TYPE_MISMATCH` because `CAST` cannot convert them.
std::optional<VectorWithMemoryTracking<std::optional<size_t>>> tryGetTupleCastElementPositions(
    const DataTypeTuple & from, const DataTypeTuple & to, TupleElementsMatching matching = TupleElementsMatching::Default);

FunctionOverloadResolverPtr createCastOverloadResolver(ContextPtr context, CastType cast_type, std::optional<CastDiagnostic> diagnostic);

}
