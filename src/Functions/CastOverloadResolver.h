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

/// Internal, set only by `castColumnToRequestedType` for the input format readers: named tuples are matched by name
/// even when they have no element name in common.
enum class TupleElementsMatching : uint8_t
{
    Default,
    ByName,
    /// As `DataTypeTuple::matchElementsByName` with `case_insensitive`.
    ByNameCaseInsensitive,
};

struct CastDiagnostic
{
    std::string column_from;
    std::string column_to;
};

FunctionBasePtr createInternalCast(ColumnWithTypeAndName from, DataTypePtr to, CastType cast_type, std::optional<CastDiagnostic> diagnostic,
    ContextPtr context, TupleElementsMatching tuple_elements_matching = TupleElementsMatching::Default);

/// Whether CastType::accurateOrNull accepts this target. Failure is reported by wrapping the target in
/// Nullable, so the target itself must be able to be inside Nullable; a nested type is also accepted
/// when it can carry a NULL of its own, as Dynamic and Variant do.
bool canBeAccurateCastOrNullTarget(const DataTypePtr & type);

FunctionOverloadResolverPtr createCastOverloadResolver(ContextPtr context, CastType cast_type, std::optional<CastDiagnostic> diagnostic);

}
