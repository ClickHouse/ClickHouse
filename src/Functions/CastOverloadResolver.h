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

/// With nulls_from_source_only, a Nullable `to` holds only the NULLs of `from`: any other value converts as for the non-Nullable type.
FunctionBasePtr createInternalCast(
    ColumnWithTypeAndName from,
    DataTypePtr to,
    CastType cast_type,
    std::optional<CastDiagnostic> diagnostic,
    ContextPtr context,
    bool nulls_from_source_only = false);

/// Whether CastType::accurateOrNull accepts this target. Failure is reported by wrapping the target in
/// Nullable, so the target itself must be able to be inside Nullable; a nested type is also accepted
/// when it can carry a NULL of its own, as Dynamic and Variant do.
bool canBeAccurateCastOrNullTarget(const DataTypePtr & type);

FunctionOverloadResolverPtr createCastOverloadResolver(ContextPtr context, CastType cast_type, std::optional<CastDiagnostic> diagnostic);

}
