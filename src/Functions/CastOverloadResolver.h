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

FunctionBasePtr createInternalCast(ColumnWithTypeAndName from, DataTypePtr to, CastType cast_type, std::optional<CastDiagnostic> diagnostic, ContextPtr context);

/// Whether CastType::accurateOrNull accepts this target. Failure is reported by wrapping the target in
/// Nullable, so the target itself must be able to be inside Nullable; a nested type is also accepted
/// when it can carry a NULL of its own, as Dynamic and Variant do.
bool canBeAccurateCastOrNullTarget(const DataTypePtr & type);

/// Map each destination tuple element to its source position, or no position for a defaulted field.
/// Named tuples with common names are matched by name; other tuples are matched by position.
VectorWithMemoryTracking<std::optional<size_t>> getTupleCastElementPositions(const DataTypeTuple & from, const DataTypeTuple & to);

FunctionOverloadResolverPtr createCastOverloadResolver(ContextPtr context, CastType cast_type, std::optional<CastDiagnostic> diagnostic);

}
