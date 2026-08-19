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

FunctionBasePtr createInternalCast(ColumnWithTypeAndName from, DataTypePtr to, CastType cast_type, std::optional<CastDiagnostic> diagnostic, ContextPtr context);

/// Whether CastType::accurateOrNull accepts this target. It rejects a target whose nested type cannot
/// be inside Nullable (Array, Map), so a caller that cannot let that exception escape must ask first.
bool canBeAccurateCastOrNullTarget(const DataTypePtr & type);

}
