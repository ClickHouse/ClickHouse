#pragma once

#include <memory>
#include <optional>
#include <Interpreters/Context_fwd.h>


namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

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

FunctionOverloadResolverPtr createInternalCastOverloadResolver(CastType type, std::optional<CastDiagnostic> diagnostic);

}
