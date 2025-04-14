#include <memory>
#include <Functions/CastOverloadResolver.h>
#include <Core/ColumnsWithTypeAndName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class IFunctionBase;
using FunctionBasePtr = std::shared_ptr<const IFunctionBase>;

FunctionBasePtr createFunctionBaseCast(
    ContextPtr, const char *, const ColumnsWithTypeAndName &, const DataTypePtr &, std::optional<CastDiagnostic>, CastType)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Type conversions are not implemented for Library Bridge");
}

}
