#include <Functions/IFunction.h>
#include <Functions/CastOverloadResolver.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

FunctionBasePtr createFunctionBaseCast(
    ContextPtr, const ColumnsWithTypeAndName &, const DataTypePtr &, std::optional<CastDiagnostic>, CastType)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Type conversions are not implemented for ODBC Bridge");
}

}
