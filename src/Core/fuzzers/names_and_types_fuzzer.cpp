#include <Core/ColumnsWithTypeAndName.h>
#include <Functions/CastOverloadResolver.h>
#include <Core/NamesAndTypes.h>
#include <IO/ReadBufferFromMemory.h>


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


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        DB::ReadBufferFromMemory in(data, size);
        DB::NamesAndTypesList res;
        res.readText(in);
    }
    catch (...)
    {
    }

    return 0;
}
