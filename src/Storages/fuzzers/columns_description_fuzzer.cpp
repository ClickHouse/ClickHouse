#include <Functions/CastOverloadResolver.h>
#include <Storages/ColumnsDescription.h>
#include <iostream>


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
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Type conversions are not implemented for columns_description_fuzzer");
}

}


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        using namespace DB;
        ColumnsDescription columns = ColumnsDescription::parse(std::string(reinterpret_cast<const char *>(data), size));
        std::cerr << columns.toString() << "\n";
    }
    catch (...)
    {
    }

    return 0;
}
