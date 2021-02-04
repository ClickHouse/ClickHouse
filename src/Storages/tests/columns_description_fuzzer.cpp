#include <Storages/ColumnsDescription.h>


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
try
{
    using namespace DB;
    ColumnsDescription columns = ColumnsDescription::parse(std::string(reinterpret_cast<const char *>(data), size));
    std::cerr << columns.toString() << "\n";
    return 0;
}
catch (...)
{
    return 1;
}
