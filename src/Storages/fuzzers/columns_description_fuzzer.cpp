#include <Storages/ColumnsDescription.h>
#include <iostream>

#include <iostream>


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
