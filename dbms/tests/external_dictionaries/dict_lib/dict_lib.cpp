#include <iostream>
#include <vector>
#include <cstdint>

struct ClickhouseVectorUint64 {
    const uint64_t size;
    const uint64_t * data;
};
using ClickhouseColumn = const char*;
using ClickhouseColumns = ClickhouseColumn[];

extern "C" void loadIds(void * data_ptr, const struct ClickhouseVectorUint64 ids)
{
    std::cerr << "loadIds Runned!!!="<<ids.size<<"\n";
    return;
}

extern "C" void loadAll()
{
    std::cerr << "loadAll Runned!!!"<<"\n";
    return;
}

extern "C" void loadKeys(ClickhouseColumns columns, const struct ClickhouseVectorUint64 requested_rows)
{
    std::cerr << "loadKeys Runned!!!="<<requested_rows.size<<"\n";
    size_t i = 0;
    //auto column = columns[i];
    ClickhouseColumn column;
    while ((column = columns[i++])) {
        std::cerr << "column i=" << i << " = [" << column << "] p=" << (size_t)column << "\n";
    }
    return;
}

extern "C" void * dataAllocate()
{
    int size = 100;
    auto data_ptr = ::operator new(size);
    std::cerr <<  "dataAllocate Runned!!! ptr=" << data_ptr << "\n";
    return data_ptr;
}

extern "C" void dataDelete(void * data_ptr)
{
    std::cerr << "dataDelete Runned!!! ptr=" << data_ptr << "\n";
    //delete ptr;
    return;
}
