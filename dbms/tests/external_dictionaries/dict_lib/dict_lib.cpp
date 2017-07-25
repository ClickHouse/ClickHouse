#include <cstdint>
#include <iostream>
#include <vector>
#include <memory>

struct ClickhouseVectorUint64
{
    const uint64_t size;
    const uint64_t * data;
};
using ClickhouseColumn = const char *;
using ClickhouseColumns = ClickhouseColumn[];

struct DataHolder
{
    std::shared_ptr<std::vector<uint64_t>> vectorPtr;
};

extern "C" void loadIds(void * data_ptr, const struct ClickhouseVectorUint64 ids)
{
    auto ptr = static_cast<DataHolder*>(data_ptr);
    std::cerr << "loadIds Runned!!! ptr=" << data_ptr << " => " << ptr << " size=" << ids.size << "\n";
    if (ptr) {}
    return;
}

extern "C" void loadAll(void * data_ptr)
{
    auto ptr = static_cast<DataHolder*>(data_ptr);
    std::cerr << "loadAll Runned!!! ptr=" << data_ptr << " => " << ptr << "\n";
    if (ptr) {}
    return;
}

/*
extern "C" void loadKeys(void * data_ptr, ClickhouseColumns columns, const struct ClickhouseVectorUint64 requested_rows)
{
    std::cerr << "loadKeys Runned!!! ptr=" << data_ptr << " size=" << requested_rows.size << "\n";
    size_t i = 0;
    //auto column = columns[i];
    ClickhouseColumn column;
    while ((column = columns[i++]))
    {
        std::cerr << "column i=" << i << " = [" << column << "] p=" << (size_t)column << "\n";
    }
    return;
}
*/

extern "C" void * dataAllocate()
{
    //int size = 100;
    //auto data_ptr = ::operator new(size);
    auto data_ptr = new DataHolder;

    //auto ptr = static_cast<DataHolder*>(data_ptr);

    std::cerr << "dataAllocate Runned!!! ptr=" << data_ptr << "\n";
    return data_ptr;
}

extern "C" void dataDelete(void * data_ptr)
{
    auto ptr = static_cast<DataHolder*>(data_ptr);
    std::cerr << "dataDelete Runned!!! ptr=" << data_ptr << " => " << ptr << "\n";
    delete ptr;
    return;
}
