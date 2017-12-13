/// c++ sample dictionary library

#include <cstdint>
#include <iostream>
#include <memory>
#include <vector>

#include <Dictionaries/LibraryDictionarySourceExternal.h>

//#define DUMPS(VAR) #VAR " = " << VAR
//#define DUMP(VAR) std::cerr << __FILE__ << ":" << __LINE__ << " " << DUMPS(VAR) << "\n";

struct DataHolder
{
    std::vector<std::vector<uint64_t>> vector;
    std::unique_ptr<ClickHouseLibrary::VectorUInt64[]> columnsHolder;
    ClickHouseLibrary::ColumnsUInt64 columns;
};

extern "C" {

void * ClickHouseDictionary_v1_loadIds(
    void * data_ptr, ClickHouseLibrary::CStrings * settings, ClickHouseLibrary::CStrings * columns, const struct ClickHouseLibrary::VectorUInt64 * ids)
{
    auto ptr = static_cast<DataHolder *>(data_ptr);

    if (ids)
        std::cerr << "loadIds lib call ptr=" << data_ptr << " => " << ptr << " size=" << ids->size << "\n";

    if (settings)
    {
        std::cerr << "settings passed: " << settings->size << "\n";
        for (size_t i = 0; i < settings->size; ++i)
            std::cerr << "setting " << i << " :" << settings->data[i] << " \n";
    }

    if (columns)
    {
        std::cerr << "columns passed:" << columns->size << "\n";
        for (size_t i = 0; i < columns->size; ++i)
            std::cerr << "column " << i << " :" << columns->data[i] << "\n";
    }
    if (ptr)
    {
        if (ids)
        {
            std::cerr << "ids passed: " << ids->size << "\n";
            for (size_t i = 0; i < ids->size; ++i)
            {
                std::cerr << "id " << i << " :" << ids->data[i] << " replying.\n";
                ptr->vector.emplace_back(std::vector<uint64_t>{ids->data[i], ids->data[i] + 1, (1 + ids->data[i]) * 10, 65});
            }
        }

        ptr->columnsHolder = std::make_unique<ClickHouseLibrary::VectorUInt64[]>(ptr->vector.size());
        size_t i = 0;
        for (auto & col : ptr->vector)
        {
            //DUMP(i);
            //DUMP(col);
            ptr->columnsHolder[i].size = col.size();
            ptr->columnsHolder[i].data = col.data();
            ++i;
        }
        ptr->columns.size = ptr->vector.size();
        //DUMP(ptr->columns.size);
        ptr->columns.data = ptr->columnsHolder.get();
        //DUMP(ptr->columns.columns);
        return static_cast<void *>(&ptr->columns);
    }

    return nullptr;
}

void * ClickHouseDictionary_v1_loadAll(void * data_ptr, ClickHouseLibrary::CStrings * settings, ClickHouseLibrary::CStrings * /*columns*/)
{
    auto ptr = static_cast<DataHolder *>(data_ptr);
    std::cerr << "loadAll lib call ptr=" << data_ptr << " => " << ptr << "\n";
    if (settings)
    {
        std::cerr << "settings passed: " << settings->size << "\n";
        for (size_t i = 0; i < settings->size; ++i)
        {
            std::cerr << "setting " << i << " :" << settings->data[i] << " \n";
        }
    }

    if (ptr)
    {
        for (size_t i = 0; i < 7; ++i)
        {
            std::cerr << "id " << i << " :"
                      << " generating.\n";
            ptr->vector.emplace_back(std::vector<uint64_t>{i, i + 1, (1 + i) * 10, 65});
        }

        ptr->columnsHolder = std::make_unique<ClickHouseLibrary::VectorUInt64[]>(ptr->vector.size());
        size_t i = 0;
        for (auto & col : ptr->vector)
        {
            ptr->columnsHolder[i].size = col.size();
            ptr->columnsHolder[i].data = col.data();
            ++i;
        }
        ptr->columns.size = ptr->vector.size();
        //DUMP(ptr->columns.size);
        ptr->columns.data = ptr->columnsHolder.get();
        //DUMP(ptr->columns.columns);
        return static_cast<void *>(&ptr->columns);
    }
    //return;
    return nullptr;
}

void * ClickHouseDictionary_v1_loadKeys(void * data_ptr,
    ClickHouseLibrary::CStrings * settings,
    ClickHouseLibrary::CStrings * columns,
    const ClickHouseLibrary::VectorUInt64 * requested_rows)
{
    auto ptr = static_cast<DataHolder *>(data_ptr);
    std::cerr << "loadKeys lib call ptr=" << data_ptr << " => " << ptr << "\n";
    if (settings)
    {
        std::cerr << "settings passed: " << settings->size << "\n";
        for (size_t i = 0; i < settings->size; ++i)
        {
            std::cerr << "setting " << i << " :" << settings->data[i] << " \n";
        }
    }
    if (columns)
    {
        std::cerr << "columns passed:" << columns->size << "\n";
        for (size_t i = 0; i < columns->size; ++i)
        {
            std::cerr << "col " << i << " :" << columns->data[i] << "\n";
        }
    }
    if (requested_rows)
    {
        std::cerr << "requested_rows passed: " << requested_rows->size << "\n";
        for (size_t i = 0; i < requested_rows->size; ++i)
        {
            std::cerr << "id " << i << " :" << requested_rows->data[i] << "\n";
        }
    }

    return nullptr;
}

void * ClickHouseDictionary_v1_dataAllocate()
{
    auto data_ptr = new DataHolder;
    return data_ptr;
}

void ClickHouseDictionary_v1_dataDelete(void * data_ptr)
{
    auto ptr = static_cast<DataHolder *>(data_ptr);
    delete ptr;
    return;
}
}
