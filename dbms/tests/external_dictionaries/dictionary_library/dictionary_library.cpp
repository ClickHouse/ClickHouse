/// c++ sample dictionary library

/// proller: TODO: describe

#include <cstdint>
#include <iostream>
#include <memory>
#include <vector>

#include <Dictionaries/LibraryDictionarySourceExternal.h>

//#define DUMPS(VAR) #VAR " = " << VAR
//#define DUMP(VAR) std::cerr << __FILE__ << ":" << __LINE__ << " " << DUMPS(VAR) << "\n";


struct LibHolder
{
    //Some your data, maybe service connection
};

struct DataHolder
{
    std::vector<std::vector<uint64_t>> dataHolder; // Actual data storage
    std::vector<std::vector<ClickHouseLibrary::Field>> fieldHolder; // Pointers and sizes of data
    std::unique_ptr<ClickHouseLibrary::Row[]> rowHolder;
    ClickHouseLibrary::Table ctable; // Result data prepared for transfer via c-style interface
    LibHolder * lib = nullptr;
};


void MakeColumnsFromVector(DataHolder * ptr)
{
    for (const auto & row : ptr->dataHolder)
    {
        std::vector<ClickHouseLibrary::Field> fields;
        for (const auto & field : row)
            fields.push_back({&field, sizeof(field)});

        ptr->fieldHolder.push_back(fields);
    }

    const auto rows_num = ptr->fieldHolder.size();
    ptr->rowHolder = std::make_unique<ClickHouseLibrary::Row[]>(rows_num);
    size_t i = 0;
    for (auto & row : ptr->fieldHolder)
    {
        ptr->rowHolder[i].size = row.size();
        ptr->rowHolder[i].data = row.data();
        ++i;
    }
    ptr->ctable.size = rows_num;
    ptr->ctable.data = ptr->rowHolder.get();
}

extern "C" {

void * ClickHouseDictionary_v2_loadIds(void * data_ptr,
    ClickHouseLibrary::CStrings * settings,
    ClickHouseLibrary::CStrings * columns,
    const struct ClickHouseLibrary::VectorUInt64 * ids)
{
    auto ptr = static_cast<DataHolder *>(data_ptr);

    if (ids)
        std::cerr << "loadIds lib call ptr=" << data_ptr << " => " << ptr << " size=" << ids->size << "\n";

    if (!ptr)
        return nullptr;

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

    if (ids)
    {
        std::cerr << "ids passed: " << ids->size << "\n";
        for (size_t i = 0; i < ids->size; ++i)
        {
            std::cerr << "id " << i << " :" << ids->data[i] << " replying.\n";
            ptr->dataHolder.emplace_back(std::vector<uint64_t>{ids->data[i], ids->data[i] + 1, (1 + ids->data[i]) * 10, 65});
        }
    }

    MakeColumnsFromVector(ptr);
    return static_cast<void *>(&ptr->ctable);
}

void * ClickHouseDictionary_v2_loadAll(void * data_ptr, ClickHouseLibrary::CStrings * settings, ClickHouseLibrary::CStrings * /*columns*/)
{
    auto ptr = static_cast<DataHolder *>(data_ptr);
    std::cerr << "loadAll lib call ptr=" << data_ptr << " => " << ptr << "\n";
    if (!ptr)
        return nullptr;
    if (settings)
    {
        std::cerr << "settings passed: " << settings->size << "\n";
        for (size_t i = 0; i < settings->size; ++i)
        {
            std::cerr << "setting " << i << " :" << settings->data[i] << " \n";
        }
    }

    for (size_t i = 0; i < 7; ++i)
    {
        std::cerr << "id " << i << " :"
                  << " generating.\n";
        ptr->dataHolder.emplace_back(std::vector<uint64_t>{i, i + 1, (1 + i) * 10, 65});
    }

    MakeColumnsFromVector(ptr);
    return static_cast<void *>(&ptr->ctable);
}

void * ClickHouseDictionary_v2_loadKeys(void * data_ptr,
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

    //MakeColumnsFromVector(ptr);

    return nullptr;
}

void * ClickHouseDictionary_v2_libNew(ClickHouseLibrary::CStrings * /*settings*/)
{
    auto lib_ptr = new LibHolder;
    return lib_ptr;
}

void ClickHouseDictionary_v2_libDelete(void * lib_ptr)
{
    auto ptr = static_cast<LibHolder *>(lib_ptr);
    delete ptr;
    return;
}

void * ClickHouseDictionary_v2_dataNew(void * lib_ptr)
{
    auto data_ptr = new DataHolder;
    data_ptr->lib = static_cast<decltype(data_ptr->lib)>(lib_ptr);
    return data_ptr;
}

void ClickHouseDictionary_v2_dataDelete(void * /*lib_ptr*/, void * data_ptr)
{
    auto ptr = static_cast<DataHolder *>(data_ptr);
    delete ptr;
    return;
}
}
