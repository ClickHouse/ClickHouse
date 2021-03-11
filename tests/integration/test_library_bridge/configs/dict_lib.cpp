/// c++ sample dictionary library

#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <vector>

#define CLICKHOUSE_DICTIONARY_LIBRARY_API 1

namespace ClickHouseLibrary
{
using CString = const char *;
using ColumnName = CString;
using ColumnNames = ColumnName[];

struct CStrings
{
    CString * data = nullptr;
    uint64_t size = 0;
};

struct VectorUInt64
{
    const uint64_t * data = nullptr;
    uint64_t size = 0;
};

struct ColumnsUInt64
{
    VectorUInt64 * data = nullptr;
    uint64_t size = 0;
};

struct Field
{
    const void * data = nullptr;
    uint64_t size = 0;
};

struct Row
{
    const Field * data = nullptr;
    uint64_t size = 0;
};

struct Table
{
    const Row * data = nullptr;
    uint64_t size = 0;
    uint64_t error_code = 0; // 0 = ok; !0 = error, with message in error_string
    const char * error_string = nullptr;
};

enum LogLevel
{
    FATAL = 1,
    CRITICAL,
    ERROR,
    WARNING,
    NOTICE,
    INFORMATION,
    DEBUG,
    TRACE,
};

void log(LogLevel level, CString msg);
}


#define LOG(logger, message)                                             \
    do                                                                   \
    {                                                                    \
        std::stringstream builder;                                       \
        builder << message;                                              \
        (logger)(ClickHouseLibrary::INFORMATION, builder.str().c_str()); \
    } while (false)


struct LibHolder
{
    std::function<void(ClickHouseLibrary::LogLevel, ClickHouseLibrary::CString)> log;
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


extern "C"
{

void * ClickHouseDictionary_v3_loadIds(void * data_ptr,
    ClickHouseLibrary::CStrings * settings,
    ClickHouseLibrary::CStrings * columns,
    const struct ClickHouseLibrary::VectorUInt64 * ids)
{
    auto ptr = static_cast<DataHolder *>(data_ptr);

    if (ids)
        LOG(ptr->lib->log, "loadIds lib call ptr=" << data_ptr << " => " << ptr << " size=" << ids->size);

    if (!ptr)
        return nullptr;

    if (settings)
    {
        LOG(ptr->lib->log, "settings passed: " << settings->size);
        for (size_t i = 0; i < settings->size; ++i)
        {
            LOG(ptr->lib->log, "setting " << i << " :" << settings->data[i]);
        }
    }

    if (columns)
    {
        LOG(ptr->lib->log, "columns passed:" << columns->size);
        for (size_t i = 0; i < columns->size; ++i)
        {
            LOG(ptr->lib->log, "column " << i << " :" << columns->data[i]);
        }
    }

    if (ids)
    {
        LOG(ptr->lib->log, "ids passed: " << ids->size);
        for (size_t i = 0; i < ids->size; ++i)
        {
            LOG(ptr->lib->log, "id " << i << " :" << ids->data[i] << " replying.");
            ptr->dataHolder.emplace_back(std::vector<uint64_t>{ids->data[i], ids->data[i] + 100, ids->data[i] + 200, ids->data[i] + 300});
        }
    }

    MakeColumnsFromVector(ptr);
    return static_cast<void *>(&ptr->ctable);
}


void * ClickHouseDictionary_v3_loadAll(void * data_ptr, ClickHouseLibrary::CStrings * settings, ClickHouseLibrary::CStrings * /*columns*/)
{
    auto ptr = static_cast<DataHolder *>(data_ptr);

    LOG(ptr->lib->log, "loadAll lib call ptr=" << data_ptr << " => " << ptr);

    if (!ptr)
        return nullptr;

    if (settings)
    {
        LOG(ptr->lib->log, "settings passed: " << settings->size);
        for (size_t i = 0; i < settings->size; ++i)
        {
            LOG(ptr->lib->log, "setting " << i << " :" << settings->data[i]);
        }
    }

    for (size_t i = 0; i < 10; ++i)
    {
        LOG(ptr->lib->log, "id " << i << " :" << " generating.");
        ptr->dataHolder.emplace_back(std::vector<uint64_t>{i, i + 10, i + 20, i + 30});
    }

    MakeColumnsFromVector(ptr);
    return static_cast<void *>(&ptr->ctable);
}


void * ClickHouseDictionary_v3_loadKeys(void * data_ptr, ClickHouseLibrary::CStrings * settings, ClickHouseLibrary::Table * requested_keys)
{
    auto ptr = static_cast<DataHolder *>(data_ptr);
    LOG(ptr->lib->log, "loadKeys lib call ptr=" << data_ptr << " => " << ptr);
    if (settings)
    {
        LOG(ptr->lib->log, "settings passed: " << settings->size);
        for (size_t i = 0; i < settings->size; ++i)
        {
            LOG(ptr->lib->log, "setting " << i << " :" << settings->data[i]);
        }
    }
    if (requested_keys)
    {
        LOG(ptr->lib->log, "requested_keys columns passed: " << requested_keys->size);
        for (size_t i = 0; i < requested_keys->size; ++i)
        {
            LOG(ptr->lib->log, "requested_keys at column " << i << " passed: " << requested_keys->data[i].size);
            ptr->dataHolder.emplace_back(std::vector<uint64_t>{i, i + 100, i + 200, i + 300});
        }
    }

    MakeColumnsFromVector(ptr);
    return static_cast<void *>(&ptr->ctable);
}

void * ClickHouseDictionary_v3_libNew(
    ClickHouseLibrary::CStrings * /*settings*/, void (*logFunc)(ClickHouseLibrary::LogLevel, ClickHouseLibrary::CString))
{
    auto lib_ptr = new LibHolder;
    lib_ptr->log = logFunc;
    return lib_ptr;
}

void ClickHouseDictionary_v3_libDelete(void * lib_ptr)
{
    auto ptr = static_cast<LibHolder *>(lib_ptr);
    delete ptr;
    return;
}

void * ClickHouseDictionary_v3_dataNew(void * lib_ptr)
{
    auto data_ptr = new DataHolder;
    data_ptr->lib = static_cast<decltype(data_ptr->lib)>(lib_ptr);
    return data_ptr;
}

void ClickHouseDictionary_v3_dataDelete(void * /*lib_ptr*/, void * data_ptr)
{
    auto ptr = static_cast<DataHolder *>(data_ptr);
    delete ptr;
    return;
}

}
