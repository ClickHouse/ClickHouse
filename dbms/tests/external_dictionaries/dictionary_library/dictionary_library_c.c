/// Pure c sample dictionary library

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

typedef const char * CString;

typedef struct
{
    const uint64_t size;
    const uint64_t * data;
} ClickHouseLibVectorUInt64;

typedef struct
{
    uint64_t size;
    CString * data;
} ClickHouseLibCStrings;

typedef struct
{
    void (*log)(int, CString);
} LibHolder;

typedef struct
{
    LibHolder * lib;
    int someField;
} DataHolder;

typedef struct
{
    const void * data;
    uint64_t size;
} ClickHouseLibField;

typedef struct
{
    const ClickHouseLibField * data;
    uint64_t size;
} ClickHouseLibRow;

typedef struct
{
    const ClickHouseLibRow * data;
    uint64_t size;
    uint64_t error_code;
    const char * error_string;
} ClickHouseLibTable;


#define LOG(logger, format, ...)                  \
    do                                            \
    {                                             \
        char buffer[128];                         \
        sprintf(buffer, (format), ##__VA_ARGS__); \
        (logger)(6, buffer);                      \
    } while (0)


void * ClickHouseDictionary_v3_loadIds(
    void * data_ptr, ClickHouseLibCStrings * settings, ClickHouseLibCStrings * columns, ClickHouseLibVectorUInt64 * ids)
{
    LibHolder * lib = ((DataHolder *)(data_ptr))->lib;
    LOG(lib->log, "loadIds c lib call ptr=%p size=%" PRIu64, data_ptr, ids->size);
    return 0;
}

void * ClickHouseDictionary_v3_loadAll(void * data_ptr, ClickHouseLibCStrings * settings, ClickHouseLibCStrings * columns)
{
    LibHolder * lib = ((DataHolder *)(data_ptr))->lib;
    LOG(lib->log, "loadAll c lib call ptr=%p", data_ptr);
    return 0;
}

void * ClickHouseDictionary_v3_loadKeys(void * data_ptr, ClickHouseLibCStrings * settings, ClickHouseLibTable* requested_keys)
{
    LibHolder * lib = ((DataHolder *)(data_ptr))->lib;
    LOG(lib->log, "loadKeys c lib call ptr=%p size=%" PRIu64, data_ptr, requested_keys->size);
    return 0;
}

void * ClickHouseDictionary_v3_libNew(ClickHouseLibCStrings * settings, void (*logFunc)(int, CString))
{
    LibHolder * lib_ptr = (LibHolder *)malloc(sizeof(LibHolder));
    lib_ptr->log = logFunc;
    LOG(lib_ptr->log, "libNew c lib call lib_ptr=%p", lib_ptr);
    return lib_ptr;
}

void ClickHouseDictionary_v3_libDelete(void * lib_ptr)
{
    LibHolder * lib = (LibHolder *)(lib_ptr);
    LOG(lib->log, "libDelete c lib call lib_ptr=%p", lib_ptr);
    free(lib_ptr);
    return;
}


void * ClickHouseDictionary_v3_dataNew(void * lib_ptr)
{
    DataHolder * data_ptr = (DataHolder *)malloc(sizeof(DataHolder));
    data_ptr->lib = (LibHolder *)lib_ptr;
    data_ptr->someField = 42;
    LOG(data_ptr->lib->log, "dataNew c lib call lib_ptr=%p data_ptr=%p", lib_ptr, data_ptr);
    return data_ptr;
}

void ClickHouseDictionary_v3_dataDelete(void * lib_ptr, void * data_ptr)
{
    LibHolder * lib = (LibHolder *)(lib_ptr);
    LOG(lib->log, "dataDelete c lib call lib_ptr=%p data_ptr=%p", lib_ptr, data_ptr);
    free(data_ptr);
    return;
}
