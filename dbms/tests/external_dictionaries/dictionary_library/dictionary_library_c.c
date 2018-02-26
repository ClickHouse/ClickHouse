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

void * ClickHouseDictionary_v2_loadIds(
    void * data_ptr, ClickHouseLibCStrings * settings, ClickHouseLibCStrings * columns, ClickHouseLibVectorUInt64 * ids)
{
    printf("loadIds c lib call ptr=%p size=%" PRIu64 "\n", data_ptr, ids->size);
    return 0;
}

void * ClickHouseDictionary_v2_loadAll(void * data_ptr, ClickHouseLibCStrings * settings, ClickHouseLibCStrings * columns)
{
    printf("loadAll c lib call ptr=%p \n", data_ptr);
    return 0;
}

void * ClickHouseDictionary_v2_loadKeys(
    void * data_ptr, ClickHouseLibCStrings * settings, ClickHouseLibCStrings * columns, const ClickHouseLibVectorUInt64 * requested_rows)
{
    printf("loadKeys c lib call ptr=%p size=%" PRIu64 "\n", data_ptr, requested_rows->size);
    return 0;
}

void * ClickHouseDictionary_v2_libNew()
{
    int size = 101;
    void * lib_ptr = malloc(size);
    printf("libNew c lib call lib_ptr=%p \n", lib_ptr);
    return lib_ptr;
}

void ClickHouseDictionary_v2_libDelete(void * lib_ptr)
{
    printf("libDelete c lib call lib_ptr=%p \n", lib_ptr);
    free(lib_ptr);
    return;
}


void * ClickHouseDictionary_v2_dataNew(void * lib_ptr)
{
    int size = 100;
    void * data_ptr = malloc(size);
    printf("dataNew c lib call lib_ptr=%p data_ptr=%p \n", lib_ptr, data_ptr);
    return data_ptr;
}

void ClickHouseDictionary_v2_dataDelete(void * lib_ptr, void * data_ptr)
{
    printf("dataDelete c lib call lib_ptr=%p data_ptr=%p \n", lib_ptr, data_ptr);
    free(data_ptr);
    return;
}
