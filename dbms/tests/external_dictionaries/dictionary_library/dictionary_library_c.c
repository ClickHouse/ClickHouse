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

void * ClickHouseDictionary_v1_loadIds(void * data_ptr, ClickHouseLibCStrings * settings, ClickHouseLibCStrings * columns, ClickHouseLibVectorUInt64 * ids)
{
    printf("loadIds c lib call ptr=%p size=%" PRIu64 "\n", data_ptr, ids->size);
    return 0;
}

void * ClickHouseDictionary_v1_loadAll(void * data_ptr, ClickHouseLibCStrings * settings, ClickHouseLibCStrings * columns)
{
    printf("loadAll c lib call ptr=%p \n", data_ptr);
    return 0;
}

void * ClickHouseDictionary_v1_loadKeys(void * data_ptr,
    ClickHouseLibCStrings * settings,
    ClickHouseLibCStrings * columns,
    const ClickHouseLibVectorUInt64 * requested_rows)
{
    printf("loadKeys c lib call ptr=%p size=%" PRIu64 "\n", data_ptr, requested_rows->size);
    return 0;
}


void * ClickHouseDictionary_v1_dataAllocate()
{
    int size = 100;
    void * data_ptr = malloc(size);
    printf("dataAllocate c lib call ptr=%p \n", data_ptr);
    return data_ptr;
}

void ClickHouseDictionary_v1_dataDelete(void * data_ptr)
{
    printf("dataDelete c lib call ptr=%p \n", data_ptr);
    free(data_ptr);
    return;
}
