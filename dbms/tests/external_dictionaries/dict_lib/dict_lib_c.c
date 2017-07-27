#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

struct ClickhouseVectorUint64
{
    const uint64_t size;
    const uint64_t * data;
};
#define ClickhouseColumns const char **

void * loadIds(void * data_ptr, struct ClickhouseVectorUint64 ids)
{
    printf("loadIds c Runned!!! ptr=%p size=%" PRIu64 "\n", data_ptr, ids.size);
    return 0;
}

void * loadAll(void * data_ptr)
{
    printf("loadAll c Runned!!! ptr=%p \n", data_ptr);
    return 0;
}

/*
void loadKeys(ClickhouseColumns columns, struct ClickhouseVectorUint64 requested_rows)
{
    printf("loadIds c Runned!!!=%" PRIu64 "\n", requested_rows.size);
    return;
}
*/


void * dataAllocate()
{
    int size = 100;
    void * data_ptr = malloc(size);
    printf("dataAllocate c Runned!!! ptr=%p \n", data_ptr);
    return data_ptr;
}

void dataDelete(void * data_ptr)
{
    printf("dataDelete c Runned!!! ptr=%p \n", data_ptr);
    free(data_ptr);
    return;
}
