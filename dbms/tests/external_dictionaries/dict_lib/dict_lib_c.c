#include <stdint.h>
#include <stdio.h>
//typedef uint64_t UInt64;

struct VectorUint64 {const uint64_t size; const uint64_t * data;};

void loadIds(struct VectorUint64 ids)
{
  printf("loadIds c Runned!!!=%d\n", ids.size);
  return;
}

void loadAll()
{
  printf("loadAll c Runned!!!");
  return;
}

void loadKeys(struct VectorUint64 requested_rows)
{
  printf("loadIds c Runned!!!=%d\n", requested_rows.size);
  return;
}
