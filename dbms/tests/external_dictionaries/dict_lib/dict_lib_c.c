#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>

struct VectorUint64 {const uint64_t size; const uint64_t * data;};

void loadIds(struct VectorUint64 ids)
{
  printf("loadIds c Runned!!!=%" PRIu64 "\n", ids.size);
  return;
}

void loadAll()
{
  printf("loadAll c Runned!!!");
  return;
}

void loadKeys(struct VectorUint64 requested_rows)
{
  printf("loadIds c Runned!!!=%" PRIu64 "\n", requested_rows.size);
  return;
}
