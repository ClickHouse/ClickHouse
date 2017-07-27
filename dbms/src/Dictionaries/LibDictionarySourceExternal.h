
//struct LoadIdsParams {const uint64_t size; const uint64_t * data;};

//using ClickhouseColumns = const char**;

using ClickhouseColumn = const char *;
using ClickhouseColumns = ClickhouseColumn[];

struct ClickhouseVectorUint64
{
    uint64_t size = 0;
    const uint64_t * data = nullptr;
};

struct ClickhouseColumnsUint64
{
    uint64_t size = 0;
    ClickhouseVectorUint64 * columns = nullptr;
};
