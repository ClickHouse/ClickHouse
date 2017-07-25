
//struct LoadIdsParams {const uint64_t size; const uint64_t * data;};
struct ClickhouseVectorUint64
{
    const uint64_t size;
    const uint64_t * data;
};

//using ClickhouseColumns = const char**;

using ClickhouseColumn = const char *;
using ClickhouseColumns = ClickhouseColumn[];
