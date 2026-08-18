#include <DataTypes/DataTypesCache.h>

namespace DB
{

DataTypesCache & getDataTypesCache()
{
    thread_local static DataTypesCache data_types_cache;
    return data_types_cache;
}

}
