#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemErrors.h>
#include <Storages/Distributed/DirectoryMonitor.h>
#include <Storages/StorageDistributed.h>
#include <Storages/VirtualColumnUtils.h>
#include <Access/ContextAccess.h>
#include <Common/typeid_cast.h>
#include <Common/HashTable/HashMap.h>
#include <Databases/IDatabase.h>
#include <Interpreters/AggregationCommon.h>
#include <atomic>

extern std::string_view errorCodeToName(int code);
extern HashMap<int, std::atomic<uint64_t>, DefaultHash<int>> error_codes_count;

namespace DB
{

NamesAndTypesList StorageSystemErrors::getNamesAndTypes()
{
    return {
        { "name",              std::make_shared<DataTypeString>() },
        { "code",              std::make_shared<DataTypeInt32>() },
        { "value",             std::make_shared<DataTypeUInt64>() },
    };
}


void StorageSystemErrors::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    for (const auto & error_code_pair : error_codes_count)
    {
        size_t col_num = 0;
        res_columns[col_num++]->insert(errorCodeToName(error_code_pair.getKey()));
        res_columns[col_num++]->insert(error_code_pair.getKey());
        res_columns[col_num++]->insert(uint64_t(error_code_pair.getMapped()));
    }
}

}
