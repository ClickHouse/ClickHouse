#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/CacheLog.h>


namespace DB
{

NamesAndTypesList CacheLogElement::getNamesAndTypes()
{
    return {
        {"query_id", std::make_shared<DataTypeString>()},
        {"hit_count", std::make_shared<DataTypeUInt64>()},
        {"miss_count", std::make_shared<DataTypeUInt64>()}};
}

void CacheLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(query_id);
    columns[i++]->insert(hit_count);
    columns[i++]->insert(miss_count);
}

}
