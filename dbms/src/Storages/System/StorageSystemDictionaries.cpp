#include <Storages/System/StorageSystemDictionaries.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Interpreters/ExternalDictionaries.h>
#include <ext/map.h>
#include <mutex>

namespace DB
{

StorageSystemDictionaries::StorageSystemDictionaries(const std::string & name)
    : name{name}
{
    setColumns(ColumnsDescription({
        { "name", std::make_shared<DataTypeString>() },
        { "origin", std::make_shared<DataTypeString>() },
        { "type", std::make_shared<DataTypeString>() },
        { "key", std::make_shared<DataTypeString>() },
        { "attribute.names", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "attribute.types", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()) },
        { "bytes_allocated", std::make_shared<DataTypeUInt64>() },
        { "query_count", std::make_shared<DataTypeUInt64>() },
        { "hit_rate", std::make_shared<DataTypeFloat64>() },
        { "element_count", std::make_shared<DataTypeUInt64>() },
        { "load_factor", std::make_shared<DataTypeFloat64>() },
        { "creation_time", std::make_shared<DataTypeDateTime>() },
        { "source", std::make_shared<DataTypeString>() },
        { "last_exception", std::make_shared<DataTypeString>() },
    }));
}


BlockInputStreams StorageSystemDictionaries::read(
    const Names & column_names,
    const SelectQueryInfo &,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t,
    const unsigned)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    const auto & external_dictionaries = context.getExternalDictionaries();
    auto objects_map = external_dictionaries.getObjectsMap();
    const auto & dictionaries = objects_map.get();

    MutableColumns res_columns = getSampleBlock().cloneEmptyColumns();

    for (const auto & dict_info : dictionaries)
    {
        size_t i = 0;

        res_columns[i++]->insert(dict_info.first);
        res_columns[i++]->insert(dict_info.second.origin);

        if (dict_info.second.loadable)
        {
            const auto dict_ptr = std::static_pointer_cast<IDictionaryBase>(dict_info.second.loadable);

            res_columns[i++]->insert(dict_ptr->getTypeName());

            const auto & dict_struct = dict_ptr->getStructure();
            res_columns[i++]->insert(dict_struct.getKeyDescription());
            res_columns[i++]->insert(ext::map<Array>(dict_struct.attributes, [] (auto & attr) { return attr.name; }));
            res_columns[i++]->insert(ext::map<Array>(dict_struct.attributes, [] (auto & attr) { return attr.type->getName(); }));
            res_columns[i++]->insert(static_cast<UInt64>(dict_ptr->getBytesAllocated()));
            res_columns[i++]->insert(static_cast<UInt64>(dict_ptr->getQueryCount()));
            res_columns[i++]->insert(dict_ptr->getHitRate());
            res_columns[i++]->insert(static_cast<UInt64>(dict_ptr->getElementCount()));
            res_columns[i++]->insert(dict_ptr->getLoadFactor());
            res_columns[i++]->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(dict_ptr->getCreationTime())));
            res_columns[i++]->insert(dict_ptr->getSource()->toString());
        }
        else
        {
            while (i < 13)
                res_columns[i++]->insertDefault();
        }

        if (dict_info.second.exception)
        {
            try
            {
                std::rethrow_exception(dict_info.second.exception);
            }
            catch (...)
            {
                res_columns[i++]->insert(getCurrentExceptionMessage(false));
            }
        }
        else
            res_columns[i++]->insertDefault();
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}

}
