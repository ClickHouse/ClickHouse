#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeUUID.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Access/ContextAccess.h>
#include <Storages/System/StorageSystemDictionaries.h>
#include <Storages/VirtualColumnUtils.h>
#include <Columns/ColumnString.h>
#include <Core/Names.h>

#include <ext/map.h>
#include <mutex>

namespace DB
{

NamesAndTypesList StorageSystemDictionaries::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},
        {"status", std::make_shared<DataTypeEnum8>(getStatusEnumAllPossibleValues())},
        {"origin", std::make_shared<DataTypeString>()},
        {"type", std::make_shared<DataTypeString>()},
        {"key", std::make_shared<DataTypeString>()},
        {"attribute.names", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"attribute.types", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"bytes_allocated", std::make_shared<DataTypeUInt64>()},
        {"query_count", std::make_shared<DataTypeUInt64>()},
        {"hit_rate", std::make_shared<DataTypeFloat64>()},
        {"element_count", std::make_shared<DataTypeUInt64>()},
        {"load_factor", std::make_shared<DataTypeFloat64>()},
        {"source", std::make_shared<DataTypeString>()},
        {"lifetime_min", std::make_shared<DataTypeUInt64>()},
        {"lifetime_max", std::make_shared<DataTypeUInt64>()},
        {"loading_start_time", std::make_shared<DataTypeDateTime>()},
        {"last_successful_update_time", std::make_shared<DataTypeDateTime>()},
        {"loading_duration", std::make_shared<DataTypeFloat32>()},
        //{ "creation_time", std::make_shared<DataTypeDateTime>() },
        {"last_exception", std::make_shared<DataTypeString>()}
    };
}

void StorageSystemDictionaries::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & /*query_info*/) const
{
    const auto access = context.getAccess();
    const bool check_access_for_dictionaries = !access->isGranted(AccessType::SHOW_DICTIONARIES);

    const auto & external_dictionaries = context.getExternalDictionariesLoader();
    for (const auto & load_result : external_dictionaries.getLoadResults())
    {
        const auto dict_ptr = std::dynamic_pointer_cast<const IDictionaryBase>(load_result.object);

        StorageID dict_id = StorageID::createEmpty();
        if (dict_ptr)
            dict_id = dict_ptr->getDictionaryID();
        else if (load_result.config)
            dict_id = StorageID::fromDictionaryConfig(*load_result.config->config, load_result.config->key_in_config);
        else
            dict_id.table_name = load_result.name;

        String db_or_tag = dict_id.database_name.empty() ? IDictionary::NO_DATABASE_TAG : dict_id.database_name;
        if (check_access_for_dictionaries
            && !access->isGranted(AccessType::SHOW_DICTIONARIES, db_or_tag, dict_id.table_name))
            continue;

        size_t i = 0;
        res_columns[i++]->insert(dict_id.database_name);
        res_columns[i++]->insert(dict_id.table_name);
        res_columns[i++]->insert(dict_id.uuid);
        res_columns[i++]->insert(static_cast<Int8>(load_result.status));
        res_columns[i++]->insert(load_result.config ? load_result.config->path : "");

        std::exception_ptr last_exception = load_result.exception;

        if (dict_ptr)
        {
            res_columns[i++]->insert(dict_ptr->getTypeName());

            const auto & dict_struct = dict_ptr->getStructure();
            res_columns[i++]->insert(dict_struct.getKeyDescription());
            res_columns[i++]->insert(ext::map<Array>(dict_struct.attributes, [] (auto & attr) { return attr.name; }));
            res_columns[i++]->insert(ext::map<Array>(dict_struct.attributes, [] (auto & attr) { return attr.type->getName(); }));
            res_columns[i++]->insert(dict_ptr->getBytesAllocated());
            res_columns[i++]->insert(dict_ptr->getQueryCount());
            res_columns[i++]->insert(dict_ptr->getHitRate());
            res_columns[i++]->insert(dict_ptr->getElementCount());
            res_columns[i++]->insert(dict_ptr->getLoadFactor());
            res_columns[i++]->insert(dict_ptr->getSource()->toString());

            const auto & lifetime = dict_ptr->getLifetime();
            res_columns[i++]->insert(lifetime.min_sec);
            res_columns[i++]->insert(lifetime.max_sec);
            if (!last_exception)
                last_exception = dict_ptr->getLastException();
        }
        else
        {
            for (size_t j = 0; j != 12; ++j) // Number of empty fields if dict_ptr is null
                res_columns[i++]->insertDefault();
        }

        res_columns[i++]->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(load_result.loading_start_time)));
        res_columns[i++]->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(load_result.last_successful_update_time)));
        res_columns[i++]->insert(std::chrono::duration_cast<std::chrono::duration<float>>(load_result.loading_duration).count());

        if (last_exception)
            res_columns[i++]->insert(getExceptionMessage(last_exception, false));
        else
            res_columns[i++]->insertDefault();

    }
}

}

