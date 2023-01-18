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

#include <base/map.h>
#include <mutex>

namespace DB
{
namespace
{
    /// Used for column indexes!  Don't change the numbers!
    enum SystemDictionariesColumn
    {
        Database = 0,
        Name,
        Uuid,
        Status,
        Origin,
        Type,
        KeyNames,
        KeyTypes,
        AttributeNames,
        AttributeTypes,
        BytesAllocated,
        HierarchicalIndexBytesAllocated,
        QueryCount,
        HitRate,
        FoundRate,
        ElementCount,
        LoadFactor,
        Source,
        LifetimeMin,
        LifetimeMax,
        LoadingStartTime,
        LastSuccessfulUpdateTime,
        LoadingDuration,
        LastException,
        Comment,
        /// Virtual columns
        Key
    };

    std::shared_ptr<const IDictionary> getDictionaryPtr(const ExternalLoader::LoadResult & load_result)
    {
        return std::dynamic_pointer_cast<const IDictionary>(load_result.object);
    }

    StorageID getDictionaryID(const ExternalLoader::LoadResult & load_result, const std::shared_ptr<const IDictionary> & dict_ptr)
    {
        StorageID dict_id = StorageID::createEmpty();

        if (dict_ptr)
            dict_id = dict_ptr->getDictionaryID();
        else if (load_result.config)
            dict_id = StorageID::fromDictionaryConfig(*load_result.config->config, load_result.config->key_in_config);
        else
            dict_id.table_name = load_result.name;

        return dict_id;
    }

    std::optional<DictionaryStructure>
    getDictionaryStructure(const ExternalLoader::LoadResult & load_result, std::exception_ptr & last_exception)
    try
    {
        return ExternalDictionariesLoader::getDictionaryStructure(*load_result.config);
    }
    catch (const DB::Exception &)
    {
        if (!last_exception)
            last_exception = std::current_exception();
        return {};
    }


}

NamesAndTypesList StorageSystemDictionaries::getNamesAndTypes()
{
    return {
        {"database", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},
        {"status", std::make_shared<DataTypeEnum8>(getStatusEnumAllPossibleValues())},
        {"origin", std::make_shared<DataTypeString>()},
        {"type", std::make_shared<DataTypeString>()},
        {"key.names", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"key.types", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"attribute.names", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"attribute.types", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"bytes_allocated", std::make_shared<DataTypeUInt64>()},
        {"hierarchical_index_bytes_allocated", std::make_shared<DataTypeUInt64>()},
        {"query_count", std::make_shared<DataTypeUInt64>()},
        {"hit_rate", std::make_shared<DataTypeFloat64>()},
        {"found_rate", std::make_shared<DataTypeFloat64>()},
        {"element_count", std::make_shared<DataTypeUInt64>()},
        {"load_factor", std::make_shared<DataTypeFloat64>()},
        {"source", std::make_shared<DataTypeString>()},
        {"lifetime_min", std::make_shared<DataTypeUInt64>()},
        {"lifetime_max", std::make_shared<DataTypeUInt64>()},
        {"loading_start_time", std::make_shared<DataTypeDateTime>()},
        {"last_successful_update_time", std::make_shared<DataTypeDateTime>()},
        {"loading_duration", std::make_shared<DataTypeFloat32>()},
        {"last_exception", std::make_shared<DataTypeString>()},
        {"comment", std::make_shared<DataTypeString>()}
    };
}

NamesAndTypesList StorageSystemDictionaries::getVirtuals() const
{
    return {
        {"key", std::make_shared<DataTypeString>()}
    };
}

void StorageSystemDictionaries::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & /*query_info*/) const
{
    const auto access = context->getAccess();
    const bool check_access_for_dictionaries = access->isGranted(AccessType::SHOW_DICTIONARIES);

    const auto & external_dictionaries = context->getExternalDictionariesLoader();

    if (!check_access_for_dictionaries)
        return;

    for (size_t index = 0; const auto & load_result : external_dictionaries.getLoadResults())
    {
        const auto dict_ptr = getDictionaryPtr(load_result);
        StorageID dict_id = getDictionaryID(load_result, dict_ptr);

        auto last_exception = load_result.exception;
        auto dict_structure = getDictionaryStructure(load_result, last_exception);

        String db_or_tag = dict_id.database_name.empty() ? IDictionary::NO_DATABASE_TAG : dict_id.database_name;
        if (!access->isGranted(AccessType::SHOW_DICTIONARIES, db_or_tag, dict_id.table_name))
            continue;

        res_columns[SystemDictionariesColumn::Database]->insert(dict_id.database_name);
        res_columns[SystemDictionariesColumn::Name]->insert(dict_id.table_name);
        res_columns[SystemDictionariesColumn::Uuid]->insert(dict_id.uuid);
        res_columns[SystemDictionariesColumn::Status]->insert(static_cast<Int8>(load_result.status));
        res_columns[SystemDictionariesColumn::Origin]->insert(load_result.config ? load_result.config->path : "");

        if (dict_ptr)
        {
            res_columns[SystemDictionariesColumn::Type]->insert(dict_ptr->getTypeName());
            res_columns[SystemDictionariesColumn::BytesAllocated]->insert(dict_ptr->getBytesAllocated());
            res_columns[SystemDictionariesColumn::HierarchicalIndexBytesAllocated]->insert(dict_ptr->getHierarchicalIndexBytesAllocated());
            res_columns[SystemDictionariesColumn::QueryCount]->insert(dict_ptr->getQueryCount());
            res_columns[SystemDictionariesColumn::HitRate]->insert(dict_ptr->getHitRate());
            res_columns[SystemDictionariesColumn::FoundRate]->insert(dict_ptr->getFoundRate());
            res_columns[SystemDictionariesColumn::ElementCount]->insert(dict_ptr->getElementCount());
            res_columns[SystemDictionariesColumn::LoadFactor]->insert(dict_ptr->getLoadFactor());
            res_columns[SystemDictionariesColumn::Source]->insert(dict_ptr->getSource()->toString());

            const auto & lifetime = dict_ptr->getLifetime();
            res_columns[SystemDictionariesColumn::LifetimeMin]->insert(lifetime.min_sec);
            res_columns[SystemDictionariesColumn::LifetimeMax]->insert(lifetime.max_sec);

            res_columns[SystemDictionariesColumn::Comment]->insert(dict_ptr->getDictionaryComment());

            if (!last_exception)
                last_exception = dict_ptr->getLastException();
        }
        else if (load_result.config && load_result.config->config->has("dictionary.comment"))
        {
            res_columns[SystemDictionariesColumn::Comment]->insert(load_result.config->config->getString("dictionary.comment"));
        }

        if (dict_structure)
        {
            res_columns[SystemDictionariesColumn::KeyNames]->insert(
                collections::map<Array>(dict_structure->getKeysNames(), [](auto & name) { return name; }));

            if (dict_structure->id)
                res_columns[SystemDictionariesColumn::KeyTypes]->insert(Array({"UInt64"}));
            else
                res_columns[SystemDictionariesColumn::KeyTypes]->insert(
                    collections::map<Array>(*dict_structure->key, [](auto & attr) { return attr.type->getName(); }));

            res_columns[SystemDictionariesColumn::AttributeNames]->insert(
                collections::map<Array>(dict_structure->attributes, [](auto & attr) { return attr.name; }));
            res_columns[SystemDictionariesColumn::AttributeTypes]->insert(
                collections::map<Array>(dict_structure->attributes, [](auto & attr) { return attr.type->getName(); }));
            res_columns[SystemDictionariesColumn::Key]->insert(dict_structure->getKeyDescription());
        }

        res_columns[SystemDictionariesColumn::LoadingStartTime]->insert(
            static_cast<UInt64>(std::chrono::system_clock::to_time_t(load_result.loading_start_time)));
        res_columns[SystemDictionariesColumn::LastSuccessfulUpdateTime]->insert(
            static_cast<UInt64>(std::chrono::system_clock::to_time_t(load_result.last_successful_update_time)));
        res_columns[SystemDictionariesColumn::LoadingDuration]->insert(
            std::chrono::duration_cast<std::chrono::duration<float>>(load_result.loading_duration).count());

        if (last_exception)
            res_columns[SystemDictionariesColumn::LastException]->insert(getExceptionMessage(last_exception, false));

        index++;
        /// Set to default unfilled columns
        for (auto & column : res_columns)
            if (column->size() < index)
                column->insertDefault();
    }
}

}
