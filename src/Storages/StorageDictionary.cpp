#include <Storages/StorageDictionary.h>
#include <Storages/StorageFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ExternalLoaderDictionaryStorageConfigRepository.h>
#include <Parsers/ASTLiteral.h>
#include <Common/quoteString.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Pipe.h>
#include <IO/Operators.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int THERE_IS_NO_COLUMN;
    extern const int CANNOT_DETACH_DICTIONARY_AS_TABLE;
    extern const int DICTIONARY_ALREADY_EXISTS;
}

namespace
{
    void checkNamesAndTypesCompatibleWithDictionary(const String & dictionary_name, const ColumnsDescription & columns, const DictionaryStructure & dictionary_structure)
    {
        auto dictionary_names_and_types = StorageDictionary::getNamesAndTypes(dictionary_structure);
        std::set<NameAndTypePair> names_and_types_set(dictionary_names_and_types.begin(), dictionary_names_and_types.end());

        for (const auto & column : columns.getOrdinary())
        {
            if (names_and_types_set.find(column) == names_and_types_set.end())
            {
                throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Not found column {} {} in dictionary {}. There are only columns {}",
                                column.name, column.type->getName(), backQuote(dictionary_name),
                                StorageDictionary::generateNamesAndTypesDescription(dictionary_names_and_types));
            }
        }
    }
}


NamesAndTypesList StorageDictionary::getNamesAndTypes(const DictionaryStructure & dictionary_structure)
{
    NamesAndTypesList dictionary_names_and_types;

    if (dictionary_structure.id)
        dictionary_names_and_types.emplace_back(dictionary_structure.id->name, std::make_shared<DataTypeUInt64>());

    /// In old-style (XML) configuration we don't have this attributes in the
    /// main attribute list, so we have to add them to columns list explicitly.
    /// In the new configuration (DDL) we have them both in range_* nodes and
    /// main attribute list, but for compatibility we add them before main
    /// attributes list.
    if (dictionary_structure.range_min)
        dictionary_names_and_types.emplace_back(dictionary_structure.range_min->name, dictionary_structure.range_min->type);

    if (dictionary_structure.range_max)
        dictionary_names_and_types.emplace_back(dictionary_structure.range_max->name, dictionary_structure.range_max->type);

    if (dictionary_structure.key)
    {
        for (const auto & attribute : *dictionary_structure.key)
            dictionary_names_and_types.emplace_back(attribute.name, attribute.type);
    }

    for (const auto & attribute : dictionary_structure.attributes)
    {
        /// Some attributes can be already added (range_min and range_max)
        if (!dictionary_names_and_types.contains(attribute.name))
            dictionary_names_and_types.emplace_back(attribute.name, attribute.type);
    }

    return dictionary_names_and_types;
}


String StorageDictionary::generateNamesAndTypesDescription(const NamesAndTypesList & list)
{
    WriteBufferFromOwnString ss;
    bool first = true;
    for (const auto & name_and_type : list)
    {
        if (!std::exchange(first, false))
            ss << ", ";
        ss << name_and_type.name << ' ' << name_and_type.type->getName();
    }
    return ss.str();
}

StorageDictionary::StorageDictionary(
    const StorageID & table_id_,
    const String & dictionary_name_,
    const ColumnsDescription & columns_,
    const String & comment,
    Location location_,
    ContextPtr context_)
    : IStorage(table_id_), WithContext(context_->getGlobalContext()), dictionary_name(dictionary_name_), location(location_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}


StorageDictionary::StorageDictionary(
    const StorageID & table_id_,
    const String & dictionary_name_,
    const DictionaryStructure & dictionary_structure_,
    Location location_,
    ContextPtr context_)
    : StorageDictionary(
        table_id_, dictionary_name_, ColumnsDescription{getNamesAndTypes(dictionary_structure_)}, String{}, location_, context_)
{
}

StorageDictionary::StorageDictionary(
    const StorageID & table_id,
    LoadablesConfigurationPtr dictionary_configuration,
    ContextPtr context_)
    : StorageDictionary(
        table_id,
        table_id.getFullNameNotQuoted(),
        context_->getExternalDictionariesLoader().getDictionaryStructure(*dictionary_configuration),
        Location::SameDatabaseAndNameAsDictionary,
        context_)
{
    configuration = dictionary_configuration;

    auto repository = std::make_unique<ExternalLoaderDictionaryStorageConfigRepository>(*this);
    remove_repository_callback = context_->getExternalDictionariesLoader().addConfigRepository(std::move(repository));
}

StorageDictionary::~StorageDictionary()
{
    removeDictionaryConfigurationFromRepository();
}

void StorageDictionary::checkTableCanBeDropped() const
{
    if (location == Location::SameDatabaseAndNameAsDictionary)
        throw Exception(ErrorCodes::CANNOT_DETACH_DICTIONARY_AS_TABLE,
            "Cannot drop/detach dictionary {} as table, use DROP DICTIONARY or DETACH DICTIONARY query instead",
            dictionary_name);
    if (location == Location::DictionaryDatabase)
        throw Exception(ErrorCodes::CANNOT_DETACH_DICTIONARY_AS_TABLE,
            "Cannot drop/detach table from a database with DICTIONARY engine, use DROP DICTIONARY or DETACH DICTIONARY query instead",
            dictionary_name);
}

void StorageDictionary::checkTableCanBeDetached() const
{
    checkTableCanBeDropped();
}

Pipe StorageDictionary::read(
    const Names & column_names,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    SelectQueryInfo & /*query_info*/,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const unsigned /*threads*/)
{
    auto registered_dictionary_name = location == Location::SameDatabaseAndNameAsDictionary ? getStorageID().getInternalDictionaryName() : dictionary_name;
    auto dictionary = getContext()->getExternalDictionariesLoader().getDictionary(registered_dictionary_name, local_context);
    auto stream = dictionary->getBlockInputStream(column_names, max_block_size);
    /// TODO: update dictionary interface for processors.
    return Pipe(std::make_shared<SourceFromInputStream>(stream));
}

void StorageDictionary::shutdown()
{
    removeDictionaryConfigurationFromRepository();
}

void StorageDictionary::startup()
{
    auto global_context = getContext();

    bool lazy_load = global_context->getConfigRef().getBool("dictionaries_lazy_load", true);
    if (!lazy_load)
    {
        const auto & external_dictionaries_loader = global_context->getExternalDictionariesLoader();

        /// reloadConfig() is called here to force loading the dictionary.
        external_dictionaries_loader.reloadConfig(getStorageID().getInternalDictionaryName());
    }
}

void StorageDictionary::removeDictionaryConfigurationFromRepository()
{
    if (remove_repository_callback_executed)
        return;

    remove_repository_callback_executed = true;
    remove_repository_callback.reset();
}

Poco::Timestamp StorageDictionary::getUpdateTime() const
{
    std::lock_guard<std::mutex> lock(dictionary_config_mutex);
    return update_time;
}

LoadablesConfigurationPtr StorageDictionary::getConfiguration() const
{
    std::lock_guard<std::mutex> lock(dictionary_config_mutex);
    return configuration;
}

void StorageDictionary::renameInMemory(const StorageID & new_table_id)
{
    auto old_table_id = getStorageID();
    IStorage::renameInMemory(new_table_id);

    assert((location == Location::SameDatabaseAndNameAsDictionary) == (getConfiguration().get() != nullptr));
    if (location != Location::SameDatabaseAndNameAsDictionary)
        return;

    /// It's DDL dictionary, need to update configuration and reload

    bool move_to_atomic = old_table_id.uuid == UUIDHelpers::Nil && new_table_id.uuid != UUIDHelpers::Nil;
    bool move_to_ordinary = old_table_id.uuid != UUIDHelpers::Nil && new_table_id.uuid == UUIDHelpers::Nil;
    assert(old_table_id.uuid == new_table_id.uuid || move_to_atomic || move_to_ordinary);

    {
        std::lock_guard<std::mutex> lock(dictionary_config_mutex);

        configuration->setString("dictionary.database", new_table_id.database_name);
        configuration->setString("dictionary.name", new_table_id.table_name);
        if (move_to_atomic)
            configuration->setString("dictionary.uuid", toString(new_table_id.uuid));
        else if (move_to_ordinary)
                configuration->remove("dictionary.uuid");
    }

    /// Dictionary is moving between databases of different engines or is renaming inside Ordinary database
    bool recreate_dictionary = old_table_id.uuid == UUIDHelpers::Nil || new_table_id.uuid == UUIDHelpers::Nil;

    if (recreate_dictionary)
    {
        /// It's too hard to update both name and uuid, better to reload dictionary with new name
        removeDictionaryConfigurationFromRepository();
        auto repository = std::make_unique<ExternalLoaderDictionaryStorageConfigRepository>(*this);
        remove_repository_callback = getContext()->getExternalDictionariesLoader().addConfigRepository(std::move(repository));
        /// Dictionary will be reloaded lazily to avoid exceptions in the middle of renaming
    }
    else
    {
        const auto & external_dictionaries_loader = getContext()->getExternalDictionariesLoader();
        auto result = external_dictionaries_loader.getLoadResult(old_table_id.getInternalDictionaryName());

        if (result.object)
        {
            const auto dictionary = std::static_pointer_cast<const IDictionary>(result.object);
            dictionary->updateDictionaryName(new_table_id);
        }

        external_dictionaries_loader.reloadConfig(old_table_id.getInternalDictionaryName());
        dictionary_name = new_table_id.getFullNameNotQuoted();
    }
}

void registerStorageDictionary(StorageFactory & factory)
{
    factory.registerStorage("Dictionary", [](const StorageFactory::Arguments & args)
    {
        auto query = args.query;

        auto local_context = args.getLocalContext();

        if (query.is_dictionary)
        {
            auto dictionary_id = args.table_id;
            auto & external_dictionaries_loader = local_context->getExternalDictionariesLoader();

            /// A dictionary with the same full name could be defined in *.xml config files.
            if (external_dictionaries_loader.getCurrentStatus(dictionary_id.getFullNameNotQuoted()) != ExternalLoader::Status::NOT_EXIST)
                throw Exception(ErrorCodes::DICTIONARY_ALREADY_EXISTS,
                        "Dictionary {} already exists.", dictionary_id.getFullNameNotQuoted());

            /// Create dictionary storage that owns underlying dictionary
            auto abstract_dictionary_configuration = getDictionaryConfigurationFromAST(args.query, local_context, dictionary_id.database_name);
            auto result_storage = StorageDictionary::create(dictionary_id, abstract_dictionary_configuration, local_context);

            bool lazy_load = local_context->getConfigRef().getBool("dictionaries_lazy_load", true);
            if (!args.attach && !lazy_load)
            {
                /// load() is called here to force loading the dictionary, wait until the loading is finished,
                /// and throw an exception if the loading is failed.
                external_dictionaries_loader.load(dictionary_id.getInternalDictionaryName());
            }

            return result_storage;
        }
        else
        {
            /// Create dictionary storage that is view of underlying dictionary

            if (args.engine_args.size() != 1)
                throw Exception("Storage Dictionary requires single parameter: name of dictionary",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            args.engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args.engine_args[0], local_context);
            String dictionary_name = args.engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();

            if (!args.attach)
            {
                const auto & dictionary = args.getContext()->getExternalDictionariesLoader().getDictionary(dictionary_name, args.getContext());
                const DictionaryStructure & dictionary_structure = dictionary->getStructure();
                checkNamesAndTypesCompatibleWithDictionary(dictionary_name, args.columns, dictionary_structure);
            }

            return StorageDictionary::create(
                args.table_id, dictionary_name, args.columns, args.comment, StorageDictionary::Location::Custom, local_context);
        }
    });
}

}
