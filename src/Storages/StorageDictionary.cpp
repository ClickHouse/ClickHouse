#include <Access/Common/AccessFlags.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ExternalLoaderDictionaryStorageConfigRepository.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Config/ConfigHelper.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <QueryPipeline/Pipe.h>
#include <IO/Operators.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Storages/AlterCommands.h>
#include <Storages/checkAndGetLiteralArgument.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool dictionary_validate_primary_key_type;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int THERE_IS_NO_COLUMN;
    extern const int CANNOT_DETACH_DICTIONARY_AS_TABLE;
    extern const int DICTIONARY_ALREADY_EXISTS;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    void checkNamesAndTypesCompatibleWithDictionary(const String & dictionary_name, const ColumnsDescription & columns, const DictionaryStructure & dictionary_structure)
    {
        auto dictionary_names_and_types = StorageDictionary::getNamesAndTypes(dictionary_structure, false);
        std::set<NameAndTypePair> names_and_types_set(dictionary_names_and_types.begin(), dictionary_names_and_types.end());

        for (const auto & column : columns.getOrdinary())
        {
            if (names_and_types_set.find(column) == names_and_types_set.end())
            {
                throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Not found column {} {} in dictionary {}. There are only columns {}",
                                column.name, column.type->getName(), backQuote(dictionary_name),
                                dictionary_names_and_types.toNamesAndTypesDescription());
            }
        }
    }
}


NamesAndTypesList StorageDictionary::getNamesAndTypes(const DictionaryStructure & dictionary_structure, bool validate_id_type)
{
    NamesAndTypesList dictionary_names_and_types;

    if (dictionary_structure.id)
    {
        if (validate_id_type && dictionary_structure.id->type->getTypeId() != TypeIndex::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect type of ID column: must be UInt64, but it is {}", dictionary_structure.id->type->getFamilyName());

        dictionary_names_and_types.emplace_back(dictionary_structure.id->name, std::make_shared<DataTypeUInt64>());
    }
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
    const String & comment,
    Location location_,
    ContextPtr context_)
    : StorageDictionary(
          table_id_,
          dictionary_name_,
          ColumnsDescription{getNamesAndTypes(dictionary_structure_, context_->getSettingsRef()[Setting::dictionary_validate_primary_key_type])},
          comment,
          location_,
          context_)
{
}

StorageDictionary::StorageDictionary(
    const StorageID & table_id,
    LoadablesConfigurationPtr dictionary_configuration,
    ContextPtr context_)
    : StorageDictionary(
        table_id,
        table_id.getFullNameNotQuoted(),
        context_->getExternalDictionariesLoader().getDictionaryStructure(*dictionary_configuration), /// NOLINT(readability-static-accessed-through-instance)
        dictionary_configuration->getString("dictionary.comment", ""),
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

void StorageDictionary::checkTableCanBeDropped([[ maybe_unused ]] ContextPtr query_context) const
{
    if (location == Location::SameDatabaseAndNameAsDictionary)
        throw Exception(ErrorCodes::CANNOT_DETACH_DICTIONARY_AS_TABLE,
            "Cannot drop/detach dictionary {} as table, use DROP DICTIONARY or DETACH DICTIONARY query instead",
            dictionary_name);
    if (location == Location::DictionaryDatabase)
        throw Exception(ErrorCodes::CANNOT_DETACH_DICTIONARY_AS_TABLE,
            "Cannot drop/detach table '{}' from a database with DICTIONARY engine, use DROP DICTIONARY or DETACH DICTIONARY query instead",
            dictionary_name);
}

void StorageDictionary::checkTableCanBeDetached() const
{
    /// Actually query context (from DETACH query) should be passed here.
    /// But we don't use it for this type of storage
    checkTableCanBeDropped(getContext());
}

Pipe StorageDictionary::read(
    const Names & column_names,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & /*query_info*/,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const size_t threads)
{
    auto registered_dictionary_name = location == Location::SameDatabaseAndNameAsDictionary ? getStorageID().getInternalDictionaryName() : dictionary_name;
    auto dictionary = getContext()->getExternalDictionariesLoader().getDictionary(registered_dictionary_name, local_context);
    local_context->checkAccess(AccessType::dictGet, dictionary->getDatabaseOrNoDatabaseTag(), dictionary->getDictionaryID().getTableName());
    return dictionary->read(column_names, max_block_size, threads);
}

std::shared_ptr<const IDictionary> StorageDictionary::getDictionary() const
{
    auto registered_dictionary_name = location == Location::SameDatabaseAndNameAsDictionary ? getStorageID().getInternalDictionaryName() : dictionary_name;
    return getContext()->getExternalDictionariesLoader().getDictionary(registered_dictionary_name, getContext());
}

void StorageDictionary::shutdown(bool)
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
    remove_repository_callback.reset();
}

LoadablesConfigurationPtr StorageDictionary::getConfiguration() const
{
    std::lock_guard lock(dictionary_config_mutex);
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

    /// It's better not to update an associated `IDictionary` directly here because it can be not loaded yet or
    /// it can be in the process of loading or reloading right now.
    /// The correct way is to update the dictionary's configuration first and then ask ExternalDictionariesLoader to reload our dictionary.

    {
        std::lock_guard lock(dictionary_config_mutex);
        auto new_configuration = ConfigHelper::clone(*configuration);

        new_configuration->setString("dictionary.database", new_table_id.database_name);
        new_configuration->setString("dictionary.name", new_table_id.table_name);

        if (move_to_atomic)
            new_configuration->setString("dictionary.uuid", toString(new_table_id.uuid));
        else if (move_to_ordinary)
            new_configuration->remove("dictionary.uuid");

        configuration = new_configuration;
    }

    const auto & external_dictionaries_loader = getContext()->getExternalDictionariesLoader();

    /// Dictionary is moving between databases of different engines or is renaming inside Ordinary database
    bool recreate_dictionary = old_table_id.uuid == UUIDHelpers::Nil || new_table_id.uuid == UUIDHelpers::Nil;

    if (recreate_dictionary)
    {
        /// For an ordinary database the config repositories of dictionaries are identified by the full name (database name + dictionary name),
        /// so we cannot change the dictionary name or the database name on the fly (without extra reloading) and have to recreate the config repository.
        removeDictionaryConfigurationFromRepository();
        auto repository = std::make_unique<ExternalLoaderDictionaryStorageConfigRepository>(*this);
        remove_repository_callback = external_dictionaries_loader.addConfigRepository(std::move(repository));
        /// Dictionary will be now reloaded lazily.
    }
    else
    {
        /// For an atomic database dictionaries are identified inside the ExternalLoader by UUID,
        /// so we can change the dictionary name or the database name on the fly (without extra reloading) because UUID doesn't change.
        external_dictionaries_loader.reloadConfig(old_table_id.getInternalDictionaryName());
    }

    dictionary_name = new_table_id.getFullNameNotQuoted();
}

void StorageDictionary::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /* context */) const
{
    for (const auto & command : commands)
    {
        if (location == Location::DictionaryDatabase || command.type != AlterCommand::COMMENT_TABLE)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}",
                command.type, getName());
    }
}

void StorageDictionary::alter(const AlterCommands & params, ContextPtr alter_context, AlterLockHolder & lock_holder)
{
    IStorage::alter(params, alter_context, lock_holder);

    if (location == Location::Custom)
        return;

    auto new_comment = getInMemoryMetadataPtr()->comment;

    /// It's better not to update an associated `IDictionary` directly here because it can be not loaded yet or
    /// it can be in the process of loading or reloading right now.
    /// The correct way is to update the dictionary's configuration first and then ask ExternalDictionariesLoader to reload our dictionary.

    {
        std::lock_guard lock(dictionary_config_mutex);
        auto new_configuration = ConfigHelper::clone(*configuration);
        new_configuration->setString("dictionary.comment", new_comment);
        configuration = new_configuration;
    }

    const auto & external_dictionaries_loader = getContext()->getExternalDictionariesLoader();
    external_dictionaries_loader.reloadConfig(getStorageID().getInternalDictionaryName());
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
            auto result_storage = std::make_shared<StorageDictionary>(dictionary_id, abstract_dictionary_configuration, local_context);

            bool lazy_load = local_context->getConfigRef().getBool("dictionaries_lazy_load", true);
            if (args.mode <= LoadingStrictnessLevel::CREATE && !lazy_load)
            {
                /// load() is called here to force loading the dictionary, wait until the loading is finished,
                /// and throw an exception if the loading is failed.
                external_dictionaries_loader.load(dictionary_id.getInternalDictionaryName());
            }

            return result_storage;
        }

        /// Create dictionary storage that is view of underlying dictionary

        if (args.engine_args.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Storage Dictionary requires single parameter: name of dictionary");

        args.engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args.engine_args[0], local_context);
        String dictionary_name = checkAndGetLiteralArgument<String>(args.engine_args[0], "dictionary_name");

        if (args.mode <= LoadingStrictnessLevel::CREATE)
        {
            const auto & dictionary = args.getContext()->getExternalDictionariesLoader().getDictionary(dictionary_name, args.getContext());
            const DictionaryStructure & dictionary_structure = dictionary->getStructure();
            checkNamesAndTypesCompatibleWithDictionary(dictionary_name, args.columns, dictionary_structure);
        }

        return std::make_shared<StorageDictionary>(
            args.table_id, dictionary_name, args.columns, args.comment, StorageDictionary::Location::Custom, local_context);
    });
}

}
