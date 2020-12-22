#include <Databases/DatabaseWithDictionaries.h>
#include <Common/StatusInfo.h>
#include <Common/ExternalLoaderStatus.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ExternalLoaderTempConfigRepository.h>
#include <Interpreters/ExternalLoaderDatabaseConfigRepository.h>
#include <Dictionaries/getDictionaryConfigurationFromAST.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Parsers/ASTCreateQuery.h>
#include <Interpreters/Context.h>
#include <Storages/StorageDictionary.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/File.h>
#include <boost/smart_ptr/make_shared_object.hpp>


namespace CurrentStatusInfo
{
    extern const Status DictionaryStatus;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_GET_CREATE_DICTIONARY_QUERY;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_DICTIONARY;
    extern const int DICTIONARY_ALREADY_EXISTS;
    extern const int FILE_DOESNT_EXIST;
}


void DatabaseWithDictionaries::attachDictionary(const String & dictionary_name, const DictionaryAttachInfo & attach_info)
{
    auto dict_id = StorageID(attach_info.create_query);
    String internal_name = dict_id.getInternalDictionaryName();
    assert(attach_info.create_query->as<const ASTCreateQuery &>().table == dictionary_name);
    assert(!dict_id.database_name.empty());
    {
        std::unique_lock lock(mutex);
        auto [it, inserted] = dictionaries.emplace(dictionary_name, attach_info);
        if (!inserted)
            throw Exception(ErrorCodes::DICTIONARY_ALREADY_EXISTS,
                            "Dictionary {} already exists.", dict_id.getNameForLogs());

        /// Attach the dictionary as table too.
        try
        {
            /// TODO Make StorageDictionary an owner of IDictionaryBase objects.
            /// All DDL operations with dictionaries will work with StorageDictionary table,
            /// and StorageDictionary will be responsible for loading of DDL dictionaries.
            /// ExternalLoaderDatabaseConfigRepository and other hacks related to ExternalLoader
            /// will not be longer required.
            attachTableUnlocked(
                dictionary_name,
                StorageDictionary::create(
                    dict_id,
                    internal_name,
                    ExternalDictionariesLoader::getDictionaryStructure(*attach_info.config),
                    StorageDictionary::Location::SameDatabaseAndNameAsDictionary),
                lock);
        }
        catch (...)
        {
            dictionaries.erase(it);
            throw;
        }
    }

    CurrentStatusInfo::set(CurrentStatusInfo::DictionaryStatus, internal_name, static_cast<Int8>(ExternalLoaderStatus::NOT_LOADED));

    /// We want ExternalLoader::reloadConfig() to find out that the dictionary's config
    /// has been added and in case `dictionaries_lazy_load == false` to load the dictionary.
    reloadDictionaryConfig(internal_name);
}

void DatabaseWithDictionaries::detachDictionary(const String & dictionary_name)
{
    DictionaryAttachInfo attach_info;
    detachDictionaryImpl(dictionary_name, attach_info);
}

void DatabaseWithDictionaries::detachDictionaryImpl(const String & dictionary_name, DictionaryAttachInfo & attach_info)
{
    auto dict_id = StorageID::createEmpty();
    String internal_name;

    {
        std::unique_lock lock(mutex);
        auto it = dictionaries.find(dictionary_name);
        if (it == dictionaries.end())
            throw Exception(ErrorCodes::UNKNOWN_DICTIONARY,
                            "Dictionary {}.{} doesn't exist.", database_name, dictionary_name);
        dict_id = StorageID(it->second.create_query);
        internal_name = dict_id.getInternalDictionaryName();
        assert(dict_id.table_name == dictionary_name);
        assert(!dict_id.database_name.empty());

        attach_info = std::move(it->second);
        dictionaries.erase(it);

        /// Detach the dictionary as table too.
        try
        {
            if (!dict_id.hasUUID())
                detachTableUnlocked(dictionary_name, lock);
        }
        catch (...)
        {
            dictionaries.emplace(dictionary_name, std::move(attach_info));
            throw;
        }
    }

    CurrentStatusInfo::unset(CurrentStatusInfo::DictionaryStatus, internal_name);

    /// We want ExternalLoader::reloadConfig() to find out that the dictionary's config
    /// has been removed and to unload the dictionary.
    reloadDictionaryConfig(internal_name);

    if (dict_id.hasUUID())
        detachTable(dictionary_name);
}

void DatabaseWithDictionaries::createDictionary(const Context & context, const String & dictionary_name, const ASTPtr & query)
{
    const auto & settings = context.getSettingsRef();

    /** The code is based on the assumption that all threads share the same order of operations:
      * - create the .sql.tmp file;
      * - add the dictionary to ExternalDictionariesLoader;
      * - load the dictionary in case dictionaries_lazy_load == false;
      * - attach the dictionary;
      * - rename .sql.tmp to .sql.
      */

    auto dict_id = StorageID(query);
    assert(query->as<const ASTCreateQuery &>().table == dictionary_name);
    assert(!dict_id.database_name.empty());

    /// A race condition would be possible if a dictionary with the same name is simultaneously created using CREATE and using ATTACH.
    /// But there is protection from it - see using DDLGuard in InterpreterCreateQuery.
    if (isDictionaryExist(dictionary_name))
        throw Exception(ErrorCodes::DICTIONARY_ALREADY_EXISTS, "Dictionary {} already exists.", dict_id.getFullTableName());

    /// A dictionary with the same full name could be defined in *.xml config files.
    if (external_loader.getCurrentStatus(dict_id.getFullNameNotQuoted()) != ExternalLoader::Status::NOT_EXIST)
        throw Exception(ErrorCodes::DICTIONARY_ALREADY_EXISTS,
                        "Dictionary {} already exists.", dict_id.getFullNameNotQuoted());

    if (isTableExist(dictionary_name, global_context))
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {} already exists.", dict_id.getFullTableName());

    String dictionary_metadata_path = getObjectMetadataPath(dictionary_name);
    String dictionary_metadata_tmp_path = dictionary_metadata_path + ".tmp";
    String statement = getObjectDefinitionFromCreateQuery(query);

    {
        /// Exclusive flags guarantees, that table is not created right now in another thread. Otherwise, exception will be thrown.
        WriteBufferFromFile out(dictionary_metadata_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (settings.fsync_metadata)
            out.sync();
        out.close();
    }

    bool succeeded = false;
    SCOPE_EXIT({
        if (!succeeded)
            Poco::File(dictionary_metadata_tmp_path).remove();
    });

    /// Add a temporary repository containing the dictionary.
    /// We need this temp repository to try loading the dictionary before actually attaching it to the database.
    auto temp_repository = external_loader.addConfigRepository(std::make_unique<ExternalLoaderTempConfigRepository>(
        getDatabaseName(), dictionary_metadata_tmp_path, getDictionaryConfigurationFromAST(query->as<const ASTCreateQuery &>(), context)));

    bool lazy_load = context.getConfigRef().getBool("dictionaries_lazy_load", true);
    if (!lazy_load)
    {
        /// load() is called here to force loading the dictionary, wait until the loading is finished,
        /// and throw an exception if the loading is failed.
        external_loader.load(dict_id.getInternalDictionaryName());
    }

    auto config = getDictionaryConfigurationFromAST(query->as<const ASTCreateQuery &>(), context);
    attachDictionary(dictionary_name, DictionaryAttachInfo{query, config, time(nullptr)});
    SCOPE_EXIT({
        if (!succeeded)
            detachDictionary(dictionary_name);
    });

    /// If it was ATTACH query and file with dictionary metadata already exist
    /// (so, ATTACH is done after DETACH), then rename atomically replaces old file with new one.
    Poco::File(dictionary_metadata_tmp_path).renameTo(dictionary_metadata_path);

    /// ExternalDictionariesLoader doesn't know we renamed the metadata path.
    /// That's why we have to call ExternalLoader::reloadConfig() here.
    reloadDictionaryConfig(dict_id.getInternalDictionaryName());

    /// Everything's ok.
    succeeded = true;
}

void DatabaseWithDictionaries::removeDictionary(const Context &, const String & dictionary_name)
{
    DictionaryAttachInfo attach_info;
    detachDictionaryImpl(dictionary_name, attach_info);

    try
    {
        String dictionary_metadata_path = getObjectMetadataPath(dictionary_name);
        Poco::File(dictionary_metadata_path).remove();
        CurrentStatusInfo::unset(CurrentStatusInfo::DictionaryStatus,
                                 StorageID(attach_info.create_query).getInternalDictionaryName());
    }
    catch (...)
    {
        /// If remove was not possible for some reason
        attachDictionary(dictionary_name, attach_info);
        throw;
    }

    UUID dict_uuid = attach_info.create_query->as<ASTCreateQuery>()->uuid;
    if (dict_uuid != UUIDHelpers::Nil)
        DatabaseCatalog::instance().removeUUIDMappingFinally(dict_uuid);
}

DatabaseDictionariesIteratorPtr DatabaseWithDictionaries::getDictionariesIterator(const FilterByNameFunction & filter_by_dictionary_name)
{
    std::lock_guard lock(mutex);
    DictionariesWithID filtered_dictionaries;
    for (const auto & dictionary : dictionaries)
    {
        if (filter_by_dictionary_name && !filter_by_dictionary_name(dictionary.first))
            continue;
        filtered_dictionaries.emplace_back();
        filtered_dictionaries.back().first = dictionary.first;
        filtered_dictionaries.back().second = dictionary.second.create_query->as<const ASTCreateQuery &>().uuid;
    }
    return std::make_unique<DatabaseDictionariesSnapshotIterator>(std::move(filtered_dictionaries), database_name);
}

bool DatabaseWithDictionaries::isDictionaryExist(const String & dictionary_name) const
{
    std::lock_guard lock(mutex);
    return dictionaries.find(dictionary_name) != dictionaries.end();
}

ASTPtr DatabaseWithDictionaries::getCreateDictionaryQueryImpl(
        const String & dictionary_name,
        bool throw_on_error) const
{
    {
        /// Try to get create query ifg for an attached dictionary.
        std::lock_guard lock{mutex};
        auto it = dictionaries.find(dictionary_name);
        if (it != dictionaries.end())
        {
            ASTPtr ast = it->second.create_query->clone();
            auto & create_query = ast->as<ASTCreateQuery &>();
            create_query.attach = false;
            create_query.database = database_name;
            return ast;
        }
    }

    /// Try to get create query for non-attached dictionary.
    ASTPtr ast;
    try
    {
        auto dictionary_metadata_path = getObjectMetadataPath(dictionary_name);
        ast = getCreateQueryFromMetadata(dictionary_metadata_path, throw_on_error);
    }
    catch (const Exception & e)
    {
        if (throw_on_error && (e.code() != ErrorCodes::FILE_DOESNT_EXIST))
            throw;
    }

    if (ast)
    {
        const auto * create_query = ast->as<const ASTCreateQuery>();
        if (create_query && create_query->is_dictionary)
            return ast;
    }
    if (throw_on_error)
        throw Exception{"Dictionary " + backQuote(dictionary_name) + " doesn't exist",
                        ErrorCodes::CANNOT_GET_CREATE_DICTIONARY_QUERY};
    return nullptr;
}

Poco::AutoPtr<Poco::Util::AbstractConfiguration> DatabaseWithDictionaries::getDictionaryConfiguration(const String & dictionary_name) const
{
    std::lock_guard lock(mutex);
    auto it = dictionaries.find(dictionary_name);
    if (it != dictionaries.end())
        return it->second.config;
    throw Exception("Dictionary " + backQuote(dictionary_name) + " doesn't exist", ErrorCodes::UNKNOWN_DICTIONARY);
}

time_t DatabaseWithDictionaries::getObjectMetadataModificationTime(const String & object_name) const
{
    {
        std::lock_guard lock(mutex);
        auto it = dictionaries.find(object_name);
        if (it != dictionaries.end())
            return it->second.modification_time;
    }
    return DatabaseOnDisk::getObjectMetadataModificationTime(object_name);
}


bool DatabaseWithDictionaries::empty() const
{
    std::lock_guard lock{mutex};
    return tables.empty() && dictionaries.empty();
}

void DatabaseWithDictionaries::reloadDictionaryConfig(const String & full_name)
{
    /// Ensure that this database is attached to ExternalLoader as a config repository.
    if (!database_as_config_repo_for_external_loader.load())
    {
        auto repository = std::make_unique<ExternalLoaderDatabaseConfigRepository>(*this, global_context);
        auto remove_repository_callback = external_loader.addConfigRepository(std::move(repository));
        database_as_config_repo_for_external_loader = boost::make_shared<ext::scope_guard>(std::move(remove_repository_callback));
    }

    external_loader.reloadConfig(getDatabaseName(), full_name);
}


void DatabaseWithDictionaries::shutdown()
{
    {
        std::lock_guard lock(mutex);
        dictionaries.clear();
    }

    /// Invoke removing the database from ExternalLoader.
    database_as_config_repo_for_external_loader = nullptr;

    DatabaseOnDisk::shutdown();
}


DatabaseWithDictionaries::DatabaseWithDictionaries(
    const String & name, const String & metadata_path_, const String & data_path_, const String & logger, const Context & context)
    : DatabaseOnDisk(name, metadata_path_, data_path_, logger, context)
    , external_loader(context.getExternalDictionariesLoader())
{
}

DatabaseWithDictionaries::~DatabaseWithDictionaries() = default;

}
