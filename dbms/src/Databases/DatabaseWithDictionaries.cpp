#include <Databases/DatabaseWithDictionaries.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/Context.h>
#include <Storages/StorageDictionary.h>
#include <IO/WriteBufferFromFile.h>
#include <Poco/File.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int DICTIONARY_ALREADY_EXISTS;
}


void DatabaseWithDictionaries::attachDictionary(const String & dictionary_name, const Context & context, bool reload)
{
    const auto & external_loader = context.getExternalDictionariesLoader();

    String full_name = getDatabaseName() + "." + dictionary_name;
    {
        std::lock_guard lock(mutex);
        auto status = external_loader.getCurrentStatus(full_name);
        if (status != ExternalLoader::Status::NOT_EXIST || !dictionaries.emplace(dictionary_name).second)
            throw Exception(
                    "Dictionary " + full_name + " already exists.",
                    ErrorCodes::DICTIONARY_ALREADY_EXISTS);
    }

    if (reload)
        external_loader.reload(full_name, true);
}

void DatabaseWithDictionaries::detachDictionary(const String & dictionary_name, const Context & context, bool reload)
{
    {
        std::lock_guard lock(mutex);
        auto it = dictionaries.find(dictionary_name);
        if (it == dictionaries.end())
            throw Exception("Dictionary " + database_name + "." + dictionary_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
        dictionaries.erase(it);
    }

    if (reload)
        context.getExternalDictionariesLoader().reload(getDatabaseName() + "." + dictionary_name);

}

void DatabaseWithDictionaries::createDictionary(const Context & context, const String & dictionary_name, const ASTPtr & query)
{
    const auto & settings = context.getSettingsRef();

    /** The code is based on the assumption that all threads share the same order of operations
      * - creating the .sql.tmp file;
      * - adding a dictionary to `dictionaries`;
      * - rename .sql.tmp to .sql.
      */

    /// A race condition would be possible if a dictionary with the same name is simultaneously created using CREATE and using ATTACH.
    /// But there is protection from it - see using DDLGuard in InterpreterCreateQuery.
    if (isDictionaryExist(context, dictionary_name))
        throw Exception("Dictionary " + backQuote(getDatabaseName()) + "." + backQuote(dictionary_name) + " already exists.", ErrorCodes::DICTIONARY_ALREADY_EXISTS);

    if (isTableExist(context, dictionary_name))
        throw Exception("Table " + backQuote(getDatabaseName()) + "." + backQuote(dictionary_name) + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);


    String dictionary_metadata_path = getObjectMetadataPath(dictionary_name);
    String dictionary_metadata_tmp_path = dictionary_metadata_path + ".tmp";
    String statement;

    {
        statement = getObjectDefinitionFromCreateQuery(query);

        /// Exclusive flags guarantees, that table is not created right now in another thread. Otherwise, exception will be thrown.
        WriteBufferFromFile out(dictionary_metadata_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (settings.fsync_metadata)
            out.sync();
        out.close();
    }

    try
    {
        /// Do not load it now because we want more strict loading
        attachDictionary(dictionary_name, context, false);
        /// Load dictionary
        bool lazy_load = context.getConfigRef().getBool("dictionaries_lazy_load", true);
        String dict_name = getDatabaseName() + "." + dictionary_name;
        context.getExternalDictionariesLoader().addDictionaryWithConfig(
                dict_name, getDatabaseName(), query->as<const ASTCreateQuery &>(), !lazy_load);

        /// If it was ATTACH query and file with dictionary metadata already exist
        /// (so, ATTACH is done after DETACH), then rename atomically replaces old file with new one.
        Poco::File(dictionary_metadata_tmp_path).renameTo(dictionary_metadata_path);

    }
    catch (...)
    {
        detachDictionary(dictionary_name, context);
        Poco::File(dictionary_metadata_tmp_path).remove();
        throw;
    }
}

void DatabaseWithDictionaries::removeDictionary(const Context & context, const String & dictionary_name)
{
    detachDictionary(dictionary_name, context);

    String dictionary_metadata_path = getObjectMetadataPath(dictionary_name);

    try
    {
        Poco::File(dictionary_metadata_path).remove();
    }
    catch (...)
    {
        /// If remove was not possible for some reason
        attachDictionary(dictionary_name, context);
        throw;
    }
}

StoragePtr DatabaseWithDictionaries::tryGetTable(const Context & context, const String & table_name) const
{
    if (auto table_ptr = DatabaseWithOwnTablesBase::tryGetTable(context, table_name))
        return table_ptr;

    if (isDictionaryExist(context, table_name))
        /// We don't need lock database here, because database doesn't store dictionary itself
        /// just metadata
        return getDictionaryStorage(context, table_name);

    return {};
}

DatabaseTablesIteratorPtr DatabaseWithDictionaries::getTablesWithDictionaryTablesIterator(const Context & context, const FilterByNameFunction & filter_by_name)
{
    /// NOTE: it's not atomic
    auto tables_it = getTablesIterator(context, filter_by_name);
    auto dictionaries_it = getDictionariesIterator(context, filter_by_name);

    Tables result;
    while (tables_it && tables_it->isValid())
    {
        result.emplace(tables_it->name(), tables_it->table());
        tables_it->next();
    }

    while (dictionaries_it && dictionaries_it->isValid())
    {
        auto table_name = dictionaries_it->name();
        auto table_ptr = getDictionaryStorage(context, table_name);
        if (table_ptr)
            result.emplace(table_name, table_ptr);
        dictionaries_it->next();
    }

    return std::make_unique<DatabaseTablesSnapshotIterator>(result);
}

DatabaseDictionariesIteratorPtr DatabaseWithDictionaries::getDictionariesIterator(const Context & /*context*/, const FilterByNameFunction & filter_by_dictionary_name)
{
    std::lock_guard lock(mutex);
    if (!filter_by_dictionary_name)
        return std::make_unique<DatabaseDictionariesSnapshotIterator>(dictionaries);

    Dictionaries filtered_dictionaries;
    for (const auto & dictionary_name : dictionaries)
        if (filter_by_dictionary_name(dictionary_name))
            filtered_dictionaries.emplace(dictionary_name);
    return std::make_unique<DatabaseDictionariesSnapshotIterator>(std::move(filtered_dictionaries));
}

bool DatabaseWithDictionaries::isDictionaryExist(const Context & /*context*/, const String & dictionary_name) const
{
    std::lock_guard lock(mutex);
    return dictionaries.find(dictionary_name) != dictionaries.end();
}

StoragePtr DatabaseWithDictionaries::getDictionaryStorage(const Context & context, const String & table_name) const
{
    auto dict_name = database_name + "." + table_name;
    const auto & external_loader = context.getExternalDictionariesLoader();
    auto dict_ptr = external_loader.tryGetDictionary(dict_name);
    if (dict_ptr)
    {
        const DictionaryStructure & dictionary_structure = dict_ptr->getStructure();
        auto columns = StorageDictionary::getNamesAndTypes(dictionary_structure);
        return StorageDictionary::create(StorageID(database_name, table_name), ColumnsDescription{columns}, context, true, dict_name);
    }
    return nullptr;
}

ASTPtr DatabaseWithDictionaries::getCreateDictionaryQueryImpl(
        const Context & context,
        const String & dictionary_name,
        bool throw_on_error) const
{
    ASTPtr ast;

    auto dictionary_metadata_path = getObjectMetadataPath(dictionary_name);
    ast = getCreateQueryFromMetadata(dictionary_metadata_path, throw_on_error);
    if (!ast && throw_on_error)
    {
        /// Handle system.* tables for which there are no table.sql files.
        bool has_dictionary = isDictionaryExist(context, dictionary_name);

        auto msg = has_dictionary ? "There is no CREATE DICTIONARY query for table " : "There is no metadata file for dictionary ";

        throw Exception(msg + backQuote(dictionary_name), ErrorCodes::CANNOT_GET_CREATE_DICTIONARY_QUERY);
    }

    return ast;
}

}
