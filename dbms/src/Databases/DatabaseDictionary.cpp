#include <Databases/DatabasesCommon.h>
#include <Databases/DatabaseDictionary.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionaries.h>
#include <Storages/StorageDictionary.h>
#include <common/logger_useful.h>
#include <Parsers/IAST.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Poco/DirectoryIterator.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int DICTIONARY_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int SYNTAX_ERROR;
    extern const int INCORRECT_FILE_NAME;
    extern const int CANNOT_CREATE_DICTIONARY_FROM_METADATA;
}


namespace
{
    constexpr size_t METADATA_FILE_BUFFER_SIZE = 32768;

    void loadDictionary(
        Context & context,
        DatabaseDictionary & database,
        const String & database_name,
        const Poco::Path & dictionaries_metadata_path,
        const String & filename)
    {
        Logger * log = &Logger::get("loadDictionary");
        Poco::Path dictionary_metadata_path = dictionaries_metadata_path;
        dictionary_metadata_path.append(filename);

        String s;
        {
            char in_buf[METADATA_FILE_BUFFER_SIZE];
            ReadBufferFromFile in(dictionary_metadata_path.toString(), METADATA_FILE_BUFFER_SIZE, -1, in_buf);
            readStringUntilEOF(s, in);
        }

        if (s.empty())
        {
            LOG_ERROR(log, "File " << dictionary_metadata_path.toString() << " is empty. Removing");
            Poco::File(dictionary_metadata_path).remove();
            return;
        }

        try
        {
            // TODO: убрать возврат конфигурации из createDictionaryFromDefinition
            auto [dictionary_name, dictionary_ptr] = createDictionaryFromDefinition(
                s, database_name, context, "in file " + dictionary_metadata_path.toString());
            database.attachDictionary(dictionary_name, dictionary_ptr);

            // TODO: тут лучше передавать database_name.dictionary_name
            context.getExternalDictionaries().addObjectFromDDL(dictionary_name, dictionary_ptr);
        }
        catch (const Exception & e)
        {
            throw Exception("Cannot create dictionary from metadata file " + dictionary_metadata_path.toString() + ", error: " + e.displayText() +
                ", stack trace:\n" + e.getStackTrace().toString(),
                ErrorCodes::CANNOT_CREATE_DICTIONARY_FROM_METADATA);
        }
    }
}

DatabaseDictionary::DatabaseDictionary(const String & name_, const Poco::Path & metadata_path_, const Context & context_)
    : name(name_)
    , metadata_path(metadata_path_)
    , dictionaries_metadata_path(metadata_path_)
    , external_dictionaries(context_.getExternalDictionaries())
    , log(&Logger::get("DatabaseDictionary(" + name + ")"))
{
    dictionaries_metadata_path.append("dictionaries");
    try
    {
        Poco::File(dictionaries_metadata_path).createDirectory();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Can't create directory for dictionary database metadata");
    }
}

void DatabaseDictionary::loadTables(Context &, ThreadPool *, bool)
{
}

Tables DatabaseDictionary::loadTables()
{
    auto objects_map = external_dictionaries.getObjectsMap();
    const auto & dictionaries = objects_map.get();

    Tables tables;
    for (const auto & pair : dictionaries)
    {
        const std::string & dict_name = pair.first;
        if (deleted_tables.count(dict_name))
            continue;
        auto dict_ptr = std::static_pointer_cast<IDictionaryBase>(pair.second.loadable);
        if (dict_ptr)
        {
            const DictionaryStructure & dictionary_structure = dict_ptr->getStructure();
            auto columns = StorageDictionary::getNamesAndTypes(dictionary_structure);
            tables[dict_name] = StorageDictionary::create(
                dict_name,
                name,
                ColumnsDescription{columns},
                std::cref(dictionary_structure),
                dict_name,
                false);
        }
    }

    return tables;
}


void DatabaseDictionary::loadDictionaries(Context & context, ThreadPool *, bool)
{
    using FileNames = std::vector<std::string>;
    FileNames file_names;

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it(dictionaries_metadata_path); dir_it != dir_end; ++dir_it)
    {
        const String & filename = dir_it.name();
        /// For '.svn', '.gitignore' and similar
        if (filename.at(0) == '.')
            continue;

        /// There are files .sql.tmp - delete.
        if (endsWith(filename, ".sql.tmp"))
        {
            LOG_INFO(log, "Removing file " << filename);
            Poco::File(filename).remove();
            continue;
        }

        /// The required files have names like `dictionary_name.sql`
        if (endsWith(filename, ".sql"))
            file_names.push_back(filename);
        else
            throw Exception("Incorrect file extension: " + filename + " in metadata directory " + dictionaries_metadata_path.toString(),
                ErrorCodes::INCORRECT_FILE_NAME);
    }

    if (file_names.empty())
        return;

    /*
     * TODO: explanation about placing on disk and ext4 filesystem.
     */
    std::sort(file_names.begin(), file_names.end());
    size_t total_dictionaries = file_names.size();
    LOG_INFO(log, "Total " << total_dictionaries << " dictionaries.");

    for (const auto & filename : file_names)
    {
        loadDictionary(context, *this, name, dictionaries_metadata_path, filename);
    }
}

bool DatabaseDictionary::isTableExist(
    const Context & /*context*/,
    const String & table_name) const
{
    auto objects_map = external_dictionaries.getObjectsMap();
    const auto & dictionaries = objects_map.get();
    return dictionaries.count(table_name) && !deleted_tables.count(table_name);
}


bool DatabaseDictionary::isDictionaryExist(const Context & /*context*/, const String & dictionary_name) const
{
    std::lock_guard lock(mutex);

    if (dictionaries.count(dictionary_name))
        return true;

    auto objects_map = external_dictionaries.getObjectsMap();
    const auto & dicts = objects_map.get();
    return static_cast<bool>(dicts.count(dictionary_name));
}


StoragePtr DatabaseDictionary::tryGetTable(
    const Context & /*context*/,
    const String & table_name) const
{
    auto objects_map = external_dictionaries.getObjectsMap();
    const auto & dictionaries = objects_map.get();

    if (deleted_tables.count(table_name))
        return {};
    {
        auto it = dictionaries.find(table_name);
        if (it != dictionaries.end())
        {
            const auto & dict_ptr = std::static_pointer_cast<IDictionaryBase>(it->second.loadable);
            if (dict_ptr)
            {
                const DictionaryStructure & dictionary_structure = dict_ptr->getStructure();
                auto columns = StorageDictionary::getNamesAndTypes(dictionary_structure);
                return StorageDictionary::create(
                    table_name,
                    name,
                    ColumnsDescription{columns},
                    std::cref(dictionary_structure),
                    table_name,
                    false);
            }
        }
    }

    return {};
}


DictionaryPtr DatabaseDictionary::tryGetDictionary(const Context & context, const String & dictionary_name) const
{
    {
        std::lock_guard lock(mutex);
        auto it = dictionaries.find(dictionary_name);
        if (it != dictionaries.end())
            return it->second;
    }

    return context.getExternalDictionaries().tryGetDictionary(dictionary_name);
}


DatabaseIteratorPtr DatabaseDictionary::getIterator(const Context & /*context*/)
{
    std::lock_guard lock(mutex);
    return std::make_unique<DatabaseSnapshotIterator>(loadTables());
}


DatabaseIteratorPtr DatabaseDictionary::getDictionaryIterator(const Context & /*context*/)
{
    std::lock_guard lock(mutex);
    return std::make_unique<DatabaseSnapshotDictionariesIterator>(dictionaries);
}

bool DatabaseDictionary::empty(const Context & /*context*/) const
{
    auto objects_map = external_dictionaries.getObjectsMap();
    const auto & dictionaries = objects_map.get();
    for (const auto & pair : dictionaries)
        if (pair.second.loadable && !deleted_tables.count(pair.first))
            return false;
    return true;
}

StoragePtr DatabaseDictionary::detachTable(const String & /*table_name*/)
{
    throw Exception("DatabaseDictionary: detachTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DatabaseDictionary::attachTable(const String & /*table_name*/, const StoragePtr & /*table*/)
{
    throw Exception("DatabaseDictionary: attachTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DatabaseDictionary::createTable(
    const Context & /*context*/,
    const String & /*table_name*/,
    const StoragePtr & /*table*/,
    const ASTPtr & /*query*/)
{
    throw Exception("DatabaseDictionary: createTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}


void DatabaseDictionary::createDictionary(Context & context,
                                          const String & dictionary_name,
                                          const DictionaryPtr & dict_ptr,
                                          const ASTPtr & query)
{
    const auto & settings = context.getSettingsRef();
    if (isDictionaryExist(context, dictionary_name))
        throw Exception("Dictionary " + dictionary_name + " already exists.", ErrorCodes::DICTIONARY_ALREADY_EXISTS);

    String dictionary_metadata_path = getDictionaryMetadataPath(dictionary_name);
    String dictionary_metadata_tmp_path = dictionary_metadata_path + ".tmp";
    String statement;

    // TODO: here is a problem that `dictionaries` is missing by default and would be great to create it.
    {
        statement = getDictionaryDefinitionFromCreateQuery(query);
        WriteBufferFromFile out(dictionary_metadata_tmp_path, statement.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(statement, out);
        out.next();
        if (settings.fsync_metadata)
            out.sync();
        out.close();
    }

    try
    {
        {
            std::lock_guard lock(mutex);
            if (!dictionaries.emplace(dictionary_name, dict_ptr).second)
                throw Exception("Dictionary " + dictionary_name + " already exists.", ErrorCodes::DICTIONARY_ALREADY_EXISTS);

            auto info = ExternalLoader::LoadableInfo{
                dict_ptr,
                ExternalLoader::ConfigurationSourceType::DDL,
                dictionary_name,
                /* exception_ptr */ {},
            };

            context.getExternalDictionaries().addObjectFromDDL(dictionary_name, dict_ptr);
        }

        Poco::File(dictionary_metadata_tmp_path).renameTo(dictionary_metadata_path);
    }
    catch (...)
    {
        Poco::File(dictionary_metadata_tmp_path).remove();
    }
}


void DatabaseDictionary::attachDictionary(const String & dictionary_name, DictionaryPtr dictionary)
{
    std::lock_guard lock(mutex);
    if (!dictionaries.emplace(dictionary_name, dictionary).second)
        throw Exception("Database " + name + ". " + dictionary_name + " already exists.", ErrorCodes::DICTIONARY_ALREADY_EXISTS);
}

void DatabaseDictionary::removeTable(
    const Context & context,
    const String & table_name)
{
    if (!isTableExist(context, table_name))
        throw Exception("Table " + name + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

    auto objects_map = external_dictionaries.getObjectsMap();
    deleted_tables.insert(table_name);
}


void DatabaseDictionary::removeDictionary(Context & context, const String & dictionary_name)
{
    std::lock_guard lock{mutex};
    if (dictionaries.count(dictionary_name) == 0)
        return; // TODO: maybe throw an exception would be better

    context.getExternalDictionaries().removeObject(dictionary_name);
    dictionaries.erase(dictionary_name);
}


void DatabaseDictionary::renameTable(
    const Context &,
    const String &,
    IDatabase &,
    const String &)
{
    throw Exception("DatabaseDictionary: renameTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DatabaseDictionary::alterTable(
    const Context &,
    const String &,
    const ColumnsDescription &,
    const ASTModifier &)
{
    throw Exception("DatabaseDictionary: alterTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

time_t DatabaseDictionary::getTableMetadataModificationTime(
    const Context &,
    const String &)
{
    return static_cast<time_t>(0);
}

ASTPtr DatabaseDictionary::getCreateTableQueryImpl(const Context & context,
                                                   const String & table_name, bool throw_on_error) const
{
    String query;
    {
        WriteBufferFromString buffer(query);

        const auto & dictionaries = context.getExternalDictionaries();
        auto dictionary = throw_on_error ? dictionaries.getDictionary(table_name)
                                         : dictionaries.tryGetDictionary(table_name);

        auto names_and_types = StorageDictionary::getNamesAndTypes(dictionary->getStructure());
        buffer << "CREATE TABLE " << backQuoteIfNeed(name) << '.' << backQuoteIfNeed(table_name) << " (";
        buffer << StorageDictionary::generateNamesAndTypesDescription(names_and_types.begin(), names_and_types.end());
        buffer << ") Engine = Dictionary(" << backQuoteIfNeed(table_name) << ")";
    }

    ParserCreateQuery parser;
    const char * pos = query.data();
    std::string error_message;
    auto ast = tryParseQuery(parser, pos, pos + query.size(), error_message,
            /* hilite = */ false, "", /* allow_multi_statements = */ false, 0);

    if (!ast && throw_on_error)
        throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);

    return ast;
}

ASTPtr DatabaseDictionary::getCreateTableQuery(const Context & context, const String & table_name) const
{
    return getCreateTableQueryImpl(context, table_name, true);
}

ASTPtr DatabaseDictionary::tryGetCreateTableQuery(const Context & context, const String & table_name) const
{
    return getCreateTableQueryImpl(context, table_name, false);
}

ASTPtr DatabaseDictionary::getCreateDatabaseQuery(const Context & /*context*/) const
{
    String query;
    {
        WriteBufferFromString buffer(query);
        buffer << "CREATE DATABASE " << backQuoteIfNeed(name) << " ENGINE = Dictionary";
    }
    ParserCreateQuery parser;
    return parseQuery(parser, query.data(), query.data() + query.size(), "", 0);
}

void DatabaseDictionary::shutdown()
{
}


String DatabaseDictionary::getMetadataPath() const
{
    return metadata_path.toString();
}


String DatabaseDictionary::getDictionaryMetadataPath(const String & dictionary_name) const
{
    Poco::Path dictionary_metadata_path = dictionaries_metadata_path;
    dictionary_metadata_path.append(escapeForFileName(dictionary_name) + ".sql");
    return dictionary_metadata_path.toString();
}


String DatabaseDictionary::getDatabaseName() const
{
    return name;
}

}
