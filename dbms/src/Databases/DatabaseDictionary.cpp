#include <Databases/DatabaseDictionary.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionaries.h>
#include <Storages/StorageDictionary.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TABLE_ALREADY_EXISTS;
extern const int UNKNOWN_TABLE;
extern const int LOGICAL_ERROR;
}

DatabaseDictionary::DatabaseDictionary(const String & name_, const Context & context)
    : name(name_),
      external_dictionaries(context.getExternalDictionaries()),
      log(&Logger::get("DatabaseDictionary(" + name + ")"))
{
}

void DatabaseDictionary::loadTables(Context & context, ThreadPool * thread_pool, bool has_force_restore_data_flag)
{
}

Tables DatabaseDictionary::loadTables()
{
    const std::lock_guard<std::mutex> lock_dictionaries {external_dictionaries.dictionaries_mutex};

    Tables tables;
    for (const auto & pair : external_dictionaries.dictionaries)
    {
        const std::string & name = pair.first;
        if (deleted_tables.count(name))
            continue;
        auto dict_ptr = pair.second.dict;
        if (dict_ptr)
        {
            const DictionaryStructure & dictionary_structure = dict_ptr->get()->getStructure();
            auto columns = StorageDictionary::getNamesAndTypes(dictionary_structure);
            tables[name] = StorageDictionary::create(name, columns, {}, {}, {}, dictionary_structure, name);
        }
    }

    return tables;
}

bool DatabaseDictionary::isTableExist(const String & table_name) const
{
    const std::lock_guard<std::mutex> lock_dictionaries {external_dictionaries.dictionaries_mutex};
    return external_dictionaries.dictionaries.count(table_name) && !deleted_tables.count(table_name);
}

StoragePtr DatabaseDictionary::tryGetTable(const String & table_name)
{
    const std::lock_guard<std::mutex> lock_dictionaries {external_dictionaries.dictionaries_mutex};

    if (deleted_tables.count(table_name))
        return {};
    {
        auto it = external_dictionaries.dictionaries.find(table_name);
        if (it != external_dictionaries.dictionaries.end())
        {
            const auto & dict_ptr = it->second.dict;
            if (dict_ptr)
            {
                const DictionaryStructure & dictionary_structure = dict_ptr->get()->getStructure();
                auto columns = StorageDictionary::getNamesAndTypes(dictionary_structure);
                return StorageDictionary::create(table_name, columns, {}, {}, {}, dictionary_structure, table_name);
            }
        }
    }

    return {};
}

DatabaseIteratorPtr DatabaseDictionary::getIterator()
{
    return std::make_unique<DatabaseSnaphotIterator>(loadTables());
}

bool DatabaseDictionary::empty() const
{
    const std::lock_guard<std::mutex> lock_dictionaries {external_dictionaries.dictionaries_mutex};
    for (const auto & pair : external_dictionaries.dictionaries)
        if (pair.second.dict && !deleted_tables.count(pair.first))
            return false;
    return true;
}

StoragePtr DatabaseDictionary::detachTable(const String & table_name)
{
    throw Exception("DatabaseDictionary: detachTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DatabaseDictionary::attachTable(const String & table_name, const StoragePtr & table)
{
    throw Exception("DatabaseDictionary: attachTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DatabaseDictionary::createTable(const String & table_name,
                                     const StoragePtr & table,
                                     const ASTPtr & query,
                                     const String & engine,
                                     const Settings & settings)
{
    throw Exception("DatabaseDictionary: createTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DatabaseDictionary::removeTable(const String & table_name)
{
    if (!isTableExist(table_name))
        throw Exception("Table " + name + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

    const std::lock_guard<std::mutex> lock_dictionaries {external_dictionaries.dictionaries_mutex};
    deleted_tables.insert(table_name);
}

void DatabaseDictionary::renameTable(const Context & context,
                                     const String & table_name,
                                     IDatabase & to_database,
                                     const String & to_table_name,
                                     const Settings & settings)
{
    throw Exception("DatabaseDictionary: renameTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

time_t DatabaseDictionary::getTableMetadataModificationTime(const String & table_name)
{
    return static_cast<time_t>(0);
}

ASTPtr DatabaseDictionary::getCreateQuery(const String & table_name) const
{
    throw Exception("DatabaseDictionary: getCreateQuery() is not supported", ErrorCodes::NOT_IMPLEMENTED);
    return nullptr;
}

void DatabaseDictionary::shutdown()
{
}

void DatabaseDictionary::drop()
{
    /// Additional actions to delete database are not required.
}

void DatabaseDictionary::alterTable(const Context & context,
                                    const String & name,
                                    const NamesAndTypesList & columns,
                                    const NamesAndTypesList & materialized_columns,
                                    const NamesAndTypesList & alias_columns,
                                    const ColumnDefaults & column_defaults,
                                    const ASTModifier & engine_modifier)
{
    throw Exception("DatabaseDictionary: alterTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}
}
