#include <common/logger_useful.h>
#include <Databases/DatabaseDictionary.h>
#include <Databases/DatabasesCommon.h>
#include <Interpreters/Context.h>
#include <Storages/StorageDictionary.h>
#include <Interpreters/ExternalDictionaries.h>

namespace DB
{

namespace ErrorCodes
{
extern const int TABLE_ALREADY_EXISTS;
extern const int UNKNOWN_TABLE;
extern const int LOGICAL_ERROR;
}

void DatabaseDictionary::loadTables(Context & context, ThreadPool * thread_pool, bool has_force_restore_data_flag)
{
    log = &Logger::get("DatabaseDictionary(" + name + ")");

    const auto & external_dictionaries = context.getExternalDictionaries();
    const std::lock_guard<std::mutex> lock_dictionaries{external_dictionaries.dictionaries_mutex};
    const std::lock_guard<std::mutex> lock_tables(mutex);

    for (const auto & pair : external_dictionaries.dictionaries)
    {
        const std::string & name = pair.first;
        auto dict_ptr = pair.second.dict;
        if (dict_ptr)
        {
            const DictionaryStructure & dictionary_structure = dict_ptr->get()->getStructure();
            auto columns = StorageDictionary::getNamesAndTypes(dictionary_structure);
            tables[name] =
                StorageDictionary::create(name, context, columns, {}, {}, {}, dictionary_structure, name);
        }
    }

    for (const auto & pair : external_dictionaries.failed_dictionaries)
    {
        const std::string & name = pair.first;
        const DictionaryStructure & dictionary_structure = pair.second.dict->getStructure();
        auto columns = StorageDictionary::getNamesAndTypes(dictionary_structure);
        tables[name] =
            StorageDictionary::create(name, context, columns, {}, {}, {}, dictionary_structure, name);
    }
}

bool DatabaseDictionary::isTableExist(const String & table_name) const
{
    std::lock_guard<std::mutex> lock(mutex);
    return tables.count(table_name);
}

StoragePtr DatabaseDictionary::tryGetTable(const String & table_name)
{
    std::lock_guard<std::mutex> lock(mutex);
    auto it = tables.find(table_name);
    if (it == tables.end())
        return {};
    return it->second;
}

DatabaseIteratorPtr DatabaseDictionary::getIterator()
{
    std::lock_guard<std::mutex> lock(mutex);
    return std::make_unique<DatabaseSnaphotIterator>(tables);
}

bool DatabaseDictionary::empty() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return tables.empty();
}

StoragePtr DatabaseDictionary::detachTable(const String & table_name)
{
    throw Exception("DatabaseDictionary: detachTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DatabaseDictionary::attachTable(const String & table_name, const StoragePtr & table)
{
    throw Exception("DatabaseDictionary: attachTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DatabaseDictionary::createTable(
    const String & table_name, const StoragePtr & table, const ASTPtr & query, const String & engine, const Settings & settings)
{
    throw Exception("DatabaseDictionary: attachTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DatabaseDictionary::removeTable(const String & table_name)
{
    std::lock_guard<std::mutex> lock(mutex);
    auto it = tables.find(table_name);
    if (it == tables.end())
        throw Exception("Table " + name + "." + table_name + " doesn't exist.", ErrorCodes::TABLE_ALREADY_EXISTS);
    tables.erase(it);
}

void DatabaseDictionary::renameTable(
    const Context & context, const String & table_name, IDatabase & to_database, const String & to_table_name, const Settings & settings)
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
    /// You can not hold a lock during shutdown.
    /// Because inside `shutdown` function tables can work with database, and mutex is not recursive.

    for (auto iterator = getIterator(); iterator->isValid(); iterator->next())
        iterator->table()->shutdown();

    std::lock_guard<std::mutex> lock(mutex);
    tables.clear();
}

void DatabaseDictionary::drop()
{
    /// Additional actions to delete database are not required.
}

void DatabaseDictionary::alterTable(
    const Context & context,
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
