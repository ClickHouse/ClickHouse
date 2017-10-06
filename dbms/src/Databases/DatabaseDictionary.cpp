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
    auto objects_map = external_dictionaries.getObjectsMap();
    const auto & dictionaries = std::get<1>(objects_map);

    Tables tables;
    for (const auto & pair : dictionaries)
    {
        const std::string & name = pair.first;
        if (deleted_tables.count(name))
            continue;
        auto dict_ptr = std::static_pointer_cast<IDictionaryBase>(pair.second.loadable);
        if (dict_ptr)
        {
            const DictionaryStructure & dictionary_structure = dict_ptr->getStructure();
            auto columns = StorageDictionary::getNamesAndTypes(dictionary_structure);
            tables[name] = StorageDictionary::create(name, columns, {}, {}, {}, dictionary_structure, name);
        }
    }

    return tables;
}

bool DatabaseDictionary::isTableExist(
    const Context & context,
    const String & table_name) const
{
    auto objects_map = external_dictionaries.getObjectsMap();
    const auto & dictionaries = std::get<1>(objects_map);
    return dictionaries.count(table_name) && !deleted_tables.count(table_name);
}

StoragePtr DatabaseDictionary::tryGetTable(
    const Context & context,
    const String & table_name)
{
    auto objects_map = external_dictionaries.getObjectsMap();
    const auto & dictionaries = std::get<1>(objects_map);

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
                return StorageDictionary::create(table_name, columns, {}, {}, {}, dictionary_structure, table_name);
            }
        }
    }

    return {};
}

DatabaseIteratorPtr DatabaseDictionary::getIterator(const Context & context)
{
    return std::make_unique<DatabaseSnaphotIterator>(loadTables());
}

bool DatabaseDictionary::empty(const Context & context) const
{
    auto objects_map = external_dictionaries.getObjectsMap();
    const auto & dictionaries = std::get<1>(objects_map);
    for (const auto & pair : dictionaries)
        if (pair.second.loadable && !deleted_tables.count(pair.first))
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

void DatabaseDictionary::createTable(
    const Context & context,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query,
    const String & engine)
{
    throw Exception("DatabaseDictionary: createTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
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

void DatabaseDictionary::renameTable(
    const Context & context,
    const String & table_name,
    IDatabase & to_database,
    const String & to_table_name)
{
    throw Exception("DatabaseDictionary: renameTable() is not supported", ErrorCodes::NOT_IMPLEMENTED);
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

time_t DatabaseDictionary::getTableMetadataModificationTime(
    const Context & context,
    const String & table_name)
{
    return static_cast<time_t>(0);
}

ASTPtr DatabaseDictionary::getCreateQuery(
    const Context & context,
    const String & table_name) const
{
    throw Exception("DatabaseDictionary: getCreateQuery() is not supported", ErrorCodes::NOT_IMPLEMENTED);
}

void DatabaseDictionary::shutdown()
{
}

void DatabaseDictionary::drop()
{
    /// Additional actions to delete database are not required.
}

}
