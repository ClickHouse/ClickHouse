#pragma once

#include <Databases/DatabasesCommon.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage_fwd.h>

#include <mutex>
#include <unordered_set>


namespace Poco
{
    class Logger;
}


namespace DB
{

/* Database to store StorageDictionary tables
 * automatically creates tables for all dictionaries
 */
class DatabaseDictionary final : public IDatabase, WithContext
{
public:
    DatabaseDictionary(const String & name_, ContextPtr context_);

    String getEngineName() const override
    {
        return "Dictionary";
    }

    bool isTableExist(const String & table_name, ContextPtr context) const override;

    StoragePtr tryGetTable(const String & table_name, ContextPtr context) const override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const override;

    bool empty() const override;

    ASTPtr getCreateDatabaseQuery() const override;

    bool shouldBeEmptyOnDetach() const override { return false; }

    void shutdown() override;

protected:
    ASTPtr getCreateTableQueryImpl(const String & table_name, ContextPtr context, bool throw_on_error) const override;

private:
    LoggerPtr log;

    Tables listTables(const FilterByNameFunction & filter_by_name) const;
};

}
