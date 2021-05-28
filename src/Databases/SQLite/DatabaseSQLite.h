#pragma once

#include <sqlite3.h>

#include <Databases/DatabasesCommon.h>
#include <Parsers/ASTCreateQuery.h>


namespace DB
{
class DatabaseSQLite final : public IDatabase, protected WithContext
{
public:
    DatabaseSQLite(ContextPtr context_, const ASTStorage * database_engine_define_, const String & database_path_);

    String getEngineName() const override { return "SQLite"; }

    bool canContainMergeTreeTables() const override { return false; }
    bool canContainDistributedTables() const override { return false; }
    bool shouldBeEmptyOnDetach() const override { return false; }

    bool isTableExist(const String & name, ContextPtr context) const override;
    StoragePtr tryGetTable(const String & name, ContextPtr context) const override;
    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr context, const FilterByNameFunction & filter_by_table_name) override;
    bool empty() const override;
    ASTPtr getCreateDatabaseQuery() const override;
    void shutdown() override;

private:
    ASTPtr database_engine_define;
    std::shared_ptr<sqlite3> db_ptr;

    Poco::Logger * log;

    bool checkSQLiteTable(const String & table_name) const;
    std::unordered_set<std::string> fetchTablesList() const;
    StoragePtr fetchTable(const String & table_name, ContextPtr context, bool table_checked) const;
};

}
