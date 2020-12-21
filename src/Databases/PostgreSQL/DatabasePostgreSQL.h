#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX

#include <Core/MultiEnum.h>
#include <Common/ThreadPool.h>
#include <Databases/DatabasesCommon.h>
#include <Parsers/ASTCreateQuery.h>

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

#include <Storages/StoragePostgreSQL.h>
#include <pqxx/pqxx>

namespace DB
{

class Context;

class DatabasePostgreSQL final : public IDatabase
{

public:
    DatabasePostgreSQL(
        const Context & context,
        const String & metadata_path_,
        const ASTStorage * database_engine_define,
        const String & dbname_,
        const String & postgres_dbname,
        PGConnectionPtr connection_);

    String getEngineName() const override { return "PostgreSQL"; }
    String getMetadataPath() const override { return metadata_path; };

    bool canContainMergeTreeTables() const override { return false; }
    bool canContainDistributedTables() const override { return false; }
    bool shouldBeEmptyOnDetach() const override { return false; }

    ASTPtr getCreateDatabaseQuery() const override;

    bool empty() const override;

    DatabaseTablesIteratorPtr getTablesIterator(const Context & context, const FilterByNameFunction & filter_by_table_name) override;

    bool isTableExist(const String & name, const Context & context) const override;
    StoragePtr tryGetTable(const String & name, const Context & context) const override;

    void createTable(const Context &, const String & table_name, const StoragePtr & storage, const ASTPtr & create_query) override;
    void dropTable(const Context &, const String & table_name, bool no_delay) override;

    void attachTable(const String & table_name, const StoragePtr & storage, const String & relative_table_path) override;
    StoragePtr detachTable(const String & table_name) override;

    void drop(const Context & /*context*/) override;
    void shutdown() override {};

protected:
    ASTPtr getCreateTableQueryImpl(const String & table_name, const Context & context, bool throw_on_error) const override;

private:
    const Context & global_context;
    String metadata_path;
    ASTPtr database_engine_define;
    String dbname;
    PGConnectionPtr connection;

    std::unordered_set<std::string> detached_tables;

    bool checkPostgresTable(const String & table_name) const;
    std::unordered_set<std::string> fetchTablesList() const;
    StoragePtr fetchTable(const String & table_name, const Context & context) const;
};

}

#endif
