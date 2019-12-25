#pragma once

#include <Databases/DatabasesCommon.h>

#include <Databases/DatabaseOrdinary.h>

namespace DB
{

class DatabaseAtomic : public DatabaseOrdinary
{
public:

    DatabaseAtomic(String name_, String metadata_path_, const Context & context_);

    String getEngineName() const override { return "Atomic"; }

    void renameTable(
            const Context & context,
            const String & table_name,
            IDatabase & to_database,
            const String & to_table_name) override;

    void attachTable(const String & name, const StoragePtr & table, const String & relative_table_path = {}) override;
    StoragePtr detachTable(const String & name) override;

    String getTableDataPath(const String & table_name) const override;
    String getTableDataPath(const ASTCreateQuery & query) const override;

    void drop(const Context & /*context*/) override;

private:
    //TODO store path in DatabaseWithOwnTables::tables
    std::map<String, String> table_name_to_path;

};

}
