#pragma once

#include <Databases/DatabasesCommon.h>


namespace Poco { class Logger; }


namespace DB
{

/** A non-persistent database to store temporary data.
  * It doesn't make any manipulations with filesystem.
  * All tables are created by calling code.
  * TODO: Maybe DatabaseRuntime is more suitable class name.
  */
class DatabaseMemory : public DatabaseWithOwnTablesBase
{
public:
    DatabaseMemory(const String & name_);

    String getEngineName() const override { return "Memory"; }

    void createTable(
        const Context & context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query) override;

    void removeTable(
        const Context & context,
        const String & table_name) override;

    ASTPtr getCreateDatabaseQuery() const override;
};

}
