#pragma once

#include <Databases/DatabasesCommon.h>

#include <Databases/DatabaseOrdinary.h>

namespace DB
{

class DatabaseAtomic : /* public DatabaseWithOwnTablesBase */ public DatabaseOrdinary
{
public:

    DatabaseAtomic(String name_, String metadata_path_, const Context & context_);

    String getEngineName() const override { return "Atomic"; }

    void renameTable(const Context & context,
                     const String & table_name,
                     IDatabase & to_database,
                     const String & to_table_name,
                     TableStructureWriteLockHolder &) override;


};

}
