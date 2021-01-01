#pragma once

#include "config_core.h"

#include <Storages/IStorage.h>
#include <ext/shared_ptr_helper.h>
#include <Interpreters/Context.h>
#include "PostgreSQLReplicationHandler.h"
#include "pqxx/pqxx"

namespace DB
{

class StorageMaterializePostgreSQL final : public ext::shared_ptr_helper<StorageMaterializePostgreSQL>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageMaterializePostgreSQL>;

public:
    StorageMaterializePostgreSQL(
        const StorageID & table_id_,
        const String & remote_table_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const Context & context_,
        std::shared_ptr<PostgreSQLReplicationHandler> replication_handler_);

    String getName() const override { return "MaterializePostgreSQL"; }

    void startup() override;
    void shutdown() override;

private:
    String remote_table_name;
    Context global_context;

    std::shared_ptr<PostgreSQLReplicationHandler> replication_handler;
};

}

