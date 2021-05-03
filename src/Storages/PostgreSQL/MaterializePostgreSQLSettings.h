#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <Core/BaseSettings.h>


namespace DB
{
    class ASTStorage;


#define LIST_OF_MATERIALIZE_POSTGRESQL_SETTINGS(M) \
    M(UInt64, materialize_postgresql_max_block_size, 65536, "Number of row collected before flushing data into table.", 0) \
    M(String, materialize_postgresql_tables_list, "", "List of tables for MaterializePostgreSQL database engine", 0) \
    M(Bool, materialize_postgresql_allow_automatic_update, 0, "Allow to reload table in the background, when schema changes are detected", 0) \

DECLARE_SETTINGS_TRAITS(MaterializePostgreSQLSettingsTraits, LIST_OF_MATERIALIZE_POSTGRESQL_SETTINGS)

struct MaterializePostgreSQLSettings : public BaseSettings<MaterializePostgreSQLSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}

#endif
