#pragma once

#include <Core/BaseSettings.h>

namespace DB
{
    class ASTStorage;


#define LIST_OF_MATERIALIZE_POSTGRESQL_SETTINGS(M) \
    M(String, postgresql_replication_slot_name, "", "PostgreSQL replication slot name.", 0) \
    M(String, postgresql_publication_name, "", "PostgreSQL publication name.", 0) \

DECLARE_SETTINGS_TRAITS(MaterializePostgreSQLSettingsTraits, LIST_OF_MATERIALIZE_POSTGRESQL_SETTINGS)

struct MaterializePostgreSQLSettings : public BaseSettings<MaterializePostgreSQLSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
