#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>


namespace DB
{
class ASTStorage;


#define JOIN_RELATED_SETTINGS(M) \
    M(Bool, persistent, true, "Disable setting to avoid the overhead of writing to disk for StorageJoin", 0)

#define LIST_OF_JOIN_SETTINGS(M) \
    JOIN_RELATED_SETTINGS(M) \
    FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(joinSettingsTraits, LIST_OF_JOIN_SETTINGS)


/** Settings for the Join engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct JoinSettings : public BaseSettings<joinSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
