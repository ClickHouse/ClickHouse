#pragma once

#include <Core/BaseSettings.h>
#include <Core/FormatFactorySettingsDeclaration.h>
#include <Core/SettingsEnums.h>

namespace DB
{
class ASTStorage;


#define SET_RELATED_SETTINGS(M, ALIAS) \
    M(Bool, persistent, true, "Disable setting to avoid the overhead of writing to disk for StorageSet", 0) \
    M(String, disk, "default", "Name of the disk used to persist set data", 0)

#define LIST_OF_SET_SETTINGS(M, ALIAS) \
    SET_RELATED_SETTINGS(M, ALIAS) \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(setSettingsTraits, LIST_OF_SET_SETTINGS)


/** Settings for the Set engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct SetSettings : public BaseSettings<setSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
