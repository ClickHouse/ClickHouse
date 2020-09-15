#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>


namespace DB
{
class ASTStorage;


#define SET_RELATED_SETTINGS(M) \
    M(Bool, disable_persistency, false, "Disable persistency for StorageSet to reduce IO overhead", 0)

#define LIST_OF_SET_SETTINGS(M) \
    SET_RELATED_SETTINGS(M) \
    FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(setSettingsTraits, LIST_OF_SET_SETTINGS)


/** Settings for the Set engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct SetSettings : public BaseSettings<setSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
