#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>


namespace DB
{
class ASTStorage;


#define SET_RELATED_SETTINGS(M, ALIAS) \
    M(Bool, persistent, true, "Disable setting to avoid the overhead of writing to disk for StorageSet", 0) \
    M(UInt32, size_of_filter, 1000, "Size of filter", 0) \
    M(Float, precision, 0.01f, "Precision", 0) \
    M(String, name_of_filter, "bloom_filter", "Name of the filter to use set", 0) \
    M(String, disk, "default", "Name of the disk used to persist set data", 0)

#define LIST_OF_SET_SETTINGS(M, ALIAS) \
    SET_RELATED_SETTINGS(M, ALIAS) \
    FORMAT_FACTORY_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(setSettingsTraits, LIST_OF_SET_SETTINGS)


/** Settings for the Set engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct SetSettings : public BaseSettings<setSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
