#pragma once

#include <Core/BaseSettings.h>
#include <Parsers/ASTSetQuery.h>


namespace DB
{
class ASTStorage;


#define MEMORY_SETTINGS(M, ALIAS) \
    M(Bool, compress, false, "Compress data in memory", 0) \
    M(UInt64, min_rows_to_keep, 0, "Minimum block size (in rows) to retain in Memory table buffer.", 0) \
    M(UInt64, max_rows_to_keep, 0, "Maximum block size (in rows) to retain in Memory table buffer.", 0) \
    M(UInt64, min_bytes_to_keep, 0, "Minimum block size (in bytes) to retain in Memory table buffer.", 0) \
    M(UInt64, max_bytes_to_keep, 0, "Maximum block size (in bytes) to retain in Memory table buffer.", 0) \

DECLARE_SETTINGS_TRAITS(memorySettingsTraits, MEMORY_SETTINGS)


/** Settings for the Memory engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct MemorySettings : public BaseSettings<memorySettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
    ASTPtr getSettingsChangesQuery();
    void sanityCheck() const;
};

}

