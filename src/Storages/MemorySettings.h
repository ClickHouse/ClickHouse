#pragma once

#include <Core/BaseSettings.h>


namespace DB
{
class ASTStorage;


#define MEMORY_SETTINGS(M) \
    M(Bool, compress, false, "Compress data in memory", 0) \

DECLARE_SETTINGS_TRAITS(memorySettingsTraits, MEMORY_SETTINGS)


/** Settings for the Memory engine.
  * Could be loaded from a CREATE TABLE query (SETTINGS clause).
  */
struct MemorySettings : public BaseSettings<memorySettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}

