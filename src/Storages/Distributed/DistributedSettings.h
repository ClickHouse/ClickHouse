#pragma once

#include <Core/Defines.h>
#include <Core/BaseSettings.h>


namespace Poco::Util
{
    class AbstractConfiguration;
}


namespace DB
{
class ASTStorage;

#define LIST_OF_DISTRIBUTED_SETTINGS(M) \
    M(Bool, fsync_after_insert, false, "Do fsync for every inserted. Will decreases performance of inserts (only for async INSERT, i.e. insert_distributed_sync=false)", 0) \
    M(Bool, fsync_directories, false, "Do fsync for temporary directory (that is used for async INSERT only) after all part operations (writes, renames, etc.).", 0) \
    /** Inserts settings. */ \
    M(UInt64, bytes_to_throw_insert, 0, "If more than this number of compressed bytes will be pending for async INSERT, an exception will be thrown. 0 - do not throw.", 0) \

DECLARE_SETTINGS_TRAITS(DistributedSettingsTraits, LIST_OF_DISTRIBUTED_SETTINGS)


/** Settings for the Distributed family of engines.
  */
struct DistributedSettings : public BaseSettings<DistributedSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
