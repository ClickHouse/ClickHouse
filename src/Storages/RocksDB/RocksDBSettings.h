#pragma once

#include <Core/BaseSettings.h>
#include <Core/Defines.h>
#include <Interpreters/Context_fwd.h>
#include <base/unit.h>
#include <Common/NamePrompter.h>


namespace Poco::Util
{
class AbstractConfiguration;
}


namespace DB
{
class ASTStorage;
struct Settings;


/** StorageEmbeddedRocksdb table settings
  */

#define ROCKSDB_SETTINGS(M, ALIAS) \
    M(Bool, optimize_for_bulk_insert, true, "Table is optimized for bulk insertions (insert pipeline will create SST files and import to rocksdb database instead of writing to memtables)", 0) \
    M(UInt64, bulk_insert_block_size, DEFAULT_INSERT_BLOCK_SIZE, "Size of block for bulk insert, if it's smaller than query setting min_insert_block_size_rows then it will be overridden by min_insert_block_size_rows", 0) \

#define LIST_OF_ROCKSDB_SETTINGS(M, ALIAS) ROCKSDB_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(RockDBSettingsTraits, LIST_OF_ROCKSDB_SETTINGS)

struct RocksDBSettings : public BaseSettings<RockDBSettingsTraits>, public IHints<2>
{
    void loadFromQuery(ASTStorage & storage_def, ContextPtr context);
    std::vector<String> getAllRegisteredNames() const override;
};

}
