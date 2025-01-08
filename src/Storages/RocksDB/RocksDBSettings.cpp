#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Storages/RocksDB/RocksDBSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

/** StorageEmbeddedRocksdb table settings
  */
#define LIST_OF_ROCKSDB_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Bool, optimize_for_bulk_insert, true, "Table is optimized for bulk insertions (insert pipeline will create SST files and import to rocksdb database instead of writing to memtables)", 0) \
    DECLARE(UInt64, bulk_insert_block_size, DEFAULT_INSERT_BLOCK_SIZE, "Size of block for bulk insert, if it's smaller than query setting min_insert_block_size_rows then it will be overridden by min_insert_block_size_rows", 0) \

DECLARE_SETTINGS_TRAITS(RocksDBSettingsTraits, LIST_OF_ROCKSDB_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(RocksDBSettingsTraits, LIST_OF_ROCKSDB_SETTINGS)

struct RocksDBSettingsImpl : public BaseSettings<RocksDBSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) RocksDBSettings##TYPE NAME = &RocksDBSettingsImpl ::NAME;

namespace RocksDBSetting
{
LIST_OF_ROCKSDB_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN


RocksDBSettings::RocksDBSettings() : impl(std::make_unique<RocksDBSettingsImpl>())
{
}

RocksDBSettings::RocksDBSettings(const RocksDBSettings & settings) : impl(std::make_unique<RocksDBSettingsImpl>(*settings.impl))
{
}

RocksDBSettings::RocksDBSettings(RocksDBSettings && settings) noexcept
    : impl(std::make_unique<RocksDBSettingsImpl>(std::move(*settings.impl)))
{
}

RocksDBSettings::~RocksDBSettings() = default;

ROCKSDB_SETTINGS_SUPPORTED_TYPES(RocksDBSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void RocksDBSettings::applyChanges(const SettingsChanges & changes)
{
    impl->applyChanges(changes);
}

void RocksDBSettings::loadFromQuery(const ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            auto changes = storage_def.settings->changes;
            impl->applyChanges(changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
}
}
