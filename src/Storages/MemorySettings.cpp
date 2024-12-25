#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Storages/MemorySettings.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
    extern const int SETTING_CONSTRAINT_VIOLATION;
}

#define MEMORY_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Bool, compress, false, "Compress data in memory", 0) \
    DECLARE(UInt64, min_rows_to_keep, 0, "Minimum block size (in rows) to retain in Memory table buffer.", 0) \
    DECLARE(UInt64, max_rows_to_keep, 0, "Maximum block size (in rows) to retain in Memory table buffer.", 0) \
    DECLARE(UInt64, min_bytes_to_keep, 0, "Minimum block size (in bytes) to retain in Memory table buffer.", 0) \
    DECLARE(UInt64, max_bytes_to_keep, 0, "Maximum block size (in bytes) to retain in Memory table buffer.", 0) \

DECLARE_SETTINGS_TRAITS(MemorySettingsTraits, MEMORY_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(MemorySettingsTraits, MEMORY_SETTINGS)


struct MemorySettingsImpl : public BaseSettings<MemorySettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) MemorySettings##TYPE NAME = &MemorySettingsImpl ::NAME;

namespace MemorySetting
{
MEMORY_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

MemorySettings::MemorySettings() : impl(std::make_unique<MemorySettingsImpl>())
{
}

MemorySettings::MemorySettings(const MemorySettings & settings) : impl(std::make_unique<MemorySettingsImpl>(*settings.impl))
{
}

MemorySettings::MemorySettings(MemorySettings && settings) noexcept : impl(std::make_unique<MemorySettingsImpl>(std::move(*settings.impl)))
{
}

MemorySettings::~MemorySettings() = default;

MemorySettings & MemorySettings::operator=(MemorySettings && settings) noexcept
{
    *impl = std::move(*settings.impl);
    return *this;
}

MEMORY_SETTINGS_SUPPORTED_TYPES(MemorySettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void MemorySettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            impl->applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
}

ASTPtr MemorySettings::getSettingsChangesQuery()
{
    auto settings_ast = std::make_shared<ASTSetQuery>();
    settings_ast->is_standalone = false;
    for (const auto & change : impl->changes())
        settings_ast->changes.push_back(change);

    return settings_ast;
}

void MemorySettings::sanityCheck() const
{
    if (impl->min_bytes_to_keep > impl->max_bytes_to_keep)
        throw Exception(
            ErrorCodes::SETTING_CONSTRAINT_VIOLATION,
            "Setting `min_bytes_to_keep` cannot be higher than the `max_bytes_to_keep`. `min_bytes_to_keep`: {}, `max_bytes_to_keep`: {}",
            impl->min_bytes_to_keep,
            impl->max_bytes_to_keep);


    if (impl->min_rows_to_keep > impl->max_rows_to_keep)
        throw Exception(
            ErrorCodes::SETTING_CONSTRAINT_VIOLATION,
            "Setting `min_rows_to_keep` cannot be higher than the `max_rows_to_keep`. `min_rows_to_keep`: {}, `max_rows_to_keep`: {}",
            impl->min_rows_to_keep,
            impl->max_rows_to_keep);
}

void MemorySettings::applyChanges(const DB::SettingsChanges & changes)
{
    impl->applyChanges(changes);
}
}

