#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Storages/GenerateRandomSettings.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_SETTING;
}

#define GENERATE_RANDOM_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Float, null_ratio, 0.0625, "Probability that a `Nullable`, `Variant` or `Dynamic` value is `NULL`. For `JSON`, it is the base probability that a key is absent from a row: most keys use exactly it, while a minority of sparse keys are absent several times more often. Must be in [0, 1]. The default reproduces the historical 1 in 16.", 0) \
    DECLARE(UInt64, max_json_depth, 3, "Maximum nesting depth of generated `JSON` objects: 1 means flat objects, objects inside arrays count as a level. Must be in [1, 32].", 0) \
    DECLARE(UInt64, max_json_keys_per_object, 8, "Maximum number of generated keys on one level of a `JSON` object; the root object gets at least half of it. 0 means only typed paths are generated. Must be at most 1000.", 0) \

DECLARE_SETTINGS_TRAITS(GenerateRandomSettingsTraits, GENERATE_RANDOM_SETTINGS, GENERATE_RANDOM_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(GenerateRandomSettingsTraits, GENERATE_RANDOM_SETTINGS, GenerateRandomSettings, GenerateRandomSetting)

GenerateRandomSettings::GenerateRandomSettings() : impl(std::make_unique<GenerateRandomSettingsImpl>())
{
}

GenerateRandomSettings::GenerateRandomSettings(const GenerateRandomSettings & settings)
    : impl(std::make_unique<GenerateRandomSettingsImpl>(*settings.impl))
{
}

GenerateRandomSettings::GenerateRandomSettings(GenerateRandomSettings && settings) noexcept = default;

GenerateRandomSettings::~GenerateRandomSettings() = default;

GenerateRandomSettings & GenerateRandomSettings::operator=(GenerateRandomSettings && settings) noexcept = default;

GENERATE_RANDOM_SETTINGS_SUPPORTED_TYPES(GenerateRandomSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void GenerateRandomSettings::loadFromQuery(ASTStorage & storage_def)
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

void GenerateRandomSettings::sanityCheck() const
{
    const Float32 null_ratio = (*impl)[GenerateRandomSetting::null_ratio];
    if (!(null_ratio >= 0 && null_ratio <= 1))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Setting `null_ratio` must be in [0, 1], got {}", null_ratio);

    const UInt64 max_json_depth = (*impl)[GenerateRandomSetting::max_json_depth];
    if (max_json_depth < 1 || max_json_depth > 32)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Setting `max_json_depth` must be in [1, 32], got {}", max_json_depth);

    const UInt64 max_json_keys_per_object = (*impl)[GenerateRandomSetting::max_json_keys_per_object];
    if (max_json_keys_per_object > 1000)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Setting `max_json_keys_per_object` must be at most 1000, got {}",
            max_json_keys_per_object);
}

void GenerateRandomSettings::applyChanges(const SettingsChanges & changes)
{
    impl->applyChanges(changes);
}

bool GenerateRandomSettings::hasBuiltin(std::string_view name)
{
    return GenerateRandomSettingsImpl::hasBuiltin(name);
}
}
