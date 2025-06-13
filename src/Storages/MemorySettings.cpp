#include <Storages/MemorySettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
    extern const int SETTING_CONSTRAINT_VIOLATION;
}

IMPLEMENT_SETTINGS_TRAITS(memorySettingsTraits, MEMORY_SETTINGS)

void MemorySettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            applyChanges(storage_def.settings->changes);
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
    for (const auto & change : changes())
        settings_ast->changes.push_back(change);

    return settings_ast;
}

void MemorySettings::sanityCheck() const
{
    if (min_bytes_to_keep > max_bytes_to_keep)
        throw Exception(ErrorCodes::SETTING_CONSTRAINT_VIOLATION,
                        "Setting `min_bytes_to_keep` cannot be higher than the `max_bytes_to_keep`. `min_bytes_to_keep`: {}, `max_bytes_to_keep`: {}",
                        min_bytes_to_keep,
                        max_bytes_to_keep);


    if (min_rows_to_keep > max_rows_to_keep)
        throw Exception(ErrorCodes::SETTING_CONSTRAINT_VIOLATION,
                        "Setting `min_rows_to_keep` cannot be higher than the `max_rows_to_keep`. `min_rows_to_keep`: {}, `max_rows_to_keep`: {}",
                        min_rows_to_keep,
                        max_rows_to_keep);
}

}

