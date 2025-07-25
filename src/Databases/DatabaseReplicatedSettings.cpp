#include <Databases/DatabaseReplicatedSettings.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{

IMPLEMENT_SETTINGS_TRAITS(DatabaseReplicatedSettingsTraits, LIST_OF_DATABASE_REPLICATED_SETTINGS)

void DatabaseReplicatedSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        applyChanges(storage_def.settings->changes);
        return;
    }

    auto settings_ast = std::make_shared<ASTSetQuery>();
    settings_ast->is_standalone = false;
    storage_def.set(storage_def.settings, settings_ast);
}

}
