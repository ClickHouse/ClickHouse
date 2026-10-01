#include <Interpreters/SecretArgumentsRegistry.h>

#include <Backups/BackupFactory.h>
#include <Common/HiddenSecret.h>
#include <Databases/DatabaseFactory.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/FunctionSecretArgumentsFinder.h>
#include <Storages/StorageFactory.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{

const SecretArgumentsRegistry & SecretArgumentsRegistry::instance()
{
    static const SecretArgumentsRegistry registry;
    return registry;
}

SecretArgumentsResult SecretArgumentsRegistry::find(ASTFunction::Kind kind, const AbstractFunction & function) const
{
    if (!function.hasArguments())
        return {};

    const String name = function.name();
    const SecretArgumentsSpec * spec = nullptr;
    /// Function arguments are expressions, where `=` is a comparison, and a backup locator reads its own
    /// arguments (positionals after named overrides included).
    bool generic_rules = true;
    switch (kind)
    {
        case ASTFunction::Kind::ORDINARY_FUNCTION:
            spec = TableFunctionFactory::instance().tryGetSecretArgumentsSpec(name);
            if (!spec)
            {
                spec = FunctionFactory::instance().tryGetSecretArgumentsSpec(name);
                generic_rules = false;
            }
            /// Every table function has a spec, so an unknown function has no secrets.
            if (!spec)
                return {};
            break;
        case ASTFunction::Kind::TABLE_ENGINE:
            spec = StorageFactory::instance().tryGetSecretArgumentsSpec(name);
            break;
        case ASTFunction::Kind::DATABASE_ENGINE:
            spec = DatabaseFactory::instance().tryGetSecretArgumentsSpec(name);
            break;
        case ASTFunction::Kind::BACKUP_NAME:
            spec = BackupFactory::instance().tryGetSecretArgumentsSpec(name);
            generic_rules = false;
            break;
        case ASTFunction::Kind::WINDOW_FUNCTION:
        case ASTFunction::Kind::LAMBDA_FUNCTION:
        case ASTFunction::Kind::CODEC:
        case ASTFunction::Kind::STATISTICS:
            return {};
    }

    FunctionSecretArgumentsFinder finder(function);
    /// Not a registered engine (a typo, or a process that did not register that factory). The statement is
    /// logged before validation rejects it, so fail closed.
    if (!spec)
        finder.maskEveryArgument();
    else
        finder.apply(*spec, generic_rules);
    return finder.result;
}

std::optional<String> SecretArgumentsRegistry::renderSecretSetting(const String & name, const Field & value) const
{
    /// Every engine is consulted whatever the engine of the statement (`ALTER TABLE t MODIFY SETTING` has none);
    /// the secret setting names are engine-prefixed, so they do not collide.
    for (const auto & [_, creator] : StorageFactory::instance().getAllStorages())
        if (auto it = creator.secret_arguments.secret_settings.find(name); it != creator.secret_arguments.secret_settings.end())
            return it->second(value);
    for (const auto & [_, creator] : DatabaseFactory::instance().getDatabaseEngines())
        if (auto it = creator.secret_arguments.secret_settings.find(name); it != creator.secret_arguments.secret_settings.end())
            return it->second(value);
    return {};
}

bool SecretArgumentsRegistry::maskDictionarySourceValue(const String & key, String & value) const
{
    /// A pair does not know the source it belongs to, so the keys of every source apply.
    for (const auto & [_, spec] : DictionarySourceFactory::instance().getSecretArgumentsSpecs())
    {
        if (std::ranges::contains(spec.secret_keys, key))
        {
            value = HIDDEN_SECRET_LITERAL;
            return true;
        }
        if (auto it = spec.partial.find(key); it != spec.partial.end() && it->second(value))
            return true;
    }
    return false;
}

}
