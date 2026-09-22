#include <Interpreters/AddDefaultDatabaseVisitor.h>

#include <Core/Settings.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool enable_global_with_statement;
}

AddDefaultDatabaseVisitor::AddDefaultDatabaseVisitor(
    ContextPtr context_,
    const String & database_name_,
    bool only_replace_current_database_function_,
    bool only_replace_in_join_,
    std::optional<bool> global_with_enabled_)
    : context(context_)
    , database_name(database_name_)
    , global_with_enabled(global_with_enabled_.value_or(context_->getSettingsRef()[Setting::enable_global_with_statement]))
    , only_replace_current_database_function(only_replace_current_database_function_)
    , only_replace_in_join(only_replace_in_join_)
{
    if (!context->isGlobalContext())
    {
        for (const auto & [table_name, _ /* storage */] : context->getExternalTables())
        {
            external_tables.insert(table_name);
        }
    }
}

namespace
{

/// Unlike `SettingFieldBool`, does not throw: a stored definition with any value has to load.
std::optional<bool> tryReadBool(const Field & value)
{
    switch (value.getType())
    {
        case Field::Types::UInt64:
            return value.safeGet<UInt64>() != 0;
        case Field::Types::Int64:
            return value.safeGet<Int64>() != 0;
        case Field::Types::Bool:
            return value.safeGet<bool>();
        case Field::Types::String:
        {
            const auto & str = value.safeGet<String>();
            if (str == "1" || Poco::icompare(str, "true") == 0)
                return true;
            if (str == "0" || Poco::icompare(str, "false") == 0)
                return false;
            return std::nullopt;
        }
        default:
            return std::nullopt;
    }
}

}

std::optional<bool> AddDefaultDatabaseVisitor::globalWithSettingOf(const ASTSelectQuery & select)
{
    const auto settings = select.settings();
    if (!settings)
        return std::nullopt;
    const auto * set_query = settings->as<ASTSetQuery>();
    if (!set_query)
        return std::nullopt;
    /// Repeated in one clause, the setting is off if any occurrence is off, as in the analyzer; an unrecognised value is ignored.
    std::optional<bool> result;
    for (const auto & change : set_query->changes)
        if (change.name == "enable_global_with_statement")
            if (auto value = tryReadBool(change.value))
                result = *value && result.value_or(true);
    return result;
}

}
