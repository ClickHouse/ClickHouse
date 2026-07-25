#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Access/Common/AccessFlags.h>
#include <Common/typeid_cast.h>
#include <base/find_symbols.h>
#include <Core/Settings.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool allow_experimental_table_namespaces;
}

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

BlockIO InterpreterUseQuery::execute()
{
    const auto & use_query = query_ptr->as<ASTUseQuery &>();

    /// `USE db.ns`, the first part is the database, the rest is a namespace path
    /// A single (possibly quoted) part is always an exact database name
    const String logical_name = use_query.getDatabase();
    String database_name = logical_name;
    if (const auto * identifier = use_query.database->as<ASTIdentifier>();
        identifier && identifier->name_parts.size() > 1)
    {
        if (!getContext()->getSettingsRef()[Setting::allow_experimental_table_namespaces])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Table namespaces are an experimental feature; enable allow_experimental_table_namespaces to use `USE {}`",
                logical_name);

        if (!getContext()->getSettingsRef()[Setting::allow_experimental_analyzer])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Table namespaces require setting enable_analyzer set to 1 to use `USE {}`",
                logical_name);
        database_name = identifier->name_parts[0];
    }

    getContext()->checkAccess(AccessType::SHOW_DATABASES, database_name);

    /// the current database stores the logical name ("db.ns"), setCurrentDatabase
    /// validates that the namespace exists and resolution folds it into table names
    getContext()->getSessionContext()->setCurrentDatabase(
        logical_name, getContext()->getSettingsRef()[Setting::allow_experimental_table_namespaces]);
    return {};
}

void registerInterpreterUseQuery(InterpreterFactory & factory);
void registerInterpreterUseQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterUseQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterUseQuery", create_fn, /*supports_table_namespace_scope*/ true);
}

}
