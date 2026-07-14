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
    extern const SettingsBool allow_experimental_table_namespaces;
}

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

BlockIO InterpreterUseQuery::execute()
{
    const auto & use_query = query_ptr->as<ASTUseQuery &>();

    /// `USE db.ns` arrives as a multipart identifier: the first part is the database,
    /// the rest is a namespace path inside it (experimental). A single (possibly quoted)
    /// part is always an exact database name.
    String database_name = use_query.getDatabase();
    String table_prefix;
    if (const auto * identifier = use_query.database->as<ASTIdentifier>();
        identifier && identifier->name_parts.size() > 1)
    {
        if (!getContext()->getSettingsRef()[Setting::allow_experimental_table_namespaces])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Table namespaces are an experimental feature; enable allow_experimental_table_namespaces to use `USE {}`",
                database_name);
        database_name = identifier->name_parts[0];
        table_prefix = identifier->name_parts[1];
        for (size_t i = 2; i < identifier->name_parts.size(); ++i)
            table_prefix += "." + identifier->name_parts[i];
    }

    getContext()->checkAccess(AccessType::SHOW_DATABASES, database_name);

    /// setCurrentDatabase validates that the namespace exists
    getContext()->getSessionContext()->setCurrentDatabase(database_name, table_prefix);
    return {};
}

void registerInterpreterUseQuery(InterpreterFactory & factory);
void registerInterpreterUseQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterUseQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterUseQuery", create_fn);
}

}
