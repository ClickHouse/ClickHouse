#include <Interpreters/InterpreterCreateIndexQuery.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Context.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateIndexQuery.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTFunction.h>

#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_create_index_without_type;
    extern const SettingsBool create_index_ignore_unique;
}

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

bool validateCreateIndexQuery(const ASTCreateIndexQuery & query, const ContextPtr & context)
{
    if (query.unique)
    {
        if (!context->getSettingsRef()[Setting::create_index_ignore_unique])
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CREATE UNIQUE INDEX is not supported."
                " SET create_index_ignore_unique=1 to ignore this UNIQUE keyword.");
        }
    }

    // Noop if allow_create_index_without_type = true. throw otherwise
    if (!query.index_decl->as<ASTIndexDeclaration>()->getType())
    {
        if (!context->getSettingsRef()[Setting::allow_create_index_without_type])
        {
            throw Exception(ErrorCodes::INCORRECT_QUERY, "CREATE INDEX without TYPE is forbidden."
                " SET allow_create_index_without_type=1 to ignore this statements");
        }

        // Nothing to do
        return false;
    }

    return true;
}

ASTPtr rewriteToAlterTable(const ASTCreateIndexQuery & query)
{
    auto alter = make_intrusive<ASTAlterQuery>();
    alter->alter_object = ASTAlterQuery::AlterObjectType::TABLE;
    alter->setDatabase(query.getDatabase());
    alter->setTable(query.getTable());
    alter->cluster = query.cluster;

    auto command_list = make_intrusive<ASTExpressionList>();
    command_list->children.push_back(query.convertToASTAlterCommand());

    alter->command_list = command_list.get();
    alter->children.push_back(std::move(command_list));

    return alter;
}

}

BlockIO InterpreterCreateIndexQuery::execute()
{
    const auto & create_index = query_ptr->as<ASTCreateIndexQuery &>();
    const auto context = Context::createCopy(getContext());

    bool need_proceed = validateCreateIndexQuery(create_index, context);
    if (!need_proceed)
        return {};

    auto alter_query = rewriteToAlterTable(create_index);
    return InterpreterAlterQuery(alter_query, context).execute();
}

void registerInterpreterCreateIndexQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateIndexQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateIndexQuery", create_fn);
}

}
