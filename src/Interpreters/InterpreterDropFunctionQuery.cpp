#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterDropFunctionQuery.h>

#include <Access/ContextAccess.h>
#include <Functions/UserDefined/ExternalUserDefinedExecutableFunctionsLoader.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionDriverInvoker.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionDriverRegistry.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/ASTCreateFunctionWithDriverQuery.h>
#include <Parsers/ASTDropFunctionQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Common/FieldVisitorToString.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <filesystem>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

namespace
{
    String formatArgsSignatureForDrop(const ASTPtr & arguments_ast)
    {
        if (!arguments_ast)
            return {};
        WriteBufferFromOwnString out;
        bool first = true;
        for (const auto & child : arguments_ast->children)
        {
            if (!first)
                out << ", ";
            first = false;

            if (const auto * name_type_pair = child->as<ASTNameTypePair>())
            {
                out << name_type_pair->name << ' ';
                IAST::FormatSettings settings(/*one_line=*/true);
                IAST::FormatState state;
                IAST::FormatStateStacked frame;
                name_type_pair->type->format(out, settings, state, frame);
            }
            else
            {
                IAST::FormatSettings settings(/*one_line=*/true);
                IAST::FormatState state;
                IAST::FormatStateStacked frame;
                child->format(out, settings, state, frame);
            }
        }
        return out.str();
    }

    String formatReturnTypeForDrop(const ASTPtr & return_type_ast)
    {
        if (!return_type_ast)
            return {};
        WriteBufferFromOwnString out;
        IAST::FormatSettings settings(/*one_line=*/true);
        IAST::FormatState state;
        IAST::FormatStateStacked frame;
        return_type_ast->format(out, settings, state, frame);
        return out.str();
    }

    void handleDriverBasedDrop(
        const ASTCreateFunctionWithDriverQuery & create_query,
        const ContextPtr & current_context)
    {
        auto log = getLogger("InterpreterDropFunctionQuery");
        const String function_name = create_query.getFunctionName();

        auto driver = UserDefinedExecutableFunctionDriverRegistry::instance().tryGet(create_query.engine_name);
        if (!driver)
            LOG_WARNING(log, "Driver '{}' for function '{}' is not registered; skipping driver drop_command",
                create_query.engine_name, function_name);

        String dynamic_dir = current_context->getDynamicUserDefinedExecutableFunctionsPath();
        if (!dynamic_dir.empty() && !dynamic_dir.ends_with('/'))
            dynamic_dir.push_back('/');

        const String escaped = escapeForFileName(function_name);
        const String working_dir = dynamic_dir.empty() ? String() : dynamic_dir + escaped + ".d";

        if (driver)
        {
            std::vector<std::pair<String, String>> engine_argument_values;
            for (const auto & [name, value_ast] : create_query.engine_arguments)
            {
                const auto * literal = value_ast->as<ASTLiteral>();
                if (!literal)
                    continue;
                engine_argument_values.emplace_back(name, applyVisitor(FieldVisitorToString(), literal->value));
            }

            try
            {
                UserDefinedExecutableFunctionDriverInvoker::runDropCommand(
                    *driver, function_name,
                    formatReturnTypeForDrop(create_query.return_type_ast),
                    formatArgsSignatureForDrop(create_query.arguments_ast),
                    working_dir,
                    engine_argument_values);
            }
            catch (...)
            {
                tryLogCurrentException(log, "while running driver drop_command");
            }
        }

        if (!dynamic_dir.empty())
        {
            std::error_code ec;
            std::filesystem::remove(dynamic_dir + escaped + ".xml", ec);
            std::filesystem::remove(dynamic_dir + escaped + ".yaml", ec);
            std::filesystem::remove_all(working_dir, ec);
        }

        /// Tell the executable UDF loader to drop the function now that its config is gone.
        try
        {
            const auto & loader = current_context->getExternalUserDefinedExecutableFunctionsLoader();
            loader.reloadFunction(function_name);
        }
        catch (...)
        {
            tryLogCurrentException(log, "while reloading external UDF loader after drop");
        }
    }
}

BlockIO InterpreterDropFunctionQuery::execute()
{
    FunctionNameNormalizer::visit(query_ptr.get());

    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    ASTDropFunctionQuery & drop_function_query = updated_query_ptr->as<ASTDropFunctionQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_FUNCTION);

    auto current_context = getContext();

    if (!drop_function_query.cluster.empty())
    {
        if (current_context->getUserDefinedSQLObjectsStorage().isReplicated())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "ON CLUSTER is not allowed because used-defined functions are replicated automatically");

        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(updated_query_ptr, current_context, params);
    }

    current_context->checkAccess(access_rights_elements);

    bool throw_if_not_exists = !drop_function_query.if_exists;

    /// If the function was created by a driver, invoke the driver's drop_command and remove its config.
    if (auto existing_ast = current_context->getUserDefinedSQLObjectsStorage().tryGet(drop_function_query.function_name))
    {
        if (const auto * driver_ast = existing_ast->as<ASTCreateFunctionWithDriverQuery>())
        {
            handleDriverBasedDrop(*driver_ast, current_context);
            current_context->getUserDefinedSQLObjectsStorage().removeObject(
                current_context,
                UserDefinedSQLObjectType::Function,
                drop_function_query.function_name,
                throw_if_not_exists);
            return {};
        }
    }

    UserDefinedSQLFunctionFactory::instance().unregisterFunction(current_context, drop_function_query.function_name, throw_if_not_exists);

    return {};
}

void registerInterpreterDropFunctionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropFunctionQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropFunctionQuery", create_fn);
}

}
