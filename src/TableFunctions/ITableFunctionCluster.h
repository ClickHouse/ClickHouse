#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionObjectStorage.h>

#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CLUSTER_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
}

/// Base class for *Cluster table functions that require cluster_name for the first argument.
template <typename Base>
class ITableFunctionCluster : public Base
{
public:
    String getName() const override = 0;

    static void updateStructureAndFormatArgumentsIfNeeded(ASTFunction * table_function, const String & structure_, const String & format_, const ContextPtr & context)
    {
        auto * expression_list = table_function->arguments->as<ASTExpressionList>();
        ASTs args = expression_list->children;

        if (args.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected empty list of arguments for {}Cluster table function", Base::name);

        if (table_function-> name == Base::name)
            Base::updateStructureAndFormatArgumentsIfNeeded(args, structure_, format_, context);
        else
        {
            ASTPtr cluster_name_arg = args.front();
            args.erase(args.begin());
            Base::updateStructureAndFormatArgumentsIfNeeded(args, structure_, format_, context);
            args.insert(args.begin(), cluster_name_arg);
        }
    }

protected:
    void parseArguments(const ASTPtr & ast, ContextPtr context) override
    {
        /// Clone ast function, because we can modify its arguments like removing cluster_name
        Base::parseArguments(ast->clone(), context);
    }

    void parseArgumentsImpl(ASTs & args, const ContextPtr & context) override
    {
        if (args.empty())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "The function {} should have arguments. The first argument must be the cluster name and the rest are the arguments of "
                "corresponding table function",
                getName());

        /// Evaluate only first argument, everything else will be done by the Base class
        args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context);

        /// Cluster name is always the first
        cluster_name = checkAndGetLiteralArgument<String>(args[0], "cluster_name");

        if (!context->tryGetCluster(cluster_name))
            throw Exception(ErrorCodes::CLUSTER_DOESNT_EXIST, "Requested cluster '{}' not found", cluster_name);

        /// Just cut the first arg (cluster_name) and try to parse other table function arguments as is
        args.erase(args.begin());

        Base::parseArgumentsImpl(args, context);
    }

    String cluster_name;
};

}
