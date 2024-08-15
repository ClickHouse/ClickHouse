#pragma once

#include "config.h"

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/StorageS3Cluster.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionAzureBlobStorage.h>
#include <TableFunctions/TableFunctionS3.h>


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
    String getSignature() const override = 0;

    static void addColumnsStructureToArguments(ASTs & args, const String & desired_structure, const ContextPtr & context)
    {
        if (args.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected empty list of arguments for {}Cluster table function", Base::name);

        ASTPtr cluster_name_arg = args.front();
        args.erase(args.begin());
        Base::addColumnsStructureToArguments(args, desired_structure, context);
        args.insert(args.begin(), cluster_name_arg);
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
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "The signature of table function {} shall be the following:\n{}", getName(), getSignature());

        /// Evaluate only first argument, everything else will be done Base class
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
