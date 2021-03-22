#include <thread>
#include <Common/config.h>
#include "DataStreams/RemoteBlockInputStream.h"
#include "Processors/Sources/SourceFromInputStream.h"
#include "Storages/StorageS3Distributed.h"
#include "Storages/System/StorageSystemOne.h"

#if USE_AWS_S3

#include <IO/S3Common.h>
#include <Storages/StorageS3.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include <TableFunctions/TableFunctionS3Distributed.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Storages/StorageTaskManager.h>
#include <Parsers/ASTLiteral.h>
#include "registerTableFunctions.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void TableFunctionS3Distributed::parseArguments(const ASTPtr & ast_function, const Context & context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.at(0)->children;

    if (args.size() < 4 || args.size() > 7)
        throw Exception("Table function '" + getName() + "' requires 4 to 7 arguments: cluster, url," + 
            "[access_key_id, secret_access_key,] format, structure and [compression_method].",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    cluster_name = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    filename = args[1]->as<ASTLiteral &>().value.safeGet<String>();

    if (args.size() < 5)
    {
        format = args[2]->as<ASTLiteral &>().value.safeGet<String>();
        structure = args[3]->as<ASTLiteral &>().value.safeGet<String>();
    }
    else
    {
        access_key_id = args[2]->as<ASTLiteral &>().value.safeGet<String>();
        secret_access_key = args[3]->as<ASTLiteral &>().value.safeGet<String>();
        format = args[4]->as<ASTLiteral &>().value.safeGet<String>();
        structure = args[5]->as<ASTLiteral &>().value.safeGet<String>();
    }

    if (args.size() == 5 || args.size() == 7)
        compression_method = args.back()->as<ASTLiteral &>().value.safeGet<String>();
}


ColumnsDescription TableFunctionS3Distributed::getActualTableStructure(const Context & context) const
{
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionS3Distributed::executeImpl(
    const ASTPtr & /*ast_function*/, const Context & context,
    const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    Poco::URI uri (filename);
    S3::URI s3_uri (uri);
    // UInt64 min_upload_part_size = context.getSettingsRef().s3_min_upload_part_size;
    // UInt64 max_single_part_upload_size = context.getSettingsRef().s3_max_single_part_upload_size;
    UInt64 max_connections = context.getSettingsRef().s3_max_connections;

    StorageS3::ClientAuthentificaiton client_auth{s3_uri, access_key_id, secret_access_key, max_connections, {}, {}};
    StorageS3::updateClientAndAuthSettings(context, client_auth);

    auto lists = StorageS3::listFilesWithRegexpMatching(*client_auth.client, client_auth.uri);
    Strings tasks;
    tasks.reserve(lists.size());

    for (auto & value : lists) {
        tasks.emplace_back(client_auth.uri.endpoint + '/' + client_auth.uri.bucket + '/' + value);
    }

    std::cout << "query_id " << context.getCurrentQueryId() << std::endl;

    /// Register resolver, which will give other nodes a task to execute
    TaskSupervisor::instance().registerNextTaskResolver(
        std::make_unique<S3NextTaskResolver>(context.getCurrentQueryId(), std::move(tasks)));

    StoragePtr storage = StorageS3Distributed::create(
            s3_uri,
            access_key_id,
            secret_access_key,
            StorageID(getDatabaseName(), table_name),
            cluster_name,
            format,
            max_connections,
            getActualTableStructure(context),
            ConstraintsDescription{},
            const_cast<Context &>(context),
            compression_method);

    storage->startup();

    // std::this_thread::sleep_for(std::chrono::seconds(60));

    return storage;
}


void registerTableFunctionS3Distributed(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionS3Distributed>();
}

void registerTableFunctionCOSDistributed(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionCOSDistributed>();
}

}

#endif
