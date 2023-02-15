#pragma once

#include "config.h"

#if USE_AWS_S3

#    include <TableFunctions/ITableFunction.h>
#    include <filesystem>
#    include <Access/Common/AccessFlags.h>
#    include <Formats/FormatFactory.h>
#    include <IO/S3Common.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/evaluateConstantExpression.h>
#    include <Interpreters/parseColumnsListForTableFunction.h>
#    include <Parsers/ASTLiteral.h>
#    include <Storages/ExternalDataSourceConfiguration.h>
#    include <Storages/StorageURL.h>
#    include <Storages/checkAndGetLiteralArgument.h>
#    include <TableFunctions/TableFunctionS3.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename Name, typename Storage, typename Configuration>
class ITableFunctionDataLake : public ITableFunction
{
public:
    static constexpr auto name = Name::name;
    std::string getName() const override
    {
        return name;
    }

protected:
    StoragePtr
    executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/)
        const override
    {
        ColumnsDescription columns;
        if (configuration.structure != "auto")
            columns = parseColumnsListFromString(configuration.structure, context);

        StoragePtr storage = std::make_shared<Storage>(
            configuration, StorageID(getDatabaseName(), table_name), columns, ConstraintsDescription{}, String{}, context, std::nullopt);

        storage->startup();

        return storage;
    }

    const char * getStorageTypeName() const override { return Storage::name; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override
    {
        if (configuration.structure == "auto")
        {
            context->checkAccess(getSourceAccessType());
            return Storage::getTableStructureFromData(configuration, std::nullopt, context);
        }

        return parseColumnsListFromString(configuration.structure, context);
    }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override
    {
        /// Parse args
        ASTs & args_func = ast_function->children;

        const auto message = fmt::format(
            "The signature of table function {} could be the following:\n"
            " - url\n"
            " - url, format\n"
            " - url, format, structure\n"
            " - url, access_key_id, secret_access_key\n"
            " - url, format, structure, compression_method\n"
            " - url, access_key_id, secret_access_key, format\n"
            " - url, access_key_id, secret_access_key, format, structure\n"
            " - url, access_key_id, secret_access_key, format, structure, compression_method",
            getName());

        if (args_func.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments", getName());

        auto & args = args_func.at(0)->children;

        TableFunctionS3::parseArgumentsImpl(message, args, context, configuration, false);

        if (configuration.format == "auto")
            configuration.format = "Parquet";
    }

    mutable Configuration configuration;
};
}

#endif
