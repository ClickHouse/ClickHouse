#pragma once

#include "config.h"

#if USE_AWS_S3

#include <TableFunctions/ITableFunction.h>
#include <Storages/ExternalDataSourceConfiguration.h>


namespace DB
{

class Context;
class TableFunctionS3Cluster;

/* hudi(source, [access_key_id, secret_access_key,] format, structure[, compression]) - creates a temporary Hudi table on S3.
 */
class TableFunctionHudi : public ITableFunction
{
public:
    static constexpr auto name = "hudi";
    std::string getName() const override
    {
        return name;
    }

protected:
    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return name; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    static void parseArgumentsImpl(const String & error_message, ASTs & args, ContextPtr context, StorageS3Configuration & configuration);

    StorageS3Configuration configuration;
};

}

#endif
