#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <TableFunctions/ITableFunction.h>


namespace DB
{

class Context;

/* s3(source, [access_key_id, secret_access_key,] format, structure) - creates a temporary storage for a file in S3
 */
class TableFunctionS3 : public ITableFunction
{
public:
    std::string getName() const override
    {
        return name;
    }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        const Context & context,
        const std::string & table_name) const override;

    static StoragePtr getStorage(
        const String & source,
        const String & access_key_id,
        const String & secret_access_key,
        const String & format,
        const ColumnsDescription & columns,
        Context & global_context,
        const std::string & table_name,
        const String & compression_method);

    const char * getStorageTypeName() const override { return storage_type_name; }
    String name;
    String storage_type_name;
};

}

#endif
