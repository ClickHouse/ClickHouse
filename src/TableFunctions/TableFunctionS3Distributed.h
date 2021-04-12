#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <TableFunctions/ITableFunction.h>


namespace DB
{

class Context;

/**
 * s3Distributed(cluster_name, source, [access_key_id, secret_access_key,] format, structure)
 * A table function, which allows to process many files from S3 on a specific cluster
 * On initiator it creates a connection to _all_ nodes in cluster, discloses asterics
 * in S3 file path and register all tasks (paths in S3)  in NextTaskResolver to dispatch
 * them dynamically.
 * On worker node it asks initiator about next task to process, processes it.
 * This is repeated until the tasks are finished.
 */
class TableFunctionS3Distributed : public ITableFunction
{
public:
    static constexpr auto name = "s3Distributed";
    std::string getName() const override
    {
        return name;
    }
    bool hasStaticStructure() const override { return true; }

protected:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        const Context & context,
        const std::string & table_name,
        ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "S3Distributed"; }

    ColumnsDescription getActualTableStructure(const Context & context) const override;
    void parseArguments(const ASTPtr & ast_function, const Context & context) override;

    String cluster_name;
    String filename;
    String format;
    String structure;
    String access_key_id;
    String secret_access_key;
    String compression_method = "auto";
};

class TableFunctionCOSDistributed : public TableFunctionS3Distributed
{
public:
    static constexpr auto name = "cosnDistributed";
    std::string getName() const override
    {
        return name;
    }
private:
    const char * getStorageTypeName() const override { return "COSNDistributed"; }
};

}

#endif
