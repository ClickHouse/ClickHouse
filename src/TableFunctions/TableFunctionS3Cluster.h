#pragma once

#include "config.h"

#if USE_AWS_S3

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionS3.h>
#include <TableFunctions/ITableFunctionCluster.h>
#include <Storages/StorageS3Cluster.h>


namespace DB
{

class Context;

/**
 * s3cluster(cluster_name, source, [access_key_id, secret_access_key,] format, structure, compression_method)
 * A table function, which allows to process many files from S3 on a specific cluster
 * On initiator it creates a connection to _all_ nodes in cluster, discloses asterisks
 * in S3 file path and dispatch each file dynamically.
 * On worker node it asks initiator about next task to process, processes it.
 * This is repeated until the tasks are finished.
 */
class TableFunctionS3Cluster : public ITableFunctionCluster<TableFunctionS3>
{
public:
    static constexpr auto name = "s3Cluster";
    static constexpr auto signature = " - cluster, url\n"
                                      " - cluster, url, format\n"
                                      " - cluster, url, format, structure\n"
                                      " - cluster, url, access_key_id, secret_access_key\n"
                                      " - cluster, url, format, structure, compression_method\n"
                                      " - cluster, url, access_key_id, secret_access_key, format\n"
                                      " - cluster, url, access_key_id, secret_access_key, format, structure\n"
                                      " - cluster, url, access_key_id, secret_access_key, format, structure, compression_method\n"
                                      " - cluster, url, access_key_id, secret_access_key, session_token, format, structure, compression_method\n"
                                      "All signatures supports optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

    String getName() const override
    {
        return name;
    }

    String getSignature() const override
    {
        return signature;
    }

protected:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "S3Cluster"; }
};

}

#endif
