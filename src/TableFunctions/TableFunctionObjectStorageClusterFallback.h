#pragma once
#include "config.h"
#include <TableFunctions/TableFunctionObjectStorageCluster.h>

namespace DB
{

/**
* Class implementing s3/hdfs/azureBlobStorage(...) table functions,
* which allow to use simple or distributed function variant based on settings.
* If setting `object_storage_cluster_function_cluster` is empty,
* simple single-host variant is used, if setting not empty, cluster variant is used.
* `SELECT * FROM s3('s3://...', ...) SETTINGS object_storage_cluster_function_cluster='cluster'`
* is equal to
* `SELECT * FROM s3Cluster('cluster', 's3://...', ...)`
*/

template <typename Definition, typename Base>
class TableFunctionObjectStorageClusterFallback : public Base
{
public:
    using BaseCluster = Base;
    using BaseSimple = BaseCluster::Base;

    virtual ~TableFunctionObjectStorageClusterFallback() override = default;

    static constexpr auto name = Definition::name;

    String getName() const override { return name; }

private:
    const char * getStorageTypeName() const override
    {
        return is_cluster_function ? Definition::storage_type_cluster_name : Definition::storage_type_name;
    }

    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    void parseArgumentsImpl(ASTs & args, const ContextPtr & context) override;

    bool is_cluster_function = false;
};

}
