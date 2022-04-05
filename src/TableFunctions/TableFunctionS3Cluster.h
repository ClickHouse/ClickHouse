#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <TableFunctions/ITableFunction.h>
#include <Storages/ExternalDataSourceConfiguration.h>


namespace DB
{

class Context;

/**
 * s3cluster(cluster_name, source, [access_key_id, secret_access_key,] format, structure)
 * A table function, which allows to process many files from S3 on a specific cluster
 * On initiator it creates a connection to _all_ nodes in cluster, discloses asterics
 * in S3 file path and dispatch each file dynamically.
 * On worker node it asks initiator about next task to process, processes it.
 * This is repeated until the tasks are finished.
 */
class TableFunctionS3Cluster : public ITableFunction
{
public:
    static constexpr auto name = "s3Cluster";
    std::string getName() const override
    {
        return name;
    }

    bool hasStaticStructure() const override { return configuration.structure != "auto"; }

    bool needStructureHint() const override { return configuration.structure == "auto"; }

    void setStructureHint(const ColumnsDescription & structure_hint_) override { structure_hint = structure_hint_; }

protected:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "S3Cluster"; }

    AccessType getSourceAccessType() const override { return AccessType::S3; }

    ColumnsDescription getActualTableStructure(ContextPtr) const override;
    void parseArguments(const ASTPtr &, ContextPtr) override;

    StorageS3ClusterConfiguration configuration;
    ColumnsDescription structure_hint;
};

}

#endif
