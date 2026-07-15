#pragma once

#include <Databases/DatabaseReplicated.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class Context;
class Cluster;
class DatabaseReplicated;

/** Implements system table 'clusters'
  *  that allows to obtain information about available clusters
  *  (which may be specified in Distributed tables).
  */
class StorageSystemClusters final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemClusters"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    using NameAndCluster = std::pair<String, std::shared_ptr<Cluster>>;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8> columns_mask) const override;
    static void writeCluster(MutableColumns & res_columns, const std::vector<UInt8> & columns_mask, const NameAndCluster & name_and_cluster, const DatabaseReplicated * replicated);
    bool supportsColumnsMask() const override { return true; }
};

}
