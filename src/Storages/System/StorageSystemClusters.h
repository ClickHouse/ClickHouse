#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class Context;

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

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8> columns_mask) const override;
    Block getFilterSampleBlock() const override;
    bool supportsColumnsMask() const override { return true; }

private:
    /// Whether any of the columns filled from the replica state in Keeper is requested.
    bool needsReplicasInfo(const std::vector<UInt8> & columns_mask, const ContextPtr & context) const;
};

}
