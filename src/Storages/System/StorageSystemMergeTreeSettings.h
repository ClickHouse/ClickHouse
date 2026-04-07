#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** implements system table "merge_tree_settings" and "replicated_merge_tree_settings",
  *  which allows to get information about the current MergeTree settings.
  */
template <bool replicated>
class SystemMergeTreeSettings final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return replicated ? "SystemReplicatedMergeTreeSettings" : "SystemMergeTreeSettings"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
