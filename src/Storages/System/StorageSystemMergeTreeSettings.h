#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** implements system table "merge_tree_settings" and "replicated_merge_tree_settings",
  *  which allows to get information about the current MergeTree settings.
  */
template <bool replicated>
class SystemMergeTreeSettings final : public ext::shared_ptr_helper<SystemMergeTreeSettings<replicated>>,
                                      public IStorageSystemOneBlock<SystemMergeTreeSettings<replicated>>
{
    friend struct ext::shared_ptr_helper<SystemMergeTreeSettings<replicated>>;

public:
    std::string getName() const override { return replicated ? "SystemReplicatedMergeTreeSettings" : "SystemMergeTreeSettings"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock<SystemMergeTreeSettings<replicated>>::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;
};

}
