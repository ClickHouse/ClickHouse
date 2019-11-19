#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** implements system table "merge_tree_settings", which allows to get information about the current MergeTree settings.
  */
class SystemMergeTreeSettings : public ext::shared_ptr_helper<SystemMergeTreeSettings>, public IStorageSystemOneBlock<SystemMergeTreeSettings>
{
public:
    std::string getName() const override { return "SystemMergeTreeSettings"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;
};

}
