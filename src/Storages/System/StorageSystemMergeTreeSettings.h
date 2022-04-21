#pragma once

#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** implements system table "merge_tree_settings" and "replicated_merge_tree_settings",
  *  which allows to get information about the current MergeTree settings.
  */
template <bool replicated>
class SystemMergeTreeSettings final : public IStorageSystemOneBlock<SystemMergeTreeSettings<replicated>>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<SystemMergeTreeSettings> create(TArgs &&... args)
    {
        return std::make_shared<SystemMergeTreeSettings>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit SystemMergeTreeSettings(CreatePasskey, TArgs &&... args) : SystemMergeTreeSettings{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return replicated ? "SystemReplicatedMergeTreeSettings" : "SystemMergeTreeSettings"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock<SystemMergeTreeSettings<replicated>>::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
