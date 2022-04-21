#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/// Implements the `mutations` system table, which provides information about the status of mutations
/// in the MergeTree tables.
class StorageSystemMutations final : public IStorageSystemOneBlock<StorageSystemMutations>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemMutations> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemMutations>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemMutations(CreatePasskey, TArgs &&... args) : StorageSystemMutations{std::forward<TArgs>(args)...}
    {
    }

    String getName() const override { return "SystemMutations"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
