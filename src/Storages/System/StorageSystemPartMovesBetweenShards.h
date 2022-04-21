#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


class StorageSystemPartMovesBetweenShards final : public IStorageSystemOneBlock<StorageSystemPartMovesBetweenShards>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemPartMovesBetweenShards> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemPartMovesBetweenShards>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemPartMovesBetweenShards(CreatePasskey, TArgs &&... args) : StorageSystemPartMovesBetweenShards{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemShardMoves"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
