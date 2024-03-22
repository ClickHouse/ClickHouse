#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


class StorageSystemPartMovesBetweenShards final : public IStorageSystemOneBlock<StorageSystemPartMovesBetweenShards>
{
public:
    std::string getName() const override { return "SystemShardMoves"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
