#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


class StorageSystemPartMovesBetweenShards final : public ext::shared_ptr_helper<StorageSystemPartMovesBetweenShards>, public IStorageSystemOneBlock<StorageSystemPartMovesBetweenShards>
{
    friend struct ext::shared_ptr_helper<StorageSystemPartMovesBetweenShards>;
public:
    std::string getName() const override { return "SystemShardMoves"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
