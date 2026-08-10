#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class StorageSystemBackgroundQueries final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemBackgroundQueries"; }
    static ColumnsDescription getColumnsDescription();

    void truncate(const ASTPtr & query_ast, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, TableExclusiveLockHolder & lock_holder) override;

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
