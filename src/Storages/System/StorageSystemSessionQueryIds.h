#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements `system.session_query_ids` table that contains the query ids of queries
  * executed in the current session. The contents are scoped to the reader's session.
  */
class StorageSystemSessionQueryIds final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemSessionQueryIds"; }

    static ColumnsDescription getColumnsDescription();

    void truncate(const ASTPtr & query_ast, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, TableExclusiveLockHolder & lock_holder) override;

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
