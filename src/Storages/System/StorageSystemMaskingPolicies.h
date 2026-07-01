#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/// Implements the `system.masking_policies` table, which allows you to get information about masking policies.
///
/// Masking policies can only be created and applied in ClickHouse Cloud; in open-source builds the table is
/// always empty. It is still present (together with the related access grants) so that introspection queries
/// such as `SHOW MASKING POLICIES` work and return an empty result instead of throwing.
class StorageSystemMaskingPolicies final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemMaskingPolicies"; }
    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
