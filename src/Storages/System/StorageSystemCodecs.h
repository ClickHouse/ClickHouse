#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

/** Implements system table 'codecs', to get information for every codec.
 */
class StorageSystemCodecs final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemCodecs"; }
    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
};

}
