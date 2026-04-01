#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;
class Cluster;

/** Implements system table 'certificates'
  *  that allows to obtain information about available certificates
  *  and their sources.
  */
class StorageSystemCertificates final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemCertificates"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
