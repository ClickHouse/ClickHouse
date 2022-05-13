#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;
class Cluster;

/** Implements system table 'clusters'
  *  that allows to obtain information about available clusters
  *  (which may be specified in Distributed tables).
  */
class StorageSystemCertificates final : public IStorageSystemOneBlock<StorageSystemCertificates>
{
public:
    std::string getName() const override { return "SystemCertificates"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
