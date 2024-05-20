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
