#pragma once

#include <boost/noncopyable.hpp>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements system.warnings table that contains warnings about server configuration
  * to be displayed in clickhouse-client.
  */
class StorageSystemWarnings final : public IStorageSystemOneBlock<StorageSystemWarnings>, boost::noncopyable
{
public:
    std::string getName() const override { return "SystemWarnings"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const override;
};
}
