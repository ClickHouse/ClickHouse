#pragma once

#include <boost/noncopyable.hpp>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/** Implements the system table `asynhronous_inserts`,
 *  which contains information about pending asynchronous inserts in queue.
*/
class StorageSystemAsynchronousInserts final : public IStorageSystemOneBlock<StorageSystemAsynchronousInserts>, boost::noncopyable
{
public:
    std::string getName() const override { return "SystemAsynchronousInserts"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
