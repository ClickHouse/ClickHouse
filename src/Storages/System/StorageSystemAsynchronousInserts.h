#pragma once

#include <base/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/** Implements the system table `asynhronous_inserts`,
 *  which contains information about pending asynchronous inserts in queue.
*/
class StorageSystemAsynchronousInserts final :
    public shared_ptr_helper<StorageSystemAsynchronousInserts>,
    public IStorageSystemOneBlock<StorageSystemAsynchronousInserts>
{
public:
    std::string getName() const override { return "SystemAsynchronousInserts"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct shared_ptr_helper<StorageSystemAsynchronousInserts>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
