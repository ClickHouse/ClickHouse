#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/** Implements the system table `asynhronous_inserts`,
 *  which contains information about pending asynchronous inserts in queue.
*/
class StorageSystemAsynchronousInserts final : public IStorageSystemOneBlock<StorageSystemAsynchronousInserts>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemAsynchronousInserts> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemAsynchronousInserts>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemAsynchronousInserts(CreatePasskey, TArgs &&... args) : StorageSystemAsynchronousInserts{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemAsynchronousInserts"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
