#pragma once

#ifdef OS_LINUX /// Because of 'sigqueue' functions and RT signals.

#include <mutex>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class Context;


/// Allows to introspect stack trace of all server threads.
/// It acts like an embedded debugger.
/// More than one instance of this table cannot be used.
class StorageSystemStackTrace final : public IStorageSystemOneBlock<StorageSystemStackTrace>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemStackTrace> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemStackTrace>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemStackTrace(CreatePasskey, TArgs &&... args) : StorageSystemStackTrace{std::forward<TArgs>(args)...}
    {
    }

    String getName() const override { return "SystemStackTrace"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    explicit StorageSystemStackTrace(const StorageID & table_id_);

    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    mutable std::mutex mutex;

    Poco::Logger * log;
};

}

#endif
