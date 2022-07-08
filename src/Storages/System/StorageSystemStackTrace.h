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
public:
    explicit StorageSystemStackTrace(const StorageID & table_id_);

    String getName() const override { return "SystemStackTrace"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    mutable std::mutex mutex;

    Poco::Logger * log;
};

}

#endif
