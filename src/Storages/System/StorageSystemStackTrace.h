#pragma once

#ifdef OS_LINUX /// Because of 'sigqueue' functions and RT signals.

#include <mutex>
#include <Storages/IStorage.h>

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
class StorageSystemStackTrace final : public IStorage
{
public:
    explicit StorageSystemStackTrace(const StorageID & table_id_);

    String getName() const override { return "SystemStackTrace"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool isSystemStorage() const override { return true; }

protected:
    mutable std::mutex mutex;
    Poco::Logger * log;
};

}

#endif
