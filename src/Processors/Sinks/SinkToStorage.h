#pragma once
#include <Processors/ISink.h>
#include <Storages/TableLockHolder.h>

namespace DB
{

/// Sink which reads everything and do nothing with it.
class SinkToStorage : public ISink
{
public:
    using ISink::ISink;

    void addTableLock(const TableLockHolder & lock) { table_locks.push_back(lock); }

private:
    std::vector<TableLockHolder> table_locks;
};

using SinkToStoragePtr = std::shared_ptr<SinkToStorage>;


class NullSinkToStorage : public SinkToStorage
{
public:
    using SinkToStorage::SinkToStorage;
    std::string getName() const override { return "NullSinkToStorage"; }
    void consume(Chunk) override {}
};

}
