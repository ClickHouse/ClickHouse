#pragma once
#include <memory>
#include <optional>
#include <boost/container/flat_set.hpp>

#include <fmt/format.h>

#include <Disks/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

struct VFSSnapshotEntry
{
    String remote_path;
    int link_count = 0;

    // NOTE: weak ordering is used for sorting/finding entries via flat_set
    bool operator<(const VFSSnapshotEntry & entry) const { return remote_path < entry.remote_path; }
    bool operator==(const VFSSnapshotEntry & entry) const { return remote_path == entry.remote_path; }
};


class VFSSnapshotEntryStringSerializer
{
public:
    static VFSSnapshotEntry deserialize(ReadBuffer & read_buffer);
    static void serialize(const VFSSnapshotEntry & entry, WriteBuffer & write_buffer);
};


class IVFSSnapshotReadStream
{
public:
    using entry_type = std::optional<VFSSnapshotEntry>;

    /// Returns empty on EOF
    entry_type next()
    {
        if (finished)
            return {};

        if (entry_type res = nextImpl())
            return res;

        finished = true;
        return {};
    }

    virtual ~IVFSSnapshotReadStream() = default;
    bool isFinished() const { return finished; }

protected:
    virtual entry_type nextImpl() = 0;

    std::atomic_bool finished{false};
};
using VFSSnapshotReadStreamPtr = std::unique_ptr<IVFSSnapshotReadStream>;


class IVFSSnapshotWriteStream
{
public:
    using entry_type = std::optional<VFSSnapshotEntry>;

    void write(VFSSnapshotEntry && entry)
    {
        if (finished)
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "VFS snapshot write stream are already finished");

        writeImpl(std::move(entry));
    }

    void finalize()
    {
        finalizeImpl();
        finished = true;
    }

    virtual ~IVFSSnapshotWriteStream() = default;
    bool isFinished() const { return finished; }

protected:
    virtual void writeImpl(VFSSnapshotEntry &&) = 0;
    virtual void finalizeImpl() = 0;

private:
    std::atomic_bool finished{false};
};
using VFSSnapshotWriteStreamPtr = std::shared_ptr<IVFSSnapshotWriteStream>;


class VFSSnapshotSortingWriteStream : public IVFSSnapshotWriteStream
{
public:
    VFSSnapshotSortingWriteStream(VFSSnapshotWriteStreamPtr write_stream_);

private:
    void writeImpl(VFSSnapshotEntry && entry) override;
    void finalizeImpl() override;

    VFSSnapshotWriteStreamPtr write_stream;
    boost::container::flat_set<VFSSnapshotEntry> sorted_entries;
};
using VFSSnapshotSortingWriteStreamPtr = std::shared_ptr<VFSSnapshotSortingWriteStream>;


class IVFSSnapshotStorage
{
public:
    virtual ~IVFSSnapshotStorage() = default;

    virtual VFSSnapshotReadStreamPtr readSnapshot(const String & name) = 0;
    virtual VFSSnapshotWriteStreamPtr writeSnapshot(const String & name) = 0;
    virtual VFSSnapshotSortingWriteStreamPtr writeSnapshotWithSorting(const String & name) = 0;
    virtual Strings listSnapshots() const = 0;
    virtual size_t removeSnapshots(Strings names) = 0;
};
using VFSSnapshotStoragePtr = std::shared_ptr<IVFSSnapshotStorage>;

}
