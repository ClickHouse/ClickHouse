#pragma once
#include <IO/Lz4DeflatingWriteBuffer.h>
#include <IO/Lz4InflatingReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <boost/container/flat_set.hpp>
#include "StoredObject.h"

namespace DB
{
class IObjectStorage;
class ReadBuffer;
class WriteBuffer;

struct VFSSnapshotEntry
{
    String remote_path;
    int link_count = 0;

    bool operator<(const VFSSnapshotEntry & entry) const;
    bool operator==(const VFSSnapshotEntry & entry) const;
    static VFSSnapshotEntry deserialize(ReadBuffer & buf);
    void serialize(WriteBuffer & buf);
};

class IVFSSnapshotReadStream
{
public:
    virtual ~IVFSSnapshotReadStream() = default;
    using Entry = std::optional<VFSSnapshotEntry>;
    Entry next();
    constexpr bool isFinished() const { return finished; }

protected:
    virtual Entry impl() = 0;
    bool finished{false};
};

class IVFSSnapshotWriteStream
{
    bool finished{false};

public:
    virtual ~IVFSSnapshotWriteStream() = default;
    void write(VFSSnapshotEntry && entry);
    void finalize();
    constexpr bool isFinished() const { return finished; }

protected:
    virtual void impl(VFSSnapshotEntry &&) = 0;
    virtual void finalizeImpl() = 0;
};

class VFSSnapshotReadStream final : public IVFSSnapshotReadStream
{
    Entry impl() override;
    Lz4InflatingReadBuffer buf;

public:
    VFSSnapshotReadStream(IObjectStorage & storage, StoredObject && obj);
};

class VFSSnapshotReadStreamFromString : public IVFSSnapshotReadStream
{
    Entry impl() override;
    ReadBufferFromString buf;

public:
    VFSSnapshotReadStreamFromString(std::string_view data); // NOLINT
};

class VFSSnapshotWriteStream : public IVFSSnapshotWriteStream
{
public:
    VFSSnapshotWriteStream(IObjectStorage & storage, StoredObject && obj, int level);

protected:
    void impl(VFSSnapshotEntry && entry) override;
    void finalizeImpl() override;
    Lz4DeflatingWriteBuffer buf;
};

class VFSSnapshotSortingWriteStream : public IVFSSnapshotWriteStream
{
    IVFSSnapshotWriteStream & stream;

    void impl(VFSSnapshotEntry && entry) override;
    void finalizeImpl() override;

    boost::container::flat_set<VFSSnapshotEntry> entries;

public:
    explicit VFSSnapshotSortingWriteStream(IVFSSnapshotWriteStream & stream_);
};
}
