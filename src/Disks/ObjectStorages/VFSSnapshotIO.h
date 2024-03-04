#pragma once
#include <IO/Lz4DeflatingWriteBuffer.h>
#include <IO/Lz4InflatingReadBuffer.h>
#include <boost/container/flat_set.hpp>

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
public:
    using entry_type = std::optional<VFSSnapshotEntry>;

    void write(VFSSnapshotEntry && entry);
    void finalize();
    virtual ~IVFSSnapshotWriteStream() = default;
    constexpr bool isFinished() const { return finished; }

protected:
    virtual void impl(VFSSnapshotEntry &&) = 0;
    virtual void finalizeImpl() = 0;

private:
    bool finished{false};
};

class VFSSnapshotReadStream final : public IVFSSnapshotReadStream
{
public:
    VFSSnapshotReadStream(IObjectStorage & storage, std::string_view name);

private:
    Entry impl() override;
    Lz4InflatingReadBuffer buf;
};

class VFSSnapshotWriteStream : public IVFSSnapshotWriteStream
{
public:
    VFSSnapshotWriteStream(IObjectStorage & storage, std::string_view name, int level);

protected:
    void impl(VFSSnapshotEntry && entry) override;
    void finalizeImpl() override;
    Lz4DeflatingWriteBuffer buf;
};

class VFSSnapshotSortingWriteStream : public IVFSSnapshotWriteStream
{
public:
    explicit VFSSnapshotSortingWriteStream(VFSSnapshotWriteStream & stream_);

private:
    VFSSnapshotWriteStream & stream;

    void impl(VFSSnapshotEntry && entry) override;
    void finalizeImpl() override;

    boost::container::flat_set<VFSSnapshotEntry> entries;
};
}
