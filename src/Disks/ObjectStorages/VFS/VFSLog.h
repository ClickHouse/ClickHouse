#pragma once

#include <Core/UUID.h>
#include <Disks/ObjectStorages/VFS/AppendLog.h>


#include <variant>

namespace DB
{

enum class VFSAction : uint8_t
{
    LINK,
    UNLINK,
    REQUEST
};


struct WALInfo
{
    String replica;
    UUID id;
    UInt64 index;

    bool operator==(const WALInfo &) const = default;
};

struct VFSEvent
{
    String remote_path;
    String local_path;
    std::optional<WALInfo> orig_wal;
    Poco::Timestamp timestamp;
    VFSAction action;

    bool operator==(const VFSEvent &) const = default;
};

struct VFSLogItem
{
    VFSEvent event;
    WALInfo wal;

    bool operator==(const VFSLogItem &) const = default;
};

using VFSLogItems = std::vector<VFSLogItem>;

struct DiskObjectStorageMetadata;
class VFSLog
{
public:
    VFSLog(const String & log_dir);
    
    void link(const String & remote_path, const String & local_path);
    void link(const DiskObjectStorageMetadata & metadata);
    void unlink(const String & remote_path, const String & local_path);
    void unlink(const DiskObjectStorageMetadata & metadata);

    UInt64 write(const VFSEvent & event);
    VFSLogItems read(size_t count) const;
    size_t dropUpTo(UInt64 index);
    size_t size() const;

private:
    WAL::AppendLog wal;
};

using VFSLogPtr = std::shared_ptr<VFSLog>;

}
