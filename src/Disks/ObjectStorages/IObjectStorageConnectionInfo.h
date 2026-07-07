#pragma once
#include <base/EnumReflection.h>
#include <Disks/DiskType.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <Common/SipHash.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class ReadBuffer;
class WriteBuffer;
class ReadBufferFromFileBase;
class WriteBufferFromFileBase;
struct ReadSettings;
struct WriteSettings;
class RemoteHostFilter;

class IObjectStorageConnectionInfo;
using ObjectStorageConnectionInfoPtr = std::shared_ptr<IObjectStorageConnectionInfo>;

/// A class which contains information about how to connect
/// and later read/write from/to object storage.
/// Exposes methods to serialize this information to later be send over network.
class IObjectStorageConnectionInfo
{
public:
    virtual ~IObjectStorageConnectionInfo() = default;

    /// Deserialize connection info from `in`.
    /// No protocol version is passed, because it is expected that
    /// in writeBinary we used "mutual" protocol version between sender and receiver.
    static ObjectStorageConnectionInfoPtr readBinary(ReadBuffer & in, const std::string & user_id);

    virtual bool equals(const IObjectStorageConnectionInfo &) const = 0;

    /// Serialize connection info into `out`.
    /// Take into account protocol version if needed.
    /// `protocol_version` must be a mutual version between sender and receiver.
    void writeBinary(size_t protocol_version, WriteBuffer & out);

    /// Get object storage type: S3, Azure, GCP, etc.
    virtual ObjectStorageType getType() const = 0;

    /// Calculate hash of the stored connection info.
    virtual void updateHash(SipHash & hash, bool include_credentials) const = 0;

    /// Refresh credentials if changed.
    virtual bool refreshCredentials() { return false; }

    /// Create reader to be able to read from object storage.
    virtual std::unique_ptr<ReadBufferFromFileBase> createReader(
        const StoredObject & object,
        const ReadSettings & read_settings) = 0;

protected:
    /// Serialize object storage specific connection info.
    virtual void writeBinaryImpl(size_t protocol_version, WriteBuffer & out) = 0;
};

/// Used for temporary data in cache,
/// where data is not stored in object storage,
/// but written by client directly to cache.
class NoneStorageConnectionInfo : public IObjectStorageConnectionInfo
{
public:
    ObjectStorageType getType() const override { return ObjectStorageType::None; }

    void updateHash(SipHash &, bool) const override {}

    bool equals(const IObjectStorageConnectionInfo & other) const override
    {
        const auto * none_info = dynamic_cast<const NoneStorageConnectionInfo *>(&other);
        return none_info;
    }

    std::unique_ptr<ReadBufferFromFileBase> createReader(
        const StoredObject & object,
        const ReadSettings &) override
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "File {} (local_path: {}) should be read from local disk",
            object.remote_path, object.local_path);
    }

protected:
    void writeBinaryImpl(size_t, WriteBuffer &) override {}
};

}
