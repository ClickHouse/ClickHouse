#pragma once
#include <cstdint>
#include <optional>
#include <fcntl.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <IO/ReadSettings.h>
#include <config.h>

#if USE_CEPH

#include <memory>
#include <librados.hpp>
#include <base/types.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

using RadosIterator = librados::NObjectIterator;

struct GetRadosObjectAttributeResult
{
    bool object_exists;
    std::optional<String> value;
};


/// Implement detail of Ceph rados IO. Do not print any sensitive information in logs
class RadosIOContext
{
public:
    RadosIOContext(std::shared_ptr<librados::Rados> rados_, const String & pool_, const String & ns_ = "", bool connect_ = true);
    explicit RadosIOContext(librados::IoCtx io_ctx_);
    RadosIOContext(const RadosIOContext &) = delete;
    RadosIOContext(RadosIOContext &&) = default;

    void connect();

    void close();

    void assertConnected() const;

    size_t getMaxObjectSize() const;

    RadosIterator begin() { return io_ctx.nobjects_begin(); }

    const RadosIterator & end() const { return io_ctx.nobjects_end(); }

    size_t read(const String & oid, char * data, size_t length, uint64_t offset = 0);

    size_t writeFull(const String & oid, const char * data, size_t length);

    size_t write(const String & oid, const char * data, size_t length, uint64_t offset = 0);

    size_t append(const String & oid, const char * data, size_t length);

    void stat(const String & oid, uint64_t * size, struct timespec * mtime);

    GetRadosObjectAttributeResult tryGetAttribute(const String & oid, const String & attr, bool if_exists, std::optional<Exception> * exception = nullptr);

    String getAttribute(const String & oid, const String & attr);

    GetRadosObjectAttributeResult getAttributeIfExists(const String & oid, const String & attr);

    void setAttribute(const String & oid, const String & attr, const String & value);

    void getAttributes(const String & oid, std::map<String, String> & attrs);

    void setAttributes(const String & oid, const std::map<String, String> & attrs);

    std::optional<ObjectMetadata> tryGetMetadata(const String & oid, std::optional<Exception> * exception = nullptr);

    ObjectMetadata getMetadata(const String & oid);

    bool exists(const String & oid);

    void create(const String & oid, bool if_not_exists = false);

    void remove(const String & oid, bool if_exists = false);

    void remove(const std::vector<String> & oids);

    void sync() {}

private:
    /// In some case can own rados object?
    std::shared_ptr<librados::Rados> rados;
    /// librados::IoCtx is thread-safe unless when changing pool, namespace, snapshot, or object locator
    librados::IoCtx io_ctx;
    String pool;
    String ns;
    bool connected{false};
};

}

#endif
