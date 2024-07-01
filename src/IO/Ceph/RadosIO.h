#pragma once
#include <cstdint>
#include <optional>
#include <fcntl.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <IO/ReadSettings.h>
#include <config.h>

#if USE_CEPH

#include <atomic>
#include <memory>
#include <rados/librados.hpp>
#include <base/types.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
namespace Ceph
{

/// Implement detail of Ceph rados IO. Do not print any sensitive information in logs
/// TODO: add connection pool
class RadosIO
{
public:
    RadosIO(std::shared_ptr<librados::Rados> rados_, const String & pool_, const String & ns_ = "", bool connect_ = false);
    explicit RadosIO(librados::IoCtx io_ctx_);
    RadosIO(const RadosIO &) = delete;
    RadosIO(RadosIO &&) = default;

    void connect();

    void assertConnected();

    size_t read(const String & oid, char * data, size_t length, uint64_t offset = 0);

    size_t writeFull(const String & oid, const char * data, size_t length);

    size_t write(const String & oid, const char * data, size_t length, uint64_t offset = 0);

    size_t append(const String & oid, const char * data, size_t length);

    void stat(const String & oid, uint64_t * size, struct timespec * mtime);

    String getAttribute(const String & oid, const String & attr);

    void getAttributes(const String & oid, std::map<String, String> & attrs);

    std::optional<ObjectMetadata> tryGetMetadata(const String & oid, std::optional<Exception> * exception);

    ObjectMetadata getMetadata(const String & oid, uint64_t * size, struct timespec * mtime, std::map<String, String> & attrs);

    bool exists(const String & oid);

    void remove(const String & oid, bool if_exists = false);

    void list(size_t max_objects, std::vector<String> & oids);

    void listWithPrefix(const String & prefix, size_t max_objects, std::vector<String> & oids);

    void sync() {}

private:
    /// In some case can own rados object?
    std::shared_ptr<librados::Rados> rados;
    librados::IoCtx io_ctx;
    String pool;
    String ns;
    bool connected{false};
};

}

}

#endif
