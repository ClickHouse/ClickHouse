#pragma once
#include <fcntl.h>
#include "config.h"

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

    size_t write_full(const String & oid, const char * data, size_t length);

    size_t write(const String & oid, const char * data, size_t length, uint64_t offset = 0);

    size_t append(const String & oid, const char * data, size_t length);

    void stat(const String & oid, uint64_t * size, struct timespec * mtime);

    bool exists(const String & oid);

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
