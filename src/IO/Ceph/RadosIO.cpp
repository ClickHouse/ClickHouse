#include "RadosIO.h"
#include <rados/librados.hpp>

#if USE_CEPH

#include <cstring>
#include <buffer_fwd.h>
#include <fmt/format.h>
#include "Common/safe_cast.h"
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CEPH_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace Ceph
{

RadosIO::RadosIO(std::shared_ptr<librados::Rados> rados_, const String & pool_, const String & ns_, bool connect_)
    : rados(std::move(rados_)), pool(pool_), ns(ns_)
{
    if (connect_)
        connect();
}

RadosIO::RadosIO(librados::IoCtx io_ctx_)
    : io_ctx(std::move(io_ctx_))
{
    if (!io_ctx_.is_valid())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RadosIO: io_ctx is not valid");
    connected = true;
}

void RadosIO::connect()
{
    if (connected)
        return;

    if (!rados)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RadosIO: rados is nullptr");

    if (auto ec = rados->connect(); ec != 0 && ec != -EISCONN)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RadosIO: rados cluster is not connected yet");

    if (auto ec = rados->ioctx_create(pool.c_str(), io_ctx); ec < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot create io_ctx for pool `{}`. Error: {}", pool, strerror(-ec));

    if (!ns.empty())
        io_ctx.set_namespace(ns);

    connected = true;
}

void RadosIO::close()
{
    if (!connected)
        return;

    io_ctx.close();
    connected = false;
}

void RadosIO::assertConnected() const
{
    if (!connected)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RadosRadosIO is not connected");
}

size_t RadosIO::read(const String & oid, char * data, size_t length, uint64_t offset)
{
    assertConnected();
    ceph::bufferlist bl;
    ceph::bufferptr bp = ceph::buffer::create_static(safe_cast<int>(length), data);
    bl.push_back(bp);
    auto bytes_read = io_ctx.read(oid, bl, length, offset);

    if (bytes_read < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot read from object `{}:{}`. Error: {}", pool, oid, strerror(-bytes_read));
    if (bl.length() > length)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Error while reading object `{}:{}`: expect maximum {} bytes, get {} bytes", pool, oid, length, bl.length());

    /// I expect that read data will be populated in the provided buffer, nevertheless, bufferlist doesn't always work this way
    /// This implementation is borrowed from librados_c.cc implementation of method `rados_read`
    if (!bl.is_provided_buffer(data))
    {
        bl.begin().copy(bl.length(), data);
        bytes_read = bl.length();
    }
    return bytes_read;
}

size_t RadosIO::writeFull(const String & oid, const char * data, size_t length)
{
    assertConnected();
    ceph::bufferlist bl;
    bl.append(data, safe_cast<int>(length));
    if(auto ec = io_ctx.write_full(oid, bl); ec < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot write to object `{}:{}`. Error: {}", pool, oid, strerror(-ec));
    return length;
}

size_t RadosIO::write(const String & oid, const char * data, size_t length, uint64_t offset)
{
    assertConnected();
    ceph::bufferlist bl;
    bl.append(data, safe_cast<int>(length));
    if (auto ec = io_ctx.write(oid, bl, length, offset); ec < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot write to object `{}:{}`. Error: {}", pool, oid, strerror(-ec));
    return length;
}

size_t RadosIO::append(const String & oid, const char * data, size_t length)
{
    assertConnected();
    ceph::bufferlist bl;
    bl.append(data, safe_cast<int>(length));
    if (auto ec = io_ctx.append(oid, bl, length); ec < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot append to object `{}:{}`. Error: {}", pool, oid, strerror(-ec));
    return length;
}

void RadosIO::stat(const String & oid, uint64_t * size, struct timespec * mtime)
{
    assertConnected();
    if (auto ec = io_ctx.stat2(oid, size, mtime); ec < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot get object `{}:{}` stats. Error: {}", pool, oid, strerror(-ec));
}

bool RadosIO::exists(const String & oid)
{
    assertConnected();
    return io_ctx.stat2(oid, nullptr, nullptr) == 0;
}

void RadosIO::remove(const String & oid, bool if_exists)
{
    assertConnected();
    if (auto ec = io_ctx.remove(oid); ec < 0 && (!if_exists || ec != -ENOENT))
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot remove object `{}:{}`. Error: {}", pool, oid, strerror(-ec));
}

String RadosIO::getAttribute(const String & oid, const String & attr)
{
    assertConnected();
    ceph::bufferlist bl;
    if (auto ec = io_ctx.getxattr(oid, attr.c_str(), bl); ec < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot get attribute `{}` for object `{}:{}`. Error: {}", attr, pool, oid, strerror(-ec));
    String res(bl.c_str(), bl.length());
    return res;
}

void RadosIO::setAttribute(const String & oid, const String & attr, const String & value)
{
    assertConnected();
    ceph::bufferlist bl;
    bl.append(value.c_str(), static_cast<UInt32>(value.length()));
    if (auto ec = io_ctx.setxattr(oid, attr.c_str(), bl); ec < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot set attribute `{}` for object `{}:{}`. Error: {}", attr, pool, oid, strerror(-ec));
}

void RadosIO::getAttributes(const String & oid, std::map<String, String> & attrs)
{
    assertConnected();
    std::map<std::string, ceph::bufferlist> xattrs;
    if (auto ec = io_ctx.getxattrs(oid, xattrs); ec < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot get attributes for object `{}:{}`. Error: {}", pool, oid, strerror(-ec));
    for (auto && [key, value] : xattrs)
    {
        attrs.emplace(key, String(value.c_str(), value.length()));
    }
}

void RadosIO::setAttributes(const String & oid, const std::map<String, String> & attrs)
{
    assertConnected();
    librados::ObjectWriteOperation ops;
    for (auto && [key, value] : attrs)
    {
        ceph::bufferlist bl;
        bl.append(value.c_str(), static_cast<UInt32>(value.length()));
        ops.setxattr(key.c_str(), bl);
    }
    if (auto ec = io_ctx.operate(oid, &ops); ec < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot set attributes for object `{}:{}`. Error: {}", pool, oid, strerror(-ec));
}

std::optional<ObjectMetadata> RadosIO::tryGetMetadata(const String & oid, std::optional<Exception> * exception)
{
    assertConnected();
    std::map<std::string, ceph::bufferlist> xattrs;
    size_t size;
    struct timespec mtime;
    librados::ObjectReadOperation ops;
    int rv1, rv2;
    ops.stat2(&size, &mtime, &rv1);
    ops.getxattrs(&xattrs, &rv2);
    if (auto ec = io_ctx.operate(oid, &ops, nullptr); ec < 0)
    {
        if (exception)
            exception->emplace(Exception(ErrorCodes::CEPH_ERROR, "Cannot get metadata for object `{}:{}`. Error: {}", pool, oid, strerror(-ec)));
        return {};

    }
    ObjectMetadata metadata;
    metadata.size_bytes = size;
    metadata.last_modified = Poco::Timestamp::fromEpochTime(mtime.tv_sec);
    for (auto && [key, value] : xattrs)
        metadata.attributes.emplace(key, String(value.c_str(), value.length()));
    return std::move(metadata);
}

ObjectMetadata RadosIO::getMetadata(const String & oid)
{
    assertConnected();
    std::optional<Exception> exception;
    if (auto metadata = tryGetMetadata(oid, &exception))
        return *metadata;
    throw std::move(*exception);

}

}

}
#endif
