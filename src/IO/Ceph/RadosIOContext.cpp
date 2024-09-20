#include "RadosIOContext.h"

#if USE_CEPH

#include <librados.hpp>
#include <cstring>
#include <fmt/format.h>
#include "Common/logger_useful.h"
#include <Common/safe_cast.h>
#include <Common/Exception.h>
#include <Poco/Error.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CEPH_ERROR;
    extern const int LOGICAL_ERROR;
}

RadosIOContext::RadosIOContext(std::shared_ptr<librados::Rados> rados_, const String & pool_, const String & ns_, bool connect_)
    : rados(std::move(rados_)), pool(pool_), ns(ns_)
{
    if (connect_)
        connect();
}

RadosIOContext::RadosIOContext(librados::IoCtx io_ctx_)
    : io_ctx(std::move(io_ctx_))
{
    if (!io_ctx.is_valid())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RadosIOContext: io_ctx is not valid");
    connected = true;
}

void RadosIOContext::connect()
{
    if (connected)
        return;

    if (!rados)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RadosIOContext: rados is nullptr");

    if (auto ec = rados->connect(); ec != 0 && ec != -EISCONN)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RadosIOContext: rados cluster is not connected yet");

    if (auto ec = rados->ioctx_create(pool.c_str(), io_ctx); ec < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot create io_ctx for pool `{}`. Error: {}", pool, Poco::Error::getMessage(-ec));

    if (!ns.empty())
        io_ctx.set_namespace(ns);

    connected = true;
}

void RadosIOContext::close()
{
    if (!connected)
        return;

    io_ctx.close();
    connected = false;
}

void RadosIOContext::assertConnected() const
{
    if (!connected)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RadosRadosIO is not connected");
}

size_t RadosIOContext::getMaxObjectSize() const
{
    assertConnected();
    String val;
    if (auto ec = rados->conf_get("osd_max_object_size", val); ec < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot get max object size. Error: {}", Poco::Error::getMessage(-ec));
    if (val.empty())
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot get max object size. Empty value");
    LOG_DEBUG(getLogger("RadosIOContext"), "Max object size: {}", val);
    return std::stoull(val);
}

size_t RadosIOContext::read(const String & oid, char * data, size_t length, uint64_t offset)
{
    assertConnected();
    ceph::bufferlist bl = ceph::bufferlist::static_from_mem(data, length);
    ceph::bufferptr bp = ceph::buffer::create_static(safe_cast<int>(length), data);
    bl.push_back(bp);
    auto bytes_read = io_ctx.read(oid, bl, length, offset);

    if (bytes_read < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot read from object `{}:{}`. Error: {}", pool, oid, Poco::Error::getMessage(-bytes_read));
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

size_t RadosIOContext::writeFull(const String & oid, const char * data, size_t length)
{
    assertConnected();
    ceph::bufferlist bl;
    bl.append(data, safe_cast<int>(length));
    if (auto ec = io_ctx.write_full(oid, bl); ec < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot write to object `{}:{}`. Error: {}", pool, oid, Poco::Error::getMessage(-ec));
    return length;
}

size_t RadosIOContext::write(const String & oid, const char * data, size_t length, uint64_t offset)
{
    assertConnected();
    ceph::bufferlist bl;
    bl.append(data, safe_cast<int>(length));
    if (auto ec = io_ctx.write(oid, bl, length, offset); ec < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot write to object `{}:{}`. Error: {}", pool, oid, Poco::Error::getMessage(-ec));
    return length;
}

size_t RadosIOContext::append(const String & oid, const char * data, size_t length)
{
    assertConnected();
    ceph::bufferlist bl;
    bl.append(data, safe_cast<int>(length));
    if (auto ec = io_ctx.append(oid, bl, length); ec < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot append to object `{}:{}`. Error: {}", pool, oid, Poco::Error::getMessage(-ec));
    return length;
}

void RadosIOContext::stat(const String & oid, uint64_t * size, struct timespec * mtime)
{
    assertConnected();
    if (auto ec = io_ctx.stat2(oid, size, mtime); ec < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot get object `{}:{}` stats. Error: {}", pool, oid, Poco::Error::getMessage(-ec));
}

bool RadosIOContext::exists(const String & oid)
{
    assertConnected();
    return io_ctx.stat2(oid, nullptr, nullptr) == 0;
}

void RadosIOContext::remove(const String & oid, bool if_exists)
{
    assertConnected();
    if (auto ec = io_ctx.remove(oid); ec < 0 && (!if_exists || ec != -ENOENT))
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot remove object `{}:{}`. Error: {}", pool, oid, Poco::Error::getMessage(-ec));
}

void RadosIOContext::remove(const std::vector<String> & oids)
{
    assertConnected();
    std::vector<std::unique_ptr<librados::AioCompletion>> completions;
    for (const auto & oid : oids)
    {
        auto completion = std::unique_ptr<librados::AioCompletion>(librados::Rados::aio_create_completion());
        if (auto ec = io_ctx.aio_remove(oid, completion.get()); ec < 0)
        {
            for (auto & c : completions)
                c->wait_for_complete(); /// No checking of return code, because we already have an error
            throw Exception(ErrorCodes::CEPH_ERROR, "Cannot remove object `{}:{}`. Error: {}", pool, oid, Poco::Error::getMessage(-ec));
        }
        completions.emplace_back(std::move(completion));
    }
    std::optional<Exception> exception;
    for (auto & c : completions)
    {
        if (auto ec = c->wait_for_complete(); ec < 0 && ec != -ENOENT)
            if (!exception)
                exception.emplace(Exception(ErrorCodes::CEPH_ERROR, "Cannot remove objects. Error: {}", Poco::Error::getMessage(-ec)));
    }
    if (exception)
        throw std::move(*exception);
}

void RadosIOContext::create(const String & oid, bool if_not_exists)
{
    assertConnected();
    if (auto ec = io_ctx.create(oid, !if_not_exists); ec < 0 && (!if_not_exists || ec != -EEXIST))
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot remove object `{}:{}`. Error: {}", pool, oid, Poco::Error::getMessage(-ec));
}

GetRadosObjectAttributeResult RadosIOContext::tryGetAttribute(const String & oid, const String & attr, bool if_exists, std::optional<Exception> * exception)
{
    assertConnected();
    String res;
    ceph::bufferlist bl = ceph::bufferlist::static_from_string(res);
    if (auto ec = io_ctx.getxattr(oid, attr.c_str(), bl); ec < 0)
    {
        if (if_exists)
        {
            if (ec == -ENOENT)
                return GetRadosObjectAttributeResult{.object_exists = false, .value = {}}; /// Object doesn't exist
            if (ec == -ENODATA)
                return GetRadosObjectAttributeResult{.object_exists = true, .value = {}}; /// Object exists, but attribute is not set
        }
        if (exception)
            exception->emplace(Exception(ErrorCodes::CEPH_ERROR, "Cannot get attribute `{}` for object `{}:{}`. Error: {}", attr, pool, oid, Poco::Error::getMessage(-ec)));
    }
    if (!bl.is_provided_buffer(res.data()))
    {
        res.resize(bl.length());
        bl.begin().copy(bl.length(), res.data());
    }
    return GetRadosObjectAttributeResult{.object_exists = true, .value = std::move(res)};
}

String RadosIOContext::getAttribute(const String & oid, const String & attr)
{
    std::optional<Exception> exception;
    auto res = tryGetAttribute(oid, attr, false, &exception);
    if (exception)
        throw std::move(*exception);
    return std::move(*res.value);
}

GetRadosObjectAttributeResult RadosIOContext::getAttributeIfExists(const String & oid, const String & attr)
{
    std::optional<Exception> exception;
    auto res = tryGetAttribute(oid, attr, true, &exception);
    if (exception)
        throw std::move(*exception);
    return res;
}

void RadosIOContext::setAttribute(const String & oid, const String & attr, const String & value)
{
    assertConnected();
    ceph::bufferlist bl;
    bl.append(value.c_str(), static_cast<UInt32>(value.length()));
    if (auto ec = io_ctx.setxattr(oid, attr.c_str(), bl); ec < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot set attribute `{}` for object `{}:{}`. Error: {}", attr, pool, oid, Poco::Error::getMessage(-ec));
}

void RadosIOContext::getAttributes(const String & oid, std::map<String, String> & attrs)
{
    assertConnected();
    std::map<std::string, ceph::bufferlist> xattrs;
    if (auto ec = io_ctx.getxattrs(oid, xattrs); ec < 0)
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot get attributes for object `{}:{}`. Error: {}", pool, oid, Poco::Error::getMessage(-ec));
    for (auto & [key, value] : xattrs)
    {
        /// TODO: zero copy from bufferlist to string
        attrs.emplace(key, value.to_str());
    }
}

void RadosIOContext::setAttributes(const String & oid, const std::map<String, String> & attrs)
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
        throw Exception(ErrorCodes::CEPH_ERROR, "Cannot set attributes for object `{}:{}`. Error: {}", pool, oid, Poco::Error::getMessage(-ec));
}

std::optional<ObjectMetadata> RadosIOContext::tryGetMetadata(const String & oid, std::optional<Exception> * exception)
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
            exception->emplace(Exception(ErrorCodes::CEPH_ERROR, "Cannot get metadata for object `{}:{}`. Error: {}", pool, oid, Poco::Error::getMessage(-ec)));
        return {};

    }
    ObjectMetadata metadata;
    metadata.size_bytes = size;
    metadata.last_modified = Poco::Timestamp::fromEpochTime(mtime.tv_sec);
    for (auto && [key, value] : xattrs)
        metadata.attributes.emplace(key, value.to_str()); /// TODO: zero copy from bufferlist to string
    return std::move(metadata);
}

ObjectMetadata RadosIOContext::getMetadata(const String & oid)
{
    assertConnected();
    std::optional<Exception> exception;
    auto res = tryGetMetadata(oid, &exception);
    if (exception)
        throw std::move(*exception);
    return std::move(*res);

}

}

#endif
