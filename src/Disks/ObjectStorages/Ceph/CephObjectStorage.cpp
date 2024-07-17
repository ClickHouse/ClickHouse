#include "CephObjectStorage.h"
#include <cstring>
#include <ctime>
#include <memory>
#include <string>
#include <fcntl.h>

#include <IO/Ceph/RadosIO.h>
#include "Common/Exception.h"
#include "Common/ObjectStorageKey.h"
#include "Common/getRandomASCIIString.h"
#include "Core/Defines.h"
#include "Disks/ObjectStorages/Ceph/CephUtils.h"
#include "Disks/ObjectStorages/IObjectStorage.h"
#include "Disks/ObjectStorages/StoredObject.h"
#include "IO/Ceph/StriperWriteBufferFromRados.h"
#include "IO/WriteSettings.h"

#if USE_CEPH

#    include <Disks/ObjectStorages/ObjectStorageIteratorAsync.h>
#    include <IO/S3Common.h>

#    include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#    include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#    include <Disks/IO/ThreadPoolRemoteFSReader.h>
#    include <IO/ReadBufferFromCeph.h>
#    include <IO/WriteBufferFromCeph.h>
#    include <Interpreters/Context.h>
#    include <Common/Macros.h>
#    include <Common/MultiVersion.h>
#    include <Common/ProfileEvents.h>
#    include <Common/StringUtils.h>
#    include <Common/logger_useful.h>
#    include <Common/threadPoolCallbackRunner.h>


namespace ProfileEvents
{
extern const Event S3DeleteObjects;
extern const Event S3ListObjects;
extern const Event DiskS3DeleteObjects;
extern const Event DiskS3ListObjects;
}

namespace CurrentMetrics
{
extern const Metric ObjectStorageS3Threads;
extern const Metric ObjectStorageS3ThreadsActive;
extern const Metric ObjectStorageS3ThreadsScheduled;
}


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_ELEMENT_IN_CONFIG;
extern const int CEPH_ERROR;
}

namespace
{

class CephIteratorAsync final : public IObjectStorageIteratorAsync
{
public:
    CephIteratorAsync(
        std::shared_ptr<Ceph::RadosIO> io_impl_,
        Ceph::RadosIterator begin_,
        Ceph::RadosIterator end_,
        const String & prefix_,
        size_t max_list_size_)
        : IObjectStorageIteratorAsync(
              CurrentMetrics::ObjectStorageS3Threads,
              CurrentMetrics::ObjectStorageS3ThreadsActive,
              CurrentMetrics::ObjectStorageS3ThreadsScheduled,
              "ListObjectCephAsync")
        , io_impl(std::move(io_impl_))
        , current(begin_)
        , end(end_)
        , prefix(prefix_)
        , max_list_size(max_list_size_)
    {
    }

    ~CephIteratorAsync() override = default;

private:
    bool getBatchAndCheckNext(RelativePathsWithMetadata & batch) override
    {
        size_t count = 0;
        while (current != end && count < max_list_size)
        {
            auto object_id = current->get_oid();
            if (!RadosStriper::isRegularObject(object_id) || !RadosStriper::isFirstObjectInStripe(io_impl, object_id)
                || !object_id.starts_with(prefix))
                continue;
            batch.emplace_back(std::make_shared<RelativePathWithMetadata>(object_id, io_impl->getMetadata(object_id)));
            ++current;
            ++count;
        }
        return true;
    }

    std::shared_ptr<Ceph::RadosIO> io_impl;
    librados::NObjectIterator current;
    const librados::NObjectIterator end;
    const String prefix;
    size_t max_list_size;
};

}

bool CephObjectStorage::exists(const StoredObject & object) const
{
    auto get_attribute_result = io_impl->getAttributeIfExists(object.remote_path, RadosStriper::XATTR_OBJECT_COUNT);
    return get_attribute_result.object_exists;
}

std::unique_ptr<ReadBufferFromFileBase> CephObjectStorage::readObjects( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    StoredObjects rados_objects_to_read = getRadosObjects(objects, false, true);
    Strings names;
    for (const auto & object : rados_objects_to_read)
        names.push_back(object.remote_path);
    LOG_TEST(log, "Reading objects from Ceph: ({})", fmt::join(names, ", "));
    return readObjectsImpl(rados_objects_to_read, read_settings);
}

std::unique_ptr<ReadBufferFromFileBase> CephObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    return readObjects({object}, read_settings);
}

std::unique_ptr<WriteBufferFromFileBase> CephObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> attributes,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    /// TODO: Support append mode: if the object already exists, continue appending to it (and striping if needed)
    if (mode != WriteMode::Rewrite)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Ceph does not support append mode");

    ThreadPoolCallbackRunnerUnsafe<void> scheduler;
    if (write_settings.rados_allow_parallel_part_write)
        scheduler = threadPoolCallbackRunnerUnsafe<void>(getThreadPoolWriter(), "VFSWrite");

    WriteSettings disk_write_settings = patchSettings(write_settings);
    return std::make_unique<StriperWriteBufferFromRados>(
        io_impl, object.remote_path, buf_size, ceph_settings.get()->max_object_size, patchSettings(write_settings), attributes, scheduler);
}

ObjectStorageIteratorPtr CephObjectStorage::iterate(const std::string & path_prefix, size_t max_keys) const
{
    return std::make_shared<CephIteratorAsync>(io_impl, io_impl->begin(), io_impl->end(), path_prefix, max_keys);
}

void CephObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
    size_t count = 0;
    for (auto it = io_impl->begin(); it != io_impl->end() && count < max_keys; ++it)
    {
        auto object_id = it->get_oid();
        if (!RadosStriper::isRegularObject(object_id) || !RadosStriper::isFirstObjectInStripe(io_impl, object_id)
            || !object_id.starts_with(path))
            continue;
        children.emplace_back(std::make_shared<RelativePathWithMetadata>(object_id, io_impl->getMetadata(object_id)));
        ++count;
    }
}

void CephObjectStorage::removeObjectImpl(const StoredObject & object, bool if_exists)
{
    auto rados_objects = getRadosObjects({object}, if_exists, false);

    /// Removing the HEAD object last
    Strings names;
    for (ssize_t i = rados_objects.size(); i > 0; --i)
        names.push_back(rados_objects[i].remote_path);

    /// Except for the HEAD object, remove only if it exists. Non-head objects can be missing
    /// because of many reasons, e.g. crash on write, crash on remove
    if (!names.empty())
        io_impl->remove(names);

    /// The head object
    io_impl->remove(object.remote_path, if_exists);
}

void CephObjectStorage::removeObjectsImpl(const StoredObjects & objects, bool if_exists)
{
    for (const auto & object : objects)
        removeObjectImpl(object, if_exists);
}

void CephObjectStorage::removeObject(const StoredObject & object)
{
    removeObjectImpl(object, false);
}

void CephObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    removeObjectImpl(object, true);
}

void CephObjectStorage::removeObjects(const StoredObjects & objects)
{
    removeObjectsImpl(objects, false);
}

void CephObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    removeObjectsImpl(objects, true);
}

std::optional<ObjectMetadata> CephObjectStorage::tryGetObjectMetadata(const std::string & path) const
{
    return io_impl->tryGetMetadata(path);
}

ObjectMetadata CephObjectStorage::getObjectMetadata(const std::string & path) const
{
    return io_impl->getMetadata(path);
}

void CephObjectStorage::copyObject( // NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::optional<ObjectAttributes> object_to_attributes)
{
    auto in = readObject(object_from, read_settings);
    auto out = writeObject(
        object_to, WriteMode::Rewrite, /* attributes= */ object_to_attributes, /* buf_size= */ DBMS_DEFAULT_BUFFER_SIZE, write_settings);
    copyData(*in, *out);
    out->finalize();
}

void CephObjectStorage::setNewSettings(std::unique_ptr<CephObjectStorageSettings> && ceph_settings_)
{
    size_t rados_max_osd_object_size = io_impl->getMaxObjectSize();
    if (ceph_settings_->max_object_size == 0 || ceph_settings_->max_object_size > rados_max_osd_object_size)
        ceph_settings_->max_object_size = rados_max_osd_object_size;
    ceph_settings.set(std::move(ceph_settings_));
}

void CephObjectStorage::shutdown()
{
    io_impl->close();
    rados->shutdown();
}

void CephObjectStorage::startup()
{
    rados->connect();
    io_impl->connect();
}

void CephObjectStorage::applyNewSettings(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    ContextPtr /*context*/,
    const ApplyNewSettingsOptions & options)
{
    auto modified_settings = std::make_unique<CephObjectStorageSettings>();
    modified_settings->loadFromConfig(config, config_prefix);

    auto current_settings = ceph_settings.get();
    if (options.allow_client_change && for_disk_ceph)
    {
        auto new_rados = std::make_shared<librados::Rados>();
        new_rados->init(modified_settings->global_options.user.c_str());
        for (const auto & [key, value] : modified_settings->global_options)
        {
            if (auto ec = new_rados->conf_set(key.c_str(), value.c_str()); ec < 0)
                throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Failed to set Ceph option: {}. Error: {}", key, strerror(-ec));
        }
        auto new_io = std::make_shared<Ceph::RadosIO>(new_rados, endpoint.pool, endpoint.nspace);
        std::atomic_store(&rados, new_rados);
        std::atomic_store(&io_impl, new_io);
    }
    ceph_settings.set(std::move(modified_settings));
}

WriteSettings CephObjectStorage::patchSettings(const WriteSettings & write_settings) const
{
    auto modified_settings = write_settings;
    /// Ceph does not support cache on write operations, because a ClickHouse object can be written to multiple Ceph objects.
    modified_settings.enable_filesystem_cache_on_write_operations = false;
    return modified_settings;
}

std::unique_ptr<IObjectStorage> CephObjectStorage::cloneObjectStorage(
    const std::string & new_namespace,
    const Poco::Util::AbstractConfiguration & /*config*/,
    const std::string & /*config_prefix*/,
    ContextPtr /*context*/)
{
    auto new_ceph_settings = std::make_unique<CephObjectStorageSettings>(*ceph_settings.get());
    CephEndpoint new_endpoint;
    new_endpoint.mon_hosts = endpoint.mon_hosts;
    new_endpoint.pool = new_namespace;

    return std::make_unique<CephObjectStorage>(rados, std::move(new_ceph_settings), new_endpoint, disk_name, for_disk_ceph);
}

ObjectStorageKey CephObjectStorage::generateObjectKeyForPath(const std::string & /*path*/) const
{
    constexpr size_t key_name_total_size = 32;
    return ObjectStorageKey::createAsRelative(endpoint.nspace, getRandomASCIIString(key_name_total_size));
}

std::unique_ptr<ReadBufferFromFileBase> CephObjectStorage::readObjectsImpl( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    ReadSettings disk_read_settings = IObjectStorage::patchSettings(read_settings);

    auto global_context = Context::getGlobalContextInstance();

    auto read_buffer_creator = [this, disk_read_settings](bool, const StoredObject & object_) -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<ReadBufferFromCeph>(
            io_impl,
            object_.remote_path,
            disk_read_settings,
            /* use_external_buffer */ true);
    };

    switch (read_settings.remote_fs_method)
    {
        case RemoteFSReadMethod::read: {
            return std::make_unique<ReadBufferFromRemoteFSGather>(
                std::move(read_buffer_creator),
                objects,
                "ceph:" + endpoint.pool + "/",
                disk_read_settings,
                global_context->getFilesystemCacheLog(),
                /* use_external_buffer */ false);
        }
        case RemoteFSReadMethod::threadpool: {
            auto impl = std::make_unique<ReadBufferFromRemoteFSGather>(
                std::move(read_buffer_creator),
                objects,
                "ceph:" + endpoint.pool + "/",
                disk_read_settings,
                global_context->getFilesystemCacheLog(),
                /* use_external_buffer */ true);

            auto & reader = global_context->getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
            return std::make_unique<AsynchronousBoundedReadBuffer>(
                std::move(impl),
                reader,
                disk_read_settings,
                global_context->getAsyncReadCounters(),
                global_context->getFilesystemReadPrefetchesLog());
        }
    }
}

StoredObjects CephObjectStorage::getRadosObjects(const StoredObjects & objects, bool if_exists, bool with_size) const
{
    StoredObjects rados_objects;
    for (const auto & object : objects)
    {
        auto get_attribute_result = io_impl->getAttributeIfExists(object.remote_path, RadosStriper::XATTR_OBJECT_COUNT);
        if (!if_exists && !get_attribute_result.object_exists)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Object {} does not exist", object.remote_path);
        rados_objects.push_back(object);
        if (!get_attribute_result.value)
            continue;

        /// Need to recalculate the size of the objects
        if (with_size)
            io_impl->stat(rados_objects.back().remote_path, &rados_objects.back().bytes_size, nullptr);
        auto stripe_count = std::stoull(*get_attribute_result.value);
        for (size_t i = 1; i < stripe_count; ++i)
        {
            rados_objects.emplace_back();
            rados_objects.back().remote_path = RadosStriper::getStripedObjectName(object.remote_path, i);
            if (with_size)
                io_impl->stat(rados_objects.back().remote_path, &rados_objects.back().bytes_size, nullptr);
        }
    }
    return rados_objects;
}

}

#endif
