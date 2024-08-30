#include "RadosObjectStorage.h"
#include <cstring>
#include <ctime>
#include <memory>
#include <string>
#include <fcntl.h>

#if USE_CEPH

#include <Core/Defines.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/ObjectStorages/Ceph/RadosUtils.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/ObjectStorages/ObjectStorageIteratorAsync.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <IO/Ceph/RadosIOContext.h>
#include <IO/ReadBufferFromRados.h>
#include <IO/S3Common.h>
#include <IO/WriteBufferFromRados.h>
#include <IO/WriteSettings.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/MultiVersion.h>
#include <Common/ObjectStorageKey.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Poco/Error.h>

namespace CurrentMetrics
{
    extern const Metric ObjectStorageRadosThreads;
    extern const Metric ObjectStorageRadosThreadsActive;
    extern const Metric ObjectStorageRadosThreadsScheduled;
}


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

namespace
{

class RadosIteratorAsync final : public IObjectStorageIteratorAsync
{
public:
    RadosIteratorAsync(
        std::shared_ptr<const RadosObjectStorage> object_storage_,
        RadosIterator begin_,
        RadosIterator end_,
        const String & prefix_,
        size_t max_list_size_)
        : IObjectStorageIteratorAsync(
              CurrentMetrics::ObjectStorageRadosThreads,
              CurrentMetrics::ObjectStorageRadosThreadsActive,
              CurrentMetrics::ObjectStorageRadosThreadsScheduled,
              "ListObjectRados")
        , object_storage(std::move(object_storage_))
        , current(begin_)
        , end(end_)
        , prefix(prefix_)
        , max_list_size(max_list_size_ ? max_list_size_ : UINT64_MAX)
    {
    }

    ~RadosIteratorAsync() override = default;

private:
    bool getBatchAndCheckNext(RelativePathsWithMetadata & batch) override
    {
        size_t count = 0;
        while (current != end && count < max_list_size)
        {
            auto object_id = current->get_oid();
            ++current;
            if (RadosStriper::isOrdinaryObject(object_id) && object_id.starts_with(prefix))
            {
                batch.emplace_back(std::make_shared<RelativePathWithMetadata>(object_id, object_storage->getObjectMetadata(object_id)));
                ++count;
            }
        }
        return true;
    }

    std::shared_ptr<const RadosObjectStorage> object_storage;
    librados::NObjectIterator current;
    const librados::NObjectIterator end;
    const String prefix;
    size_t max_list_size;
};

}

bool RadosObjectStorage::exists(const StoredObject & object) const
{
    auto get_attribute_result = io_ctx->getAttributeIfExists(object.remote_path, RadosStriper::XATTR_OBJECT_CHUNK_COUNT);
    return get_attribute_result.object_exists;
}

std::unique_ptr<ReadBufferFromFileBase> RadosObjectStorage::readObjects( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    StoredObjects rados_objects_to_read = getRadosObjects(objects, false, true);
    Strings names;
    for (const auto & object : rados_objects_to_read)
        names.push_back(object.remote_path);
    return readObjectsImpl(rados_objects_to_read, read_settings, true);
}

std::unique_ptr<ReadBufferFromFileBase> RadosObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    StoredObjects rados_objects_to_read = getRadosObjects({object}, false, true);
    Strings names;
    for (const auto & obj : rados_objects_to_read)
        names.push_back(obj.remote_path);
    return readObjectsImpl(rados_objects_to_read, read_settings, false);
}

std::unique_ptr<WriteBufferFromFileBase> RadosObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> attributes,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    /// TODO: Support append mode: if the object already exists, continue appending to it (and striping if needed)
    if (mode != WriteMode::Rewrite)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Ceph does not support append mode");

    WriteSettings disk_write_settings = patchSettings(write_settings);
    return std::make_unique<WriteBufferFromRados>(
        io_ctx, object.remote_path, buf_size, ceph_settings.get()->osd_settings, patchSettings(write_settings), attributes);
}

ObjectStorageIteratorPtr RadosObjectStorage::iterate(const std::string & path_prefix, size_t max_keys) const
{
    return std::make_shared<RadosIteratorAsync>(shared_from_this(), io_ctx->begin(), io_ctx->end(), path_prefix, max_keys);
}

void RadosObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
    size_t count = 0;
    if (max_keys == 0)
        max_keys = UINT64_MAX;
    /// TODO: use object_list_slice for scanning in parallel
    auto begin = io_ctx->begin();
    auto end = io_ctx->end();
    for (auto it = begin; it != end && count < max_keys; ++it)
    {
        auto object_id = it->get_oid();
        if (RadosStriper::isOrdinaryObject(object_id) && object_id.starts_with(path))
        {
            children.emplace_back(std::make_shared<RelativePathWithMetadata>(object_id, getObjectMetadata(object_id)));
            ++count;
        }
    }
}

void RadosObjectStorage::removeObjectImpl(const StoredObject & object, bool if_exists)
{
    auto rados_objects = getRadosObjects({object}, if_exists, false);
    auto remove_batch_size = ceph_settings.get()->osd_settings.objecter_inflight_ops;

    /// First, removing non-head objects
    Strings names;
    for (ssize_t i = rados_objects.size() - 1; i > 0; --i)
    {
        names.push_back(std::move(rados_objects[i].remote_path));
        if (names.size() >= remove_batch_size)
        {
            /// Except for the HEAD object, remove only if it exists. Non-head objects can be missing
            /// because of many reasons, e.g. crash on write, exception on remove...
            io_ctx->remove(names);
            names.clear();
        }
    }

    if (!names.empty())
        io_ctx->remove(names);

    /// The head object
    io_ctx->remove(object.remote_path, if_exists);
}

void RadosObjectStorage::removeObjectsImpl(const StoredObjects & objects, bool if_exists)
{
    for (const auto & object : objects)
        removeObjectImpl(object, if_exists);
}

void RadosObjectStorage::removeObject(const StoredObject & object)
{
    removeObjectImpl(object, false);
}

void RadosObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    removeObjectImpl(object, true);
}

void RadosObjectStorage::removeObjects(const StoredObjects & objects)
{
    removeObjectsImpl(objects, false);
}

void RadosObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    removeObjectsImpl(objects, true);
}

std::optional<ObjectMetadata> RadosObjectStorage::tryGetObjectMetadata(const std::string & path) const
{
    auto res = io_ctx->tryGetMetadata(path);
    if (res)
        RadosStriper::patchStriperAtrributes(*res);
    return res;
}

ObjectMetadata RadosObjectStorage::getObjectMetadata(const std::string & path) const
{
    auto res = io_ctx->getMetadata(path);
    RadosStriper::patchStriperAtrributes(res);
    return res;
}

void RadosObjectStorage::copyObject( // NOLINT
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

void RadosObjectStorage::setNewSettings(std::unique_ptr<RadosObjectStorageSettings> && ceph_settings_)
{
    ceph_settings.set(std::move(ceph_settings_));
}

void RadosObjectStorage::shutdown()
{
    io_ctx->close();
    rados->shutdown();
}

void RadosObjectStorage::startup()
{
    rados->connect();
    io_ctx->connect();
}

void RadosObjectStorage::applyNewSettings(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    ContextPtr /*context*/,
    const ApplyNewSettingsOptions & options)
{
    auto modified_settings = std::make_unique<RadosObjectStorageSettings>();
    modified_settings->loadFromConfig(config, config_prefix);

    auto current_settings = ceph_settings.get();
    if (options.allow_client_change && for_disk_ceph)
    {
        auto new_rados = std::make_shared<librados::Rados>();
        new_rados->init(modified_settings->global_options.user.c_str());
        for (const auto & [key, value] : modified_settings->global_options)
        {
            if (auto ec = new_rados->conf_set(key.c_str(), value.c_str()); ec < 0)
                throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Failed to set Ceph option: {}. Error: {}", key, Poco::Error::getMessage(-ec));
        }
        auto new_io_ctx = std::make_shared<RadosIOContext>(new_rados, endpoint.pool, endpoint.nspace);
        std::atomic_store(&rados, new_rados);
        std::atomic_store(&io_ctx, new_io_ctx);
    }
    ceph_settings.set(std::move(modified_settings));
}

WriteSettings RadosObjectStorage::patchSettings(const WriteSettings & write_settings) const
{
    auto modified_settings = write_settings;
    /// Ceph does not support cache on write operations, because a ClickHouse object can be written to multiple Ceph objects.
    modified_settings.enable_filesystem_cache_on_write_operations = false;
    return modified_settings;
}

std::unique_ptr<IObjectStorage> RadosObjectStorage::cloneObjectStorage(
    const std::string & new_namespace,
    const Poco::Util::AbstractConfiguration & /*config*/,
    const std::string & /*config_prefix*/,
    ContextPtr /*context*/)
{
    auto new_ceph_settings = std::make_unique<RadosObjectStorageSettings>(*ceph_settings.get());
    RadosEndpoint new_endpoint;
    new_endpoint.mon_hosts = endpoint.mon_hosts;
    new_endpoint.pool = new_namespace;

    return std::make_unique<RadosObjectStorage>(rados, std::move(new_ceph_settings), new_endpoint, disk_name, for_disk_ceph);
}

ObjectStorageKey RadosObjectStorage::generateObjectKeyForPath(const std::string & /*path*/, const std::optional<std::string> & /*key_prefix*/) const
{
    constexpr size_t key_name_total_size = 32;
    return ObjectStorageKey::createAsRelative(endpoint.path, getRandomASCIIString(key_name_total_size));
}

std::unique_ptr<ReadBufferFromFileBase> RadosObjectStorage::readObjectsImpl( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    bool with_cache) const
{
    ReadSettings disk_read_settings = IObjectStorage::patchSettings(read_settings);

    if (!with_cache)
    {
        disk_read_settings.enable_filesystem_cache = false;
        disk_read_settings.use_page_cache_for_disks_without_file_cache = false;
    }

    auto global_context = Context::getGlobalContextInstance();

    auto read_buffer_creator = [this, disk_read_settings](bool restricted_seek, const StoredObject & object_) -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<ReadBufferFromRados>(
            io_ctx,
            object_.remote_path,
            disk_read_settings,
            /* use_external_buffer */ true,
            /* offset */ 0,
            /* read_until_position */ 0,
            /* restricted_seek */ restricted_seek);
    };

    return std::make_unique<ReadBufferFromRemoteFSGather>(
        std::move(read_buffer_creator),
        objects,
        "rados:" + endpoint.pool + "/",
        disk_read_settings,
        global_context->getFilesystemCacheLog(),
        /* use_external_buffer */ false);

    /// TODO: support remote_fs_method = 'threadpool'

    // switch (read_settings.remote_fs_method)
    // {
    //     case RemoteFSReadMethod::read:
    //     {
    //         return std::make_unique<ReadBufferFromRemoteFSGather>(
    //             std::move(read_buffer_creator),
    //             objects,
    //             "rados:" + endpoint.pool + "/",
    //             disk_read_settings,
    //             global_context->getFilesystemCacheLog(),
    //             /* use_external_buffer */false);
    //     }
    //     case RemoteFSReadMethod::threadpool:
    //     {
    //         auto impl = std::make_unique<ReadBufferFromRemoteFSGather>(
    //             std::move(read_buffer_creator),
    //             objects,
    //             "rados:" + endpoint.pool + "/",
    //             disk_read_settings,
    //             global_context->getFilesystemCacheLog(),
    //             /* use_external_buffer */true);

    //         auto & reader = global_context->getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
    //         return std::make_unique<AsynchronousBoundedReadBuffer>(
    //             std::move(impl), reader, disk_read_settings,
    //             global_context->getAsyncReadCounters(),
    //             global_context->getFilesystemReadPrefetchesLog());
    //     }
    // }
}

StoredObjects RadosObjectStorage::getRadosObjects(const StoredObjects & objects, bool if_exists, bool with_size) const
{
    StoredObjects rados_objects;
    for (const auto & object : objects)
    {
        auto get_attribute_result = io_ctx->getAttributeIfExists(object.remote_path, RadosStriper::XATTR_OBJECT_CHUNK_COUNT);
        if (!if_exists && !get_attribute_result.object_exists)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Object {} does not exist", object.remote_path);

        rados_objects.push_back(object);

        if (with_size)
            io_ctx->stat(rados_objects.back().remote_path, &rados_objects.back().bytes_size, nullptr);

        /// Need to recalculate the size of the objects
        if (!get_attribute_result.value)
            continue;

        auto stripe_count = std::stoull(*get_attribute_result.value);
        for (size_t i = 1; i < stripe_count; ++i)
        {
            rados_objects.emplace_back();
            rados_objects.back().remote_path = RadosStriper::getChunkName(object.remote_path, i);
            if (with_size)
                io_ctx->stat(rados_objects.back().remote_path, &rados_objects.back().bytes_size, nullptr);
        }
    }
    return rados_objects;
}

}

#endif
