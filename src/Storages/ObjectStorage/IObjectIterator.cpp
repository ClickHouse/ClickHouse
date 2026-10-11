#include <Storages/VirtualColumnUtils.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatFilterInfo.h>
#include <Interpreters/Context.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Core/Settings.h>
#include <Core/Defines.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Processors/Formats/IInputFormat.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include "config.h"

#if USE_PARQUET
#include <IO/CompressionMethod.h>
#include <IO/SharedThreadPools.h>
#include <Processors/Formats/Impl/ParquetV3BlockInputFormat.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <base/scope_guard.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/noncopyable.hpp>
#include <condition_variable>
#include <deque>
#include <mutex>
#endif

namespace ProfileEvents
{
    extern const Event ObjectStorageListedObjects;
    extern const Event ObjectStoragePredicateFilteredObjects;
    extern const Event ParquetFooterPrefetchRequests;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 cluster_table_function_buckets_batch_size;
    extern const SettingsBool use_cache_for_count_from_files;
    extern const SettingsBool use_parquet_metadata_cache;
    extern const SettingsBool use_query_condition_cache_for_top_k;
}

static ExpressionActionsPtr getExpressionActions(
    const DB::ActionsDAG & filter_,
    const NamesAndTypesList & virtual_columns_,
    const ContextPtr & context_)
{
    auto filter = VirtualColumnUtils::createPathAndFileFilterDAG(filter_.getOutputs().at(0), virtual_columns_, context_);
    if (filter.has_value())
    {
        VirtualColumnUtils::buildSetsForDAG(*filter, context_);
        return std::make_shared<ExpressionActions>(std::move(*filter), ExpressionActionsSettings(context_));
    }
    return nullptr;
}

ObjectIteratorWithPathAndFileFilter::ObjectIteratorWithPathAndFileFilter(
    ObjectIterator iterator_,
    const DB::ActionsDAG & filter_,
    const NamesAndTypesList & virtual_columns_,
    const NamesAndTypesList & hive_partition_columns_,
    StorageObjectStorageConfigurationPtr configuration_,
    const ContextPtr & context_,
    std::function<void(FileProgress)> file_progress_callback_)
    : WithContext(context_)
    , iterator(iterator_)
    , configuration(std::move(configuration_))
    , virtual_columns(virtual_columns_)
    , hive_partition_columns(hive_partition_columns_)
    , filter_actions(getExpressionActions(filter_, virtual_columns, context_))
    , file_progress_callback(std::move(file_progress_callback_))
{
}

ObjectInfoPtr ObjectIteratorWithPathAndFileFilter::next(size_t id)
{
    while (true)
    {
        auto object = iterator->next(id);
        if (!object)
            break;

        if (emit_profile_events)
            ProfileEvents::increment(ProfileEvents::ObjectStorageListedObjects);

        if (filter_actions)
        {
            const auto key = object->getPath();
            std::vector<std::string> keys({key});

            /// Must be the formatter the `_path` column is produced with: this filter is
            /// evaluated against that column's values.
            const auto path = formatObjectPath(*configuration, key, /*include_connection_info=*/false);

            VirtualColumnUtils::filterByPathOrFile(
                keys, std::vector<std::string>{path}, filter_actions,
                virtual_columns, hive_partition_columns, getContext());

            if (keys.empty())
            {
                if (emit_profile_events)
                    ProfileEvents::increment(ProfileEvents::ObjectStoragePredicateFilteredObjects);
                LOG_TRACE(log, "Filtered out object: {}", object->getPath());
                continue;
            }
        }

        if (file_progress_callback)
        {
            if (auto metadata = object->getObjectMetadata())
                file_progress_callback(FileProgress(0, metadata->size_bytes));
            else if (auto hint = object->getFileSizeHint())
                file_progress_callback(FileProgress(0, *hint));
        }

        return object;
    }
    return {};
}

std::string ObjectInfo::getPathOrPathToArchiveIfArchive() const
{
    if (isArchive())
        return getPathToArchive();
    return getPath();
}


ObjectIteratorSplitByBuckets::ObjectIteratorSplitByBuckets(
    ObjectIterator iterator_,
    const String & format_,
    ObjectStoragePtr object_storage_,
    const ContextPtr & context_,
    const StorageID & storage_id_,
    FormatFilterInfoPtr format_filter_info_)
    : WithContext(context_)
    , iterator(iterator_)
    , format(format_)
    , object_storage(object_storage_)
    , format_settings(getFormatSettings(context_))
    , storage_id(storage_id_)
    , format_filter_info(std::move(format_filter_info_))
{
    if (format_filter_info && format_filter_info->condition_hash)
        query_condition_cache = context_->getQueryConditionCache();
}

ObjectInfoPtr ObjectIteratorSplitByBuckets::next(size_t id)
{
    while (pending_objects_info.empty())
    {
        auto last_object_info = iterator->next(id);
        if (!last_object_info)
            return {};

        auto splitter = FormatFactory::instance().getSplitter(format);
        if (splitter)
        {
            std::vector<size_t> matching_row_groups;
            bool has_cache_entry = false;
            if (query_condition_cache)
            {
                const auto query_condition_cache_key = last_object_info->getIdentifier(/*include_file_bucket_info=*/ false);
                auto matching_marks = query_condition_cache->read(
                    storage_id.uuid,
                    query_condition_cache_key,
                    *format_filter_info->condition_hash);
                if (matching_marks.has_value())
                {
                    has_cache_entry = true;
                    const auto & marks = *matching_marks;
                    for (size_t i = 0; i < marks.size(); ++i)
                        if (marks[i])
                            matching_row_groups.push_back(i);
                    if (matching_row_groups.empty())
                        continue;
                }
            }

            auto buffer = createReadBuffer(last_object_info->relative_path_with_metadata, object_storage, getContext(), log);
            size_t bucket_size = getContext()->getSettingsRef()[Setting::cluster_table_function_buckets_batch_size];
            auto file_bucket_infos = splitter->splitToBuckets(bucket_size, *buffer, format_settings);
            for (const auto & file_bucket : file_bucket_infos)
            {
                auto copy_object_info = *last_object_info;
                if (has_cache_entry)
                {
                    auto filtered = file_bucket->filterByMatchingRowGroups(matching_row_groups);
                    if (!filtered)
                        continue;
                    copy_object_info.file_bucket_info = std::move(filtered);
                }
                else
                {
                    copy_object_info.file_bucket_info = file_bucket;
                }
                pending_objects_info.push(std::make_shared<ObjectInfo>(copy_object_info));
            }
        }
    }

    auto result = pending_objects_info.front();
    pending_objects_info.pop();
    return result;
}

String ObjectInfo::getIdentifier(bool include_file_bucket_info) const
{
    return getIdentifierForPath(getPath(), include_file_bucket_info);
}

String ObjectInfo::getIdentifierForPath(const String & path, bool include_file_bucket_info) const
{
    String result = path;
    if (relative_path_with_metadata.read_source_index)
        result = std::to_string(*relative_path_with_metadata.read_source_index) + ":" + result;
    if (include_file_bucket_info && file_bucket_info)
        result += file_bucket_info->getIdentifier();
    return result;
}

#if USE_PARQUET
namespace
{

class ObjectIteratorWithParquetFooterPrefetch final : public IObjectIterator
{
public:
    ObjectIteratorWithParquetFooterPrefetch(
        ObjectIterator inner_,
        ObjectStoragePtr object_storage_,
        StorageObjectStorageConfigurationPtr configuration_,
        StorageID storage_id_,
        std::optional<FormatSettings> format_settings_,
        FormatFilterInfoPtr format_filter_info_,
        ContextPtr context_,
        bool need_only_count_,
        size_t num_streams)
        : inner(std::move(inner_))
        , configuration(std::move(configuration_))
        , storage_id(std::move(storage_id_))
        , format_settings(std::move(format_settings_))
        , format_filter_info(std::move(format_filter_info_))
        , context(std::move(context_))
        , need_only_count(need_only_count_)
        , window(std::clamp<size_t>(4 * num_streams, 16, 32))
        , ramp_start(2 * num_streams)
        , read_settings(context->getReadSettings())
        , metadata_cache(context->getSettingsRef()[Setting::use_parquet_metadata_cache] ? context->tryGetParquetMetadataCache() : nullptr)
        , schema_cache(StorageObjectStorage::getSchemaCache(context, configuration->getTypeName()))
        , state(std::make_shared<State>())
    {
        emit_profile_events = inner->emit_profile_events;
        read_settings.remote_fs_settings.random_access = true;
        if (format_filter_info && format_filter_info->condition_hash)
            query_condition_cache = Context::getGlobalContextInstance()->getQueryConditionCache();
        resources = std::make_shared<const Resources>(Resources{
            std::move(object_storage_),
            context,
            read_settings,
            format_settings ? *format_settings : getFormatSettings(context),
            metadata_cache,
            CurrentThread::getGroup()});
    }

    ~ObjectIteratorWithParquetFooterPrefetch() override
    {
        std::unique_lock lock(state->mutex);
        state->cancelled = true;
        state->no_running_requests.wait(lock, [this] { return state->running == 0; });
    }

    ObjectInfoPtr next(size_t processor) override
    {
        std::lock_guard lock(mutex);
        const size_t depth = readAheadDepth();
        while (!inner_exhausted && ahead.size() <= depth)
        {
            auto object_info = inner->next(processor);
            if (!object_info)
            {
                inner_exhausted = true;
                break;
            }
            ahead.push_back(std::move(object_info));
        }

        if (ahead.empty())
            return {};
        auto object_info = std::move(ahead.front());
        ahead.pop_front();
        if (num_prefetched_ahead > 0)
            --num_prefetched_ahead;
        ++num_handed_out;

        while (num_prefetched_ahead < ahead.size() && prefetchFooter(ahead[num_prefetched_ahead]))
            ++num_prefetched_ahead;
        return object_info;
    }

    size_t estimatedKeysCount() override { return inner->estimatedKeysCount(); }
    std::optional<UInt64> getSnapshotVersion() const override { return inner->getSnapshotVersion(); }

    void setEmitProfileEvents(bool value) override
    {
        emit_profile_events = value;
        inner->setEmitProfileEvents(value);
    }

private:
    struct State
    {
        std::mutex mutex;
        std::condition_variable no_running_requests;
        size_t in_flight = 0;
        size_t running = 0;
        bool cancelled = false;
    };

    /// One of the `window` places for a read in flight, given back by the destructor.
    class Slot : private boost::noncopyable
    {
    public:
        explicit Slot(std::shared_ptr<State> state_) : state(std::move(state_)) {}

        ~Slot()
        {
            if (!acquired)
                return;
            std::lock_guard lock(state->mutex);
            --state->in_flight;
        }

        static std::unique_ptr<Slot> tryAcquire(const std::shared_ptr<State> & state_, size_t limit)
        {
            auto slot = std::make_unique<Slot>(state_);
            std::lock_guard lock(state_->mutex);
            if (state_->in_flight >= limit)
                return nullptr;
            ++state_->in_flight;
            slot->acquired = true;
            return slot;
        }

    private:
        const std::shared_ptr<State> state;
        bool acquired = false;
    };

    /// What a footer read needs from the query, owned by the iterator.
    struct Resources
    {
        ObjectStoragePtr object_storage;
        ContextPtr context;
        ReadSettings read_settings;
        FormatSettings format_settings;
        ParquetMetadataCachePtr metadata_cache;
        ThreadGroupPtr thread_group;
    };

    struct Request
    {
        std::unique_ptr<Slot> slot;
        std::shared_ptr<State> state;
        /// Not owning: when nobody can open the object any more, its footer is not read.
        std::weak_ptr<ParquetPrefetchedFooter> footer;
        RelativePathWithMetadata object;
        /// Not owning: a request that has not started when the iterator is destroyed holds nothing of the query.
        std::weak_ptr<const Resources> resources;
    };

    /// Nothing is read ahead while the sources take their first objects, so a query that stops early reads no extra footers.
    size_t readAheadDepth() const
    {
        if (num_handed_out < ramp_start)
            return 0;
        return std::min(window, 4 * (num_handed_out - ramp_start + 1));
    }

    bool needsFooter(const ObjectInfoPtr & object_info) const
    {
        if (object_info->isArchive() || object_info->getPath().empty() || object_info->file_bucket_info)
            return false;

        const auto & metadata = object_info->relative_path_with_metadata.metadata;
        if (!metadata || !metadata->is_size_known || !metadata->isEtagUsableAsCacheKey())
            return false;

        /// A smaller object is read whole by `createReadBuffer`.
        if (metadata->size_bytes <= read_settings.remote_fs_settings.buffer_size)
            return false;

        if (!boost::iequals(object_info->getFileFormat().value_or(configuration->format), "Parquet"))
            return false;

        if (chooseCompressionMethod(object_info->getFileName(), configuration->compression_method) != CompressionMethod::None)
            return false;

        if (metadata_cache
            && metadata_cache->contains(ParquetMetadataCache::createKey(object_info->relative_path_with_metadata.getPath(), metadata->etag)))
            return false;

        if (need_only_count && context->getSettingsRef()[Setting::use_cache_for_count_from_files])
        {
            auto key = StorageObjectStorageSource::getKeyForNumRowsCache(*configuration, *object_info, format_settings, context);
            auto get_last_mod_time = [&] { return StorageObjectStorageSource::getLastModificationTime(*object_info); };
            if (key && schema_cache.hasValidNumRows(*key, get_last_mod_time))
                return false;
        }

        if (query_condition_cache
            && (context->getSettingsRef()[Setting::use_query_condition_cache_for_top_k]
                || !StorageObjectStorageSource::isTopKFilterAllowed(*configuration, format_filter_info.get(), object_info, context)))
        {
            auto key = StorageObjectStorageSource::makeQueryConditionCacheKey(*object_info, configuration->isDataLakeConfiguration());
            if (key && query_condition_cache->contains(storage_id.uuid, *key, *format_filter_info->condition_hash))
                return false;
        }

        return true;
    }

    /// Returns false if the read cannot be scheduled now, so that the object is tried again on the next call.
    bool prefetchFooter(const ObjectInfoPtr & object_info)
    {
        if (!needsFooter(object_info))
            return true;

        if (getIOThreadPool().get().getMaxThreads() == 0)
            return false;

        auto slot = Slot::tryAcquire(state, window);
        if (!slot)
            return false;

        auto footer = std::make_shared<ParquetPrefetchedFooter>();
        auto request = std::make_shared<Request>(
            Request{std::move(slot), state, footer, object_info->relative_path_with_metadata, resources});

        if (!getIOThreadPool().get().trySchedule([request] { runRequest(*request); }))
            return false;

        ProfileEvents::increment(ProfileEvents::ParquetFooterPrefetchRequests);
        object_info->prefetched_footer = std::move(footer);
        return true;
    }

    static void runRequest(Request & request)
    {
        {
            std::lock_guard lock(request.state->mutex);
            if (request.state->cancelled)
                return;
            ++request.state->running;
        }
        SCOPE_EXIT({
            std::lock_guard lock(request.state->mutex);
            --request.state->running;
            request.state->no_running_requests.notify_all();
        });
        readFooter(request);
    }

    /// The references it takes are released before `running` is decremented, so none outlives the iterator.
    static void readFooter(Request & request)
    {
        auto resources = request.resources.lock();
        auto footer = request.footer.lock();
        if (!resources || !footer)
            return;

        try
        {
            Exception::SuppressErrorCodesScope suppress_error_codes;
            ThreadGroupSwitcher switcher(resources->thread_group, ThreadName::PARQUET_PREFETCH);
            auto buf = createReadBuffer(
                request.object, resources->object_storage, resources->context, getLogger("ParquetFooterPrefetch"),
                resources->read_settings);
            if (!isBufferWithFileSize(*buf) || !(buf->supportsReadAt() || buf->checkIfActuallySeekable()))
                return;
            if (!footer->tryStart())
                return;
            footer->finish(readParquetFileMetaDataAhead(*buf, resources->format_settings, resources->metadata_cache, request.object));
        }
        catch (...)
        {
            footer->fail(std::current_exception());
        }
    }

    const ObjectIterator inner;
    const StorageObjectStorageConfigurationPtr configuration;
    const StorageID storage_id;
    const std::optional<FormatSettings> format_settings;
    const FormatFilterInfoPtr format_filter_info;
    const ContextPtr context;
    const bool need_only_count;
    const size_t window;
    const size_t ramp_start;
    ReadSettings read_settings;
    const ParquetMetadataCachePtr metadata_cache;
    SchemaCache & schema_cache;
    QueryConditionCachePtr query_condition_cache;

    std::mutex mutex;
    /// Objects taken from `inner` and not handed out yet. The first `num_prefetched_ahead` of them have their footer read
    /// scheduled or need none.
    std::deque<ObjectInfoPtr> ahead;
    size_t num_prefetched_ahead = 0;
    size_t num_handed_out = 0;
    bool inner_exhausted = false;

    std::shared_ptr<const Resources> resources;
    const std::shared_ptr<State> state;
};

}

ObjectIterator createIteratorWithParquetFooterPrefetch(
    ObjectIterator inner,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    StorageID storage_id,
    std::optional<FormatSettings> format_settings,
    FormatFilterInfoPtr format_filter_info,
    ContextPtr context,
    bool need_only_count,
    size_t num_streams)
{
    return std::make_shared<ObjectIteratorWithParquetFooterPrefetch>(
        std::move(inner),
        std::move(object_storage),
        std::move(configuration),
        std::move(storage_id),
        std::move(format_settings),
        std::move(format_filter_info),
        std::move(context),
        need_only_count,
        num_streams);
}
#else
ObjectIterator createIteratorWithParquetFooterPrefetch(
    ObjectIterator inner,
    ObjectStoragePtr,
    StorageObjectStorageConfigurationPtr,
    StorageID,
    std::optional<FormatSettings>,
    FormatFilterInfoPtr,
    ContextPtr,
    bool,
    size_t)
{
    return inner;
}
#endif

}
