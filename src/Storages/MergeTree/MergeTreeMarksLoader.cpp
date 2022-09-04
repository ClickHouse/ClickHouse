#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <IO/ReadBufferFromFile.h>
#include <Common/setThreadName.h>
#include <Common/scope_guard_safe.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>

#include <utility>

namespace ProfileEvents
{
    extern const Event WaitMarksLoadMicroseconds;
    extern const Event BackgroundLoadingMarksTasks;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

MergeTreeMarksLoader::MergeTreeMarksLoader(
    DataPartStoragePtr data_part_storage_,
    MarkCache * mark_cache_,
    const String & mrk_path_,
    size_t marks_count_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    bool save_marks_in_cache_,
    const ReadSettings & read_settings_,
    size_t columns_in_mark_)
    : data_part_storage(std::move(data_part_storage_))
    , mark_cache(mark_cache_)
    , mrk_path(mrk_path_)
    , marks_count(marks_count_)
    , index_granularity_info(index_granularity_info_)
    , save_marks_in_cache(save_marks_in_cache_)
    , columns_in_mark(columns_in_mark_)
    , read_settings(read_settings_)
{
    if (read_settings_.load_marks_asynchronously)
    {
        future = loadMarksAsync();
    }
}

MergeTreeMarksLoader::~MergeTreeMarksLoader()
{
    if (future.valid())
    {
        future.wait();
    }
}

std::shared_ptr<ThreadPool> MergeTreeMarksLoader::getLoadMarksThreadpool()
{
    constexpr size_t pool_size = 50;
    constexpr size_t queue_size = 1000000;
    static std::shared_ptr<ThreadPool> load_marks_threadpool = std::make_shared<ThreadPool>(pool_size, pool_size, queue_size);

    return load_marks_threadpool;
}


const MarkInCompressedFile & MergeTreeMarksLoader::getMark(size_t row_index, size_t column_index)
{
    if (!marks)
    {
        Stopwatch watch(CLOCK_MONOTONIC);

        if (future.valid())
        {
            marks = future.get();
            future = {};
        }
        else
        {
            marks = loadMarks();
        }

        watch.stop();
        ProfileEvents::increment(ProfileEvents::WaitMarksLoadMicroseconds, watch.elapsedMicroseconds());
    }

#ifndef NDEBUG
    if (column_index >= columns_in_mark)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column index: {} is out of range [0, {})", column_index, columns_in_mark);
#endif

    return (*marks)[row_index * columns_in_mark + column_index];
}

MarkCache::MappedPtr MergeTreeMarksLoader::loadMarksImpl()
{
    /// Memory for marks must not be accounted as memory usage for query, because they are stored in shared cache.
    MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

    size_t file_size = data_part_storage->getFileSize(mrk_path);
    size_t mark_size = index_granularity_info.getMarkSizeInBytes(columns_in_mark);
    size_t expected_file_size = mark_size * marks_count;

    if (expected_file_size != file_size)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Bad size of marks file '{}': {}, must be: {}",
            std::string(fs::path(data_part_storage->getFullPath()) / mrk_path),
            std::to_string(file_size), std::to_string(expected_file_size));

    auto res = std::make_shared<MarksInCompressedFile>(marks_count * columns_in_mark);

    if (!index_granularity_info.is_adaptive)
    {
        /// Read directly to marks.
        auto buffer = data_part_storage->readFile(mrk_path, read_settings.adjustBufferSize(file_size), file_size, std::nullopt);
        buffer->readStrict(reinterpret_cast<char *>(res->data()), file_size);

        if (!buffer->eof())
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "Cannot read all marks from file {}, is eof: {}, buffer size: {}, file size: {}",
                mrk_path, buffer->eof(), buffer->buffer().size(), file_size);
    }
    else
    {
        auto buffer = data_part_storage->readFile(mrk_path, read_settings.adjustBufferSize(file_size), file_size, std::nullopt);
        size_t i = 0;
        while (!buffer->eof())
        {
            res->read(*buffer, i * columns_in_mark, columns_in_mark);
            buffer->seek(sizeof(size_t), SEEK_CUR);
            ++i;
        }

        if (i * mark_size != file_size)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read all marks from file {}", mrk_path);
    }
    res->protect();
    return res;
}

MarkCache::MappedPtr MergeTreeMarksLoader::loadMarks()
{
    MarkCache::MappedPtr loaded_marks;

    if (mark_cache)
    {
        auto key = mark_cache->hash(fs::path(data_part_storage->getFullPath()) / mrk_path);
        if (save_marks_in_cache)
        {
            auto callback = [this]{ return loadMarksImpl(); };
            loaded_marks = mark_cache->getOrSet(key, callback);
        }
        else
        {
            loaded_marks = mark_cache->get(key);
            if (!loaded_marks)
                loaded_marks = loadMarksImpl();
        }
    }
    else
        loaded_marks = loadMarksImpl();

    if (!loaded_marks)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Failed to load marks: {}",
            (fs::path(data_part_storage->getFullPath()) / mrk_path).string());
    }

    return loaded_marks;
}

std::future<MarkCache::MappedPtr> MergeTreeMarksLoader::loadMarksAsync()
{
    ThreadGroupStatusPtr thread_group;
    if (CurrentThread::isInitialized() && CurrentThread::get().getThreadGroup())
        thread_group = CurrentThread::get().getThreadGroup();

    auto task = std::make_shared<std::packaged_task<MarkCache::MappedPtr()>>([thread_group, this]
    {
        setThreadName("loadMarksThread");

        if (thread_group)
             CurrentThread::attachTo(thread_group);

        SCOPE_EXIT_SAFE({
           if (thread_group)
               CurrentThread::detachQuery();
        });

        ProfileEvents::increment(ProfileEvents::BackgroundLoadingMarksTasks);
        return loadMarks();
    });

    auto task_future = task->get_future();
    getLoadMarksThreadpool()->scheduleOrThrow([task]{ (*task)(); });
    return task_future;
}

}
