#include <Compression/CompressedReadBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <Common/CurrentMetrics.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/ThreadPool.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>

#include <utility>

namespace ProfileEvents
{
    extern const Event WaitMarksLoadMicroseconds;
    extern const Event BackgroundLoadingMarksTasks;
    extern const Event LoadedMarksCount;
    extern const Event LoadedMarksMemoryBytes;
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
    MergeTreeDataPartInfoForReaderPtr data_part_reader_,
    MarkCache * mark_cache_,
    const String & mrk_path_,
    size_t marks_count_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    bool save_marks_in_cache_,
    const ReadSettings & read_settings_,
    ThreadPool * load_marks_threadpool_,
    size_t columns_in_mark_)
    : data_part_reader(data_part_reader_)
    , mark_cache(mark_cache_)
    , mrk_path(mrk_path_)
    , marks_count(marks_count_)
    , index_granularity_info(index_granularity_info_)
    , save_marks_in_cache(save_marks_in_cache_)
    , columns_in_mark(columns_in_mark_)
    , read_settings(read_settings_)
    , load_marks_threadpool(load_marks_threadpool_)
{
    if (load_marks_threadpool)
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


MarkInCompressedFile MergeTreeMarksLoader::getMark(size_t row_index, size_t column_index)
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

    return marks->get(row_index * columns_in_mark + column_index);
}


MarkCache::MappedPtr MergeTreeMarksLoader::loadMarksImpl()
{
    /// Memory for marks must not be accounted as memory usage for query, because they are stored in shared cache.
    MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

    auto data_part_storage = data_part_reader->getDataPartStorage();

    size_t file_size = data_part_storage->getFileSize(mrk_path);
    size_t mark_size = index_granularity_info.getMarkSizeInBytes(columns_in_mark);
    size_t expected_uncompressed_size = mark_size * marks_count;

    // We first read the marks into a temporary simple array, then compress them into a more compact
    // representation.
    PODArray<MarkInCompressedFile> plain_marks(marks_count * columns_in_mark); // temporary

    if (file_size == 0 && marks_count != 0)
    {
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Empty marks file '{}': {}, must be: {}",
            std::string(fs::path(data_part_storage->getFullPath()) / mrk_path),
            file_size, expected_uncompressed_size);
    }

    if (!index_granularity_info.mark_type.compressed && expected_uncompressed_size != file_size)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Bad size of marks file '{}': {}, must be: {}",
            std::string(fs::path(data_part_storage->getFullPath()) / mrk_path),
            file_size,
            expected_uncompressed_size);

    auto buffer = data_part_storage->readFile(mrk_path, read_settings.adjustBufferSize(file_size), file_size, std::nullopt);
    std::unique_ptr<ReadBuffer> reader;
    if (!index_granularity_info.mark_type.compressed)
        reader = std::move(buffer);
    else
        reader = std::make_unique<CompressedReadBufferFromFile>(std::move(buffer));

    if (!index_granularity_info.mark_type.adaptive)
    {
        /// Read directly to marks.
        chassert(expected_uncompressed_size == plain_marks.size() * sizeof(MarkInCompressedFile));
        reader->readStrict(reinterpret_cast<char *>(plain_marks.data()), expected_uncompressed_size);

        if (!reader->eof())
            throw Exception(
                ErrorCodes::CANNOT_READ_ALL_DATA,
                "Cannot read all marks from file {}, is eof: {}, buffer size: {}, file size: {}",
                mrk_path,
                reader->eof(),
                reader->buffer().size(),
                file_size);
    }
    else
    {
        for (size_t i = 0; i < marks_count; ++i)
        {
            if (reader->eof())
                throw Exception(
                    ErrorCodes::CANNOT_READ_ALL_DATA,
                    "Cannot read all marks from file {}, marks expected {} (bytes size {}), marks read {} (bytes size {})",
                    mrk_path, marks_count, expected_uncompressed_size, i, reader->count());

            size_t granularity;
            reader->readStrict(
                reinterpret_cast<char *>(plain_marks.data() + i * columns_in_mark), columns_in_mark * sizeof(MarkInCompressedFile));
            readIntBinary(granularity, *reader);
        }

        if (!reader->eof())
            throw Exception(
                ErrorCodes::CANNOT_READ_ALL_DATA,
                "Too many marks in file {}, marks expected {} (bytes size {})",
                mrk_path, marks_count, expected_uncompressed_size);
    }

    auto res = std::make_shared<MarksInCompressedFile>(plain_marks);

    ProfileEvents::increment(ProfileEvents::LoadedMarksCount, marks_count * columns_in_mark);
    ProfileEvents::increment(ProfileEvents::LoadedMarksMemoryBytes, res->approximateMemoryUsage());

    return res;
}

MarkCache::MappedPtr MergeTreeMarksLoader::loadMarks()
{
    MarkCache::MappedPtr loaded_marks;

    auto data_part_storage = data_part_reader->getDataPartStorage();

    if (mark_cache)
    {
        auto key = mark_cache->hash(fs::path(data_part_storage->getFullPath()) / mrk_path);
        if (save_marks_in_cache)
        {
            auto callback = [this] { return loadMarksImpl(); };
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
            ErrorCodes::LOGICAL_ERROR, "Failed to load marks: {}", (fs::path(data_part_storage->getFullPath()) / mrk_path).string());
    }

    return loaded_marks;
}

std::future<MarkCache::MappedPtr> MergeTreeMarksLoader::loadMarksAsync()
{
    return scheduleFromThreadPool<MarkCache::MappedPtr>(
        [this]() -> MarkCache::MappedPtr
        {
            ProfileEvents::increment(ProfileEvents::BackgroundLoadingMarksTasks);
            return loadMarks();
        },
        *load_marks_threadpool,
        "LoadMarksThread");
}

}
