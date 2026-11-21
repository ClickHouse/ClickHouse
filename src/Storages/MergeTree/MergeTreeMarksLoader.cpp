#include <Compression/CompressedReadBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/CurrentMetrics.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/ThreadPool.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/setThreadName.h>

#include <utility>

namespace ProfileEvents
{
    extern const Event WaitMarksLoadMicroseconds;
    extern const Event BackgroundLoadingMarksTasks;
    extern const Event LoadingMarksTasksCanceled;
    extern const Event MarksTasksFromCache;
    extern const Event LoadedMarksFiles;
    extern const Event LoadedMarksCount;
    extern const Event LoadedMarksMemoryBytes;
}

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsString columns_to_prewarm_mark_cache;
}

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int ASYNC_LOAD_CANCELED;
}

MergeTreeMarksGetter::MergeTreeMarksGetter(MarkCache::MappedPtr marks_, size_t num_columns_in_mark_)
    : marks(std::move(marks_)), num_columns_in_mark(num_columns_in_mark_)
{
    if (!marks)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Null marks passed to MergeTreeMarksGetter");

    if (num_columns_in_mark == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of columns in mark is zero");

    if (marks->getNumberOfMarks() % num_columns_in_mark != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Number of marks {} is not divisible by number of columns in mark {}",
            marks->getNumberOfMarks(), num_columns_in_mark);
}

MarkInCompressedFile MergeTreeMarksGetter::getMark(size_t row_index, size_t column_index) const
{
    if (column_index >= num_columns_in_mark)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Column index {} is out of range [0, {})", column_index, num_columns_in_mark);

    return marks->get(row_index * num_columns_in_mark + column_index);
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
    size_t num_columns_in_mark_,
    bool use_streaming_compression_)
    : data_part_reader(data_part_reader_)
    , mark_cache(mark_cache_)
    , mrk_path(mrk_path_)
    , marks_count(marks_count_)
    , index_granularity_info(index_granularity_info_)
    , save_marks_in_cache(save_marks_in_cache_)
    , read_settings(read_settings_)
    , num_columns_in_mark(num_columns_in_mark_)
    , use_streaming_compression(use_streaming_compression_)
    , load_marks_threadpool(load_marks_threadpool_)
{
}

void MergeTreeMarksLoader::startAsyncLoad()
{
    if (load_marks_threadpool)
        future = loadMarksAsync();
}

MergeTreeMarksLoader::~MergeTreeMarksLoader()
{
    if (future.valid())
    {
        is_canceled = true;
        future.wait();
    }
}

MergeTreeMarksGetterPtr MergeTreeMarksLoader::loadMarks()
{
    OpenTelemetry::SpanHolder span("MergeTreeMarksLoader::loadMarks");

    auto component_guard = Coordination::setCurrentComponent("MergeTreeMarksLoader::loadMarks");

    std::lock_guard lock(load_mutex);

    if (marks)
        return std::make_unique<MergeTreeMarksGetter>(marks, num_columns_in_mark);

    Stopwatch watch(CLOCK_MONOTONIC);

    if (future.valid())
    {
        marks = future.get();
        future = {};
    }
    else
    {
        marks = loadMarksSync();
    }

    watch.stop();
    ProfileEvents::increment(ProfileEvents::WaitMarksLoadMicroseconds, watch.elapsedMicroseconds());
    return std::make_unique<MergeTreeMarksGetter>(marks, num_columns_in_mark);
}


MarkCache::MappedPtr MergeTreeMarksLoader::loadMarksImpl()
{
    LOG_TEST(getLogger("MergeTreeMarksLoader"), "Loading marks from path {}", mrk_path);

    /// Memory for marks must not be accounted as memory usage for query, because they are stored in shared cache.
    MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

    auto data_part_storage = data_part_reader->getDataPartStorage();

    /// A part with zero granules has nothing to load: return early without opening the file.
    if (marks_count == 0)
    {
        auto res = MarksInCompressedFile::create(PODArray<MarkInCompressedFile>{});
        ProfileEvents::increment(ProfileEvents::LoadedMarksFiles);
        return res;
    }

    size_t file_size = data_part_storage->getFileSize(mrk_path);
    size_t mark_size = index_granularity_info.getMarkSizeInBytes(num_columns_in_mark);
    size_t expected_uncompressed_size = mark_size * marks_count;
    size_t total_marks = marks_count * num_columns_in_mark;

    auto full_mark_path = std::string(fs::path(data_part_storage->getFullPath()) / mrk_path);

    if (file_size == 0)
    {
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Empty marks file '{}': {}, must be: {}",
            full_mark_path,
            file_size, expected_uncompressed_size);
    }

    if (!index_granularity_info.mark_type.compressed && expected_uncompressed_size != file_size)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Bad size of marks file '{}': {}, must be: {}",
            full_mark_path,
            file_size,
            expected_uncompressed_size);

    auto buffer = data_part_storage->readFile(mrk_path, read_settings.adjustBufferSize(file_size), file_size);
    std::unique_ptr<ReadBuffer> reader;
    if (!index_granularity_info.mark_type.compressed)
        reader = std::move(buffer);
    else
        reader = std::make_unique<CompressedReadBufferFromFile>(std::move(buffer));

    /// When streaming, compress marks block-by-block via Builder to avoid
    /// materializing the full plain marks array (can be hundreds of MiB
    /// for compact parts with many substreams).
    /// When not streaming, read all marks into a plain array first,
    /// then compress (the constructor also uses Builder internally).
    std::optional<MarksInCompressedFile::Builder> builder;
    PODArray<MarkInCompressedFile> plain_marks;

    if (use_streaming_compression)
        builder.emplace(total_marks);
    else
        plain_marks.resize(total_marks);

    auto byteSwapMarksIfNeeded = [](MarkInCompressedFile * marks_data, size_t count)
    {
        if constexpr (std::endian::native == std::endian::big)
        {
            for (size_t i = 0; i < count; ++i)
            {
                marks_data[i].offset_in_compressed_file = std::byteswap(marks_data[i].offset_in_compressed_file);
                marks_data[i].offset_in_decompressed_block = std::byteswap(marks_data[i].offset_in_decompressed_block);
            }
        }
    };

    if (!index_granularity_info.mark_type.adaptive)
    {
        chassert(expected_uncompressed_size == total_marks * sizeof(MarkInCompressedFile));

        if (use_streaming_compression)
        {
            PODArray<MarkInCompressedFile> read_buf(MarksInCompressedFile::MARKS_PER_BLOCK);
            size_t marks_read = 0;
            while (marks_read < total_marks)
            {
                size_t count = std::min(MarksInCompressedFile::MARKS_PER_BLOCK, total_marks - marks_read);
                reader->readStrict(reinterpret_cast<char *>(read_buf.data()), count * sizeof(MarkInCompressedFile));
                byteSwapMarksIfNeeded(read_buf.data(), count);
                builder->addMarks(read_buf.data(), count);
                marks_read += count;
            }
        }
        else
        {
            reader->readStrict(reinterpret_cast<char *>(plain_marks.data()), expected_uncompressed_size);
            byteSwapMarksIfNeeded(plain_marks.data(), total_marks);
        }

        if (!reader->eof())
            throw Exception(
                ErrorCodes::CANNOT_READ_ALL_DATA,
                "Cannot read all marks from file {}, is eof: {}, buffer size: {}, expected size: {}",
                full_mark_path,
                reader->eof(),
                reader->buffer().size(),
                expected_uncompressed_size);
    }
    else
    {
        /// Adaptive marks: each granule has num_columns_in_mark marks + a granularity value.
        /// When streaming, read each granule's marks into a small buffer and feed to Builder.
        /// Builder handles internal MARKS_PER_BLOCK buffering.
        PODArray<MarkInCompressedFile> granule_buf;
        if (use_streaming_compression)
            granule_buf.resize(num_columns_in_mark);

        for (size_t granule = 0; granule < marks_count; ++granule)
        {
            if (reader->eof())
                throw Exception(
                    ErrorCodes::CANNOT_READ_ALL_DATA,
                    "Cannot read all marks from file {}, marks expected {} (bytes size {}), marks read {} (bytes size {})",
                    full_mark_path, marks_count, expected_uncompressed_size, granule, reader->count());

            size_t granularity = 0;
            auto * dest = use_streaming_compression
                ? granule_buf.data()
                : plain_marks.data() + granule * num_columns_in_mark;
            reader->readStrict(reinterpret_cast<char *>(dest), num_columns_in_mark * sizeof(MarkInCompressedFile));
            readBinaryLittleEndian(granularity, *reader);

            if (use_streaming_compression)
            {
                byteSwapMarksIfNeeded(granule_buf.data(), num_columns_in_mark);
                builder->addMarks(granule_buf.data(), num_columns_in_mark);
            }
        }

        if (!use_streaming_compression)
            byteSwapMarksIfNeeded(plain_marks.data(), total_marks);

        if (!reader->eof())
            throw Exception(
                ErrorCodes::CANNOT_READ_ALL_DATA,
                "Too many marks in file {}, marks expected {} (bytes size {})",
                full_mark_path, marks_count, expected_uncompressed_size);
    }

    auto res = use_streaming_compression
        ? builder->finish()
        : MarksInCompressedFile::create(plain_marks);

    ProfileEvents::increment(ProfileEvents::LoadedMarksFiles);
    ProfileEvents::increment(ProfileEvents::LoadedMarksCount, total_marks);
    ProfileEvents::increment(ProfileEvents::LoadedMarksMemoryBytes, res->approximateMemoryUsage());

    return res;
}

MarkCache::MappedPtr MergeTreeMarksLoader::loadMarksSync()
{
    MarkCache::MappedPtr loaded_marks;

    auto data_part_storage = data_part_reader->getDataPartStorage();

    if (mark_cache)
    {
        auto key = MarkCache::hash(data_part_storage->getDiskName() + ":" + (fs::path(data_part_storage->getFullPath()) / mrk_path).string());

        if (save_marks_in_cache)
        {
            auto callback = [this] { return loadMarksImpl(); };
            loaded_marks = mark_cache->getOrSet(key, callback);
        }
        else
        {
            loaded_marks = mark_cache->getWithoutMetrics(key);
            if (!loaded_marks)
                loaded_marks = loadMarksImpl();
        }
    }
    else
    {
        loaded_marks = loadMarksImpl();
    }

    if (!loaded_marks)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Failed to load marks: {}", (fs::path(data_part_storage->getFullPath()) / mrk_path).string());
    }

    return loaded_marks;
}

std::future<MarkCache::MappedPtr> MergeTreeMarksLoader::loadMarksAsync()
{
    /// Avoid queueing jobs into thread pool if marks are in cache
    auto data_part_storage = data_part_reader->getDataPartStorage();
    auto key = MarkCache::hash(data_part_storage->getDiskName() + ":" + (fs::path(data_part_storage->getFullPath()) / mrk_path).string());
    if (MarkCache::MappedPtr loaded_marks = mark_cache->getForAsyncLoading(key))
    {
        ProfileEvents::increment(ProfileEvents::MarksTasksFromCache);
        auto promise = std::promise<MarkCache::MappedPtr>();
        promise.set_value(std::move(loaded_marks));
        return promise.get_future();
    }

    return scheduleFromThreadPoolUnsafe<MarkCache::MappedPtr>(
        [this]() -> MarkCache::MappedPtr
        {
            auto component_guard = Coordination::setCurrentComponent("MergeTreeMarksLoader::loadMarksAsync");
            if (is_canceled)
            {
                ProfileEvents::increment(ProfileEvents::LoadingMarksTasksCanceled);
                throw Exception(ErrorCodes::ASYNC_LOAD_CANCELED, "Background task for loading marks was canceled");
            }

            ProfileEvents::increment(ProfileEvents::BackgroundLoadingMarksTasks);
            return loadMarksSync();
        },
        *load_marks_threadpool,
        ThreadName::LOAD_MARKS);
}

void addMarksToCache(const IMergeTreeDataPart & part, const PlainMarksByName & cached_marks, MarkCache * mark_cache)
{
    MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

    for (const auto & [stream_name, marks] : cached_marks)
    {
        auto mark_path = part.index_granularity_info.getMarksFilePath(stream_name);
        auto key = MarkCache::hash(part.getDataPartStorage().getDiskName() + ":" + (fs::path(part.getRelativePathOfActivePart()) / mark_path).string());
        mark_cache->set(key, MarksInCompressedFile::create(*marks));
    }
}

Names getColumnsToPrewarmMarks(const MergeTreeSettings & settings, const NamesAndTypesList & columns_list)
{
    auto columns_str = settings[MergeTreeSetting::columns_to_prewarm_mark_cache].toString();
    if (columns_str.empty())
        return columns_list.getNames();

    return parseIdentifiersOrStringLiterals(columns_str, Context::getGlobalContextInstance()->getSettingsRef());
}

}
