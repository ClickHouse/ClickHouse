#include <Processors/Sources/MergeTreePointReadSource.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Core/Block.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeReaderWide.h>
#include <Common/assert_cast.h>

#include <bit>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

const IDataType * getFixedArrayElementType(const IDataType & type)
{
    const auto * array_type = typeid_cast<const DataTypeArray *>(&type);
    return array_type ? array_type->getNestedType().get() : nullptr;
}

struct ElementStreamInfo
{
    String stream_name;
    String file_name;
    size_t element_size = 0;
    size_t row_size = 0;
};

std::optional<ElementStreamInfo> resolveElementStream(const RangesInDataPart & part, const NameAndTypePair & column, size_t dimensions)
{
    const IDataType * element_type = getFixedArrayElementType(*column.type);
    if (!element_type)
        return {};

    ElementStreamInfo info;
    info.element_size = element_type->getSizeOfValueInMemory();
    info.row_size = dimensions * info.element_size;
    if (info.row_size == 0)
        return {};

    ISerialization::SubstreamPath substream_path;
    substream_path.push_back(ISerialization::Substream::ArrayElements);
    auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
        column, substream_path, ".bin", part.data_part->checksums, part.data_part->storage.getSettings());
    if (!stream_name)
        return {};

    info.stream_name = *stream_name;
    info.file_name = *stream_name + ".bin";
    return info;
}

/// Extend `nested` by `dims` elements and return a writable byte pointer to that region (valid until the next resize).
template <typename T>
char * extendAndGetWriteDst(IColumn & nested, size_t dims)
{
    auto & data = assert_cast<ColumnVector<T> &>(nested).getData();
    const size_t old_size = data.size();
    data.resize(old_size + dims);
    return reinterpret_cast<char *>(data.data() + old_size);
}

char * extendAndGetWriteDstByType(TypeIndex element_type_id, IColumn & nested, size_t dims)
{
    switch (element_type_id)
    {
        case TypeIndex::BFloat16: return extendAndGetWriteDst<BFloat16>(nested, dims);
        case TypeIndex::Float32:  return extendAndGetWriteDst<Float32>(nested, dims);
        case TypeIndex::Float64:  return extendAndGetWriteDst<Float64>(nested, dims);
        case TypeIndex::Int8:     return extendAndGetWriteDst<Int8>(nested, dims);
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreePointReadSource: unsupported element type");
    }
}

}

bool MergeTreePointReadSource::isEligible(const RangesInDataPart & part, const NameAndTypePair & column, size_t dimensions)
{
    /// The point read memcpy's the stored bytes straight into the column, bypassing `SerializationNumber`, which
    /// converts numeric substreams from their little-endian on-disk form on big-endian targets.
    if constexpr (std::endian::native == std::endian::big)
        return false;

    auto info = resolveElementStream(part, column, dimensions);
    if (!info)
        return false;

    const size_t file_size = part.data_part->getFileSizeOrZero(info->file_name);
    const size_t rows = part.data_part->rows_count;

    return FixedWidthPointReadLayout::tryDetect(file_size, rows, info->row_size).has_value();
}

MergeTreePointReadSource::MergeTreePointReadSource(
    SharedHeader header_,
    RangesInDataPart part_,
    PaddedPODArray<UInt64> row_offsets_,
    NameAndTypePair vector_column_,
    size_t dimensions_,
    NamesAndTypesList other_columns_,
    StorageSnapshotPtr storage_snapshot_,
    MergeTreeReaderSettings reader_settings_,
    MarkCachePtr mark_cache_,
    size_t max_block_size_)
    /// Auto-progress would report `chunk.bytes()`; this source accounts the real read instead, as MergeTreeSource does.
    : ISource(header_, /*enable_auto_progress=*/ false)
    , header(std::move(header_))
    , part(std::move(part_))
    , row_offsets(std::move(row_offsets_))
    , vector_column(std::move(vector_column_))
    , dimensions(dimensions_)
    , other_columns(std::move(other_columns_))
    , storage_snapshot(std::move(storage_snapshot_))
    , reader_settings(std::move(reader_settings_))
    , mark_cache(std::move(mark_cache_))
    , max_block_size(max_block_size_)
{
}

MergeTreePointReadSource::~MergeTreePointReadSource() = default;

void MergeTreePointReadSource::initialize()
{
    auto info = resolveElementStream(part, vector_column, dimensions);
    if (!info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreePointReadSource: column {} is not a fixed-size Array stream", vector_column.name);

    element_size = info->element_size;

    const size_t file_size = part.data_part->getFileSizeOrZero(info->file_name);
    auto layout = FixedWidthPointReadLayout::tryDetect(file_size, part.data_part->rows_count, info->row_size);
    if (!layout)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreePointReadSource: column {} is not a fixed-width point-read stream", vector_column.name);

    auto count_read_bytes = [this](ReadBufferFromFileBase::ProfileInfo info_)
    { read_bytes.fetch_add(info_.bytes_read, std::memory_order_relaxed); };

    vector_stream = MergeTreeReaderStreamSingleColumnWholePart::createForFixedWidthPointRead(
        part.data_part->getDataPartStoragePtr(), info->stream_name, file_size, *layout, reader_settings, count_read_bytes);

    if (!other_columns.empty())
    {
        part_info = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(part.data_part, std::make_shared<AlterConversions>());
        other_reader = std::make_unique<MergeTreeReaderWide>(
            part_info,
            other_columns,
            VirtualFields{},
            storage_snapshot,
            part.data_part->storage.getSettings(),
            /*uncompressed_cache=*/ nullptr,
            mark_cache.get(),
            /*deserialization_prefixes_cache=*/ nullptr,
            part.ranges,
            reader_settings,
            ValueSizeMap{},
            count_read_bytes,
            CLOCK_MONOTONIC_COARSE);
    }

    initialized = true;
}

void MergeTreePointReadSource::readVectorColumn(size_t base, size_t batch, IColumn & dst_column)
{
    auto & array = assert_cast<ColumnArray &>(dst_column);
    IColumn & nested = array.getData();
    auto & offsets = array.getOffsets();
    offsets.reserve(batch);
    const TypeIndex element_type_id = getFixedArrayElementType(*vector_column.type)->getTypeId();

    for (size_t i = 0; i < batch; ++i)
    {
        const UInt64 row = row_offsets[base + i];
        char * dst = extendAndGetWriteDstByType(element_type_id, nested, dimensions);
        vector_stream->readFixedWidthPointByRowOffset(row, dst);
        offsets.push_back(nested.size());
    }
}

void MergeTreePointReadSource::readOtherColumns(size_t base, size_t batch, Columns & dst_columns)
{
    const auto & index_granularity = *part.data_part->index_granularity;
    /// nullptr -> reader creates the column; then it appends.
    MutableColumns read_columns(other_columns.size());

    for (size_t i = 0; i < batch; ++i)
    {
        const UInt64 row = row_offsets[base + i];
        const size_t from_mark = index_granularity.getMarkRangeForRowOffset(row).begin;

        /// Offsets ascend, so survivors of one granule arrive consecutively: continue where the last `readRows` left the
        /// streams rather than re-seeking to the mark, which would decompress the whole granule once per survivor.
        bool continue_reading = from_mark == last_read_mark && row >= next_unread_row;
        const size_t rows_to_skip = continue_reading
            ? row - next_unread_row
            : row - index_granularity.getMarkStartingRow(from_mark);

        /// `readRows` cannot skip leading rows, so drop them like `MergeTreeRangeReader::DelayedStream::finalize` does:
        /// read into throwaway columns. They are decompressed anyway; only the copy is wasted, bounded by the granule.
        if (rows_to_skip)
        {
            MutableColumns skipped_columns(other_columns.size());
            other_reader->readRows(from_mark, continue_reading, rows_to_skip, skipped_columns);
            continue_reading = true;
        }

        other_reader->readRows(from_mark, continue_reading, /*max_rows_to_read=*/ 1, read_columns);

        last_read_mark = from_mark;
        next_unread_row = row + 1;
    }

    /// Normalization below works on immutable columns; entries left null by the reader are filled by `fillMissingColumns`.
    dst_columns.clear();
    dst_columns.reserve(read_columns.size());
    for (auto & read_column : read_columns)
        dst_columns.push_back(std::move(read_column));

    /// Normalize like the standard read path: synthesize missing columns and defaults, fix partial Array/Nested offsets,
    /// then convert types - otherwise a column added by a later ALTER could come back invalid.
    bool should_evaluate_missing_defaults = false;
    other_reader->fillMissingColumns(dst_columns, should_evaluate_missing_defaults, batch);
    if (should_evaluate_missing_defaults)
    {
        /// Defaults are evaluated over a block that must carry the row count (as
        /// `MergeTreeReadersChain::executeActionsBeforePrewhere` does): with every requested column missing it is empty.
        Block additional_columns;
        addDummyColumnWithRowCount(additional_columns, batch);
        other_reader->evaluateMissingDefaults(additional_columns, dst_columns);
    }
    other_reader->performRequiredConversions(dst_columns);
}

Chunk MergeTreePointReadSource::generate()
{
    if (!initialized)
        initialize();

    if (next_offset_index >= row_offsets.size())
        return {};

    const size_t batch = std::min(max_block_size, row_offsets.size() - next_offset_index);

    auto vector_col = vector_column.type->createColumn();
    readVectorColumn(next_offset_index, batch, *vector_col);

    Columns other_result;
    if (!other_columns.empty())
        readOtherColumns(next_offset_index, batch, other_result);

    /// Assemble all columns together
    ColumnPtr vector_col_ptr = std::move(vector_col);
    Columns result;
    result.reserve(header->columns());
    size_t other_idx = 0;
    for (const auto & header_column : *header)
    {
        if (header_column.name == vector_column.name)
            result.push_back(vector_col_ptr);
        else
            result.push_back(other_result[other_idx++]);
    }

    next_offset_index += batch;

    /// Report what the readers actually fetched since the last chunk, not what the chunk holds.
    progress(batch, read_bytes.exchange(0, std::memory_order_relaxed));
    return Chunk(std::move(result), batch);
}

}
