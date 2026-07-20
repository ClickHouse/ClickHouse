#include <Processors/Sources/MergeTreePointReadSource.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressionInfo.h>
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
#include <Storages/MergeTree/MergeTreeReaderWide.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// On-disk framing per compressed block: 16-byte checksum + 9-byte header (see CompressedReadBufferBase).
constexpr size_t COMPRESSED_BLOCK_FRAMING = 16 + COMPRESSED_BLOCK_HEADER_SIZE;

const IDataType * getFixedArrayElementType(const IDataType & type)
{
    const auto * array_type = typeid_cast<const DataTypeArray *>(&type);
    return array_type ? array_type->getNestedType().get() : nullptr;
}

struct ElementStreamInfo
{
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
    auto info = resolveElementStream(part, column, dimensions);
    if (!info)
        return false;

    const size_t file_size = part.data_part->getFileSizeOrZero(info->file_name);
    const size_t rows = part.data_part->rows_count;
    /// Exactly one vector per compressed block iff the whole element stream is `rows` blocks of `framing + row_size`.
    return file_size != 0 && file_size == rows * (COMPRESSED_BLOCK_FRAMING + info->row_size);
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
    : ISource(header_)
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
    row_size = info->row_size;
    block_stride = COMPRESSED_BLOCK_FRAMING + row_size;

    auto buf = part.data_part->getDataPartStorage().readFile(
        info->file_name, reader_settings.read_settings, part.data_part->getFileSizeOrZero(info->file_name));
    vector_buffer = std::make_unique<CompressedReadBufferFromFile>(std::move(buf), /*allow_different_codecs=*/ true);

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
            ReadBufferFromFileBase::ProfileCallback{},
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
        vector_buffer->seek(row * block_stride, /*offset_in_decompressed_block=*/ 0);
        char * dst = extendAndGetWriteDstByType(element_type_id, nested, dimensions);
        vector_buffer->readStrict(dst, row_size);
        offsets.push_back(nested.size());
    }
}

void MergeTreePointReadSource::readOtherColumns(size_t base, size_t batch, Columns & dst_columns)
{
    const auto & index_granularity = *part.data_part->index_granularity;
    dst_columns.assign(other_columns.size(), nullptr); /// nullptr -> reader creates the column; then it appends.

    for (size_t i = 0; i < batch; ++i)
    {
        const UInt64 row = row_offsets[base + i];
        const size_t from_mark = index_granularity.getMarkRangeForRowOffset(row).begin;
        const size_t granule_start = index_granularity.getMarkStartingRow(from_mark);
        other_reader->readRows(
            from_mark, /*current_task_last_mark=*/ from_mark + 1, /*continue_reading=*/ false,
            /*max_rows_to_read=*/ 1, /*rows_offset=*/ row - granule_start, dst_columns);
    }
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

    /// Assemble the output in `header` order: the vector column from the point read, the rest from `other_result`
    /// (which is in `other_columns` order == header order minus the vector column).
    Columns result;
    result.reserve(header->columns());
    size_t other_idx = 0;
    for (const auto & header_column : *header)
    {
        if (header_column.name == vector_column.name)
            result.push_back(std::move(vector_col));
        else
            result.push_back(other_result[other_idx++]);
    }

    next_offset_index += batch;
    return Chunk(std::move(result), batch);
}

}
