#include <Processors/Sources/MergeTreePointReadSource.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressionInfo.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
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
    if (!array_type)
        return nullptr;
    return array_type->getNestedType().get();
}

/// Resolve the Array-elements `.bin` file name and per-row byte size for `column` in `part`.
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

template <typename T>
void appendVector(IColumn & nested, const char * src, size_t dims)
{
    auto & data = assert_cast<ColumnVector<T> &>(nested).getData();
    const size_t old_size = data.size();
    data.resize(old_size + dims);
    memcpy(data.data() + old_size, src, dims * sizeof(T));
}

void appendVectorByType(TypeIndex element_type_id, IColumn & nested, const char * src, size_t dims)
{
    switch (element_type_id)
    {
        case TypeIndex::BFloat16: appendVector<BFloat16>(nested, src, dims); return;
        case TypeIndex::Float32:  appendVector<Float32>(nested, src, dims);  return;
        case TypeIndex::Float64:  appendVector<Float64>(nested, src, dims);  return;
        case TypeIndex::Int8:     appendVector<Int8>(nested, src, dims);     return;
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
    NameAndTypePair column_,
    size_t dimensions_,
    MergeTreeReaderSettings reader_settings_,
    size_t max_block_size_)
    : ISource(std::move(header_))
    , part(std::move(part_))
    , row_offsets(std::move(row_offsets_))
    , column(std::move(column_))
    , dimensions(dimensions_)
    , reader_settings(std::move(reader_settings_))
    , max_block_size(max_block_size_)
{
}

MergeTreePointReadSource::~MergeTreePointReadSource() = default;

void MergeTreePointReadSource::initialize()
{
    auto info = resolveElementStream(part, column, dimensions);
    if (!info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreePointReadSource: column {} is not a fixed-size Array stream", column.name);

    element_size = info->element_size;
    row_size = info->row_size;
    block_stride = COMPRESSED_BLOCK_FRAMING + row_size;
    element_file_name = info->file_name;

    auto buf = part.data_part->getDataPartStorage().readFile(
        element_file_name, reader_settings.read_settings, part.data_part->getFileSizeOrZero(element_file_name));
    data_buffer = std::make_unique<CompressedReadBufferFromFile>(std::move(buf), /*allow_different_codecs=*/ true);

    initialized = true;
}

Chunk MergeTreePointReadSource::generate()
{
    if (!initialized)
        initialize();

    if (next_offset_index >= row_offsets.size())
        return {};

    const TypeIndex element_type_id = getFixedArrayElementType(*column.type)->getTypeId();
    const size_t batch = std::min(max_block_size, row_offsets.size() - next_offset_index);

    auto array_column = column.type->createColumn();
    auto & array = assert_cast<ColumnArray &>(*array_column);
    IColumn & nested = array.getData();
    auto & offsets = array.getOffsets();
    offsets.reserve(batch);

    std::vector<char> row_bytes(row_size);
    for (size_t i = 0; i < batch; ++i)
    {
        const UInt64 row = row_offsets[next_offset_index + i];
        data_buffer->seek(row * block_stride, /*offset_in_decompressed_block=*/ 0);
        data_buffer->readStrict(row_bytes.data(), row_size);
        appendVectorByType(element_type_id, nested, row_bytes.data(), dimensions);
        offsets.push_back(nested.size());
    }
    next_offset_index += batch;

    return Chunk(Columns{std::move(array_column)}, batch);
}

}
