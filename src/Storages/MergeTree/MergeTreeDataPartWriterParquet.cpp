#include <Storages/MergeTree/MergeTreeDataPartWriterParquet.h>

#if USE_PARQUET

#include <Storages/MergeTree/MergeTreeDataPartParquet.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ParallelSyncFiles.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionCodecMultiple.h>
#include <Compression/CompressionInfo.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromVector.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/isValidUTF8.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

namespace
{

/// Whether the type is stored as a Parquet column without definition and repetition levels, i.e. without nulls and arrays.
bool isRequiredWithoutArrays(const IDataType & type)
{
    switch (type.getTypeId())
    {
        case TypeIndex::Nullable:
        case TypeIndex::Array:
        case TypeIndex::Map:
            return false;
        case TypeIndex::LowCardinality:
            return !assert_cast<const DataTypeLowCardinality &>(type).getDictionaryType()->isNullable();
        case TypeIndex::Tuple:
        {
            for (const auto & element : assert_cast<const DataTypeTuple &>(type).getElements())
                if (!isRequiredWithoutArrays(*element))
                    return false;
            return true;
        }
        default:
            return true;
    }
}

bool canStoreTypeInParquetImpl(const IDataType & type)
{
    switch (type.getTypeId())
    {
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
        case TypeIndex::UInt128:
        case TypeIndex::UInt256:
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64:
        case TypeIndex::Int128:
        case TypeIndex::Int256:
        case TypeIndex::Float32:
        case TypeIndex::Float64:
        case TypeIndex::Date:
        case TypeIndex::Date32:
        case TypeIndex::DateTime:
        case TypeIndex::String:
        case TypeIndex::FixedString:
        case TypeIndex::Enum8:
        case TypeIndex::Enum16:
        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128:
        case TypeIndex::Decimal256:
        case TypeIndex::UUID:
        case TypeIndex::IPv4:
        case TypeIndex::IPv6:
            return true;
        case TypeIndex::DateTime64:
        {
            /// Parquet timestamps have milli-, micro- or nanosecond units. The values with scales 7 and 8 are stored
            /// as nanoseconds, which don't cover the whole range of `DateTime64` with these scales.
            UInt32 scale = assert_cast<const DataTypeDateTime64 &>(type).getScale();
            return scale != 7 && scale != 8;
        }
        case TypeIndex::Nullable:
        {
            const auto & nested = assert_cast<const DataTypeNullable &>(type).getNestedType();
            /// A nullable Tuple can be read back only if its elements are stored without definition and
            /// repetition levels: then the null map of the Tuple is the null map of each of its elements.
            if (nested->getTypeId() == TypeIndex::Tuple && !isRequiredWithoutArrays(*nested))
                return false;
            return canStoreTypeInParquetImpl(*nested);
        }
        case TypeIndex::LowCardinality:
            return canStoreTypeInParquetImpl(*assert_cast<const DataTypeLowCardinality &>(type).getDictionaryType());
        case TypeIndex::Array:
            return canStoreTypeInParquetImpl(*assert_cast<const DataTypeArray &>(type).getNestedType());
        case TypeIndex::Map:
        {
            const auto & map = assert_cast<const DataTypeMap &>(type);
            return canStoreTypeInParquetImpl(*map.getKeyType()) && canStoreTypeInParquetImpl(*map.getValueType());
        }
        case TypeIndex::Tuple:
        {
            const auto & elements = assert_cast<const DataTypeTuple &>(type).getElements();
            if (elements.empty())
                return false;
            for (const auto & element : elements)
                if (!canStoreTypeInParquetImpl(*element))
                    return false;
            return true;
        }
        default:
            /// E.g. `Variant`, `Dynamic`, `JSON`, `AggregateFunction`, `Time`, `BFloat16`, `Interval`.
            return false;
    }
}

/// The type of values (without `Nullable`, `LowCardinality` and arrays) if it is a primitive type.
DataTypePtr getInnermostPrimitiveType(DataTypePtr type)
{
    while (true)
    {
        switch (type->getTypeId())
        {
            case TypeIndex::Nullable:
                type = assert_cast<const DataTypeNullable &>(*type).getNestedType();
                break;
            case TypeIndex::LowCardinality:
                type = assert_cast<const DataTypeLowCardinality &>(*type).getDictionaryType();
                break;
            case TypeIndex::Array:
                type = assert_cast<const DataTypeArray &>(*type).getNestedType();
                break;
            case TypeIndex::Map:
            case TypeIndex::Tuple:
                return nullptr;
            default:
                return type;
        }
    }
}

std::optional<Int64> getCodecLevel(const ICompressionCodec & codec)
{
    /// Keep the description alive while it is inspected.
    ASTPtr description = codec.getCodecDescription();
    const auto * function = description->as<ASTFunction>();
    if (!function || !function->arguments || function->arguments->children.empty())
        return {};
    const auto * literal = function->arguments->children[0]->as<ASTLiteral>();
    if (!literal)
        return {};
    return applyVisitor(FieldVisitorConvertToNumber<Int64>(), literal->value);
}

/// Sets the compression method and the encoding for the column, similar to the codec of the column.
/// The codecs that don't have similar counterparts in Parquet are ignored.
void applyCodec(const ICompressionCodec & codec, const DataTypePtr & type, const String & column_name, Parquet::WriteOptions & options)
{
    if (const auto * multiple = typeid_cast<const CompressionCodecMultiple *>(&codec))
    {
        for (const auto & nested_codec : multiple->getCodecs())
            applyCodec(*nested_codec, type, column_name, options);
        return;
    }

    if (codec.isEncryption())
        throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
            "Cannot write column {} with an encryption codec into a data part in Parquet format", column_name);

    if (codec.isNone())
        return;

    if (codec.isGenericCompression())
    {
        if (codec.getMethodByte() == static_cast<uint8_t>(CompressionMethodByte::ZSTD))
        {
            options.compression = CompressionMethod::Zstd;
            options.compression_level = static_cast<int>(getCodecLevel(codec).value_or(1));
        }
        else
        {
            /// `LZ4`, `LZ4HC` and the other generic codecs.
            options.compression = CompressionMethod::Lz4;
        }
        return;
    }

    /// Specialized codecs are mapped to similar encodings. They are applied to all values, so the dictionary
    /// encoding is not used for such columns.
    auto primitive_type = getInnermostPrimitiveType(type);
    if (!primitive_type)
        return;

    WhichDataType which(primitive_type);
    bool is_integer = (which.isNativeInt() || which.isNativeUInt() || which.isDateOrDate32OrDateTimeOrDateTime64())
        && !which.isUInt8() && !which.isInt8() && primitive_type->getName() != "Bool";
    bool is_float = which.isFloat32() || which.isFloat64();

    if (is_integer && (codec.isDeltaCompression()
        || codec.getMethodByte() == static_cast<uint8_t>(CompressionMethodByte::T64)
        || codec.getMethodByte() == static_cast<uint8_t>(CompressionMethodByte::GCD)
        || codec.getMethodByte() == static_cast<uint8_t>(CompressionMethodByte::PCO)))
    {
        options.encoding = Parquet::parq::Encoding::DELTA_BINARY_PACKED;
        options.use_dictionary_encoding = false;
    }
    else if (is_float && (codec.isFloatingPointTimeSeriesCodec()
        || codec.getMethodByte() == static_cast<uint8_t>(CompressionMethodByte::ALP)
        || codec.getMethodByte() == static_cast<uint8_t>(CompressionMethodByte::PCO)))
    {
        options.encoding = Parquet::parq::Encoding::BYTE_STREAM_SPLIT;
        options.use_dictionary_encoding = false;
    }
}

}

bool MergeTreeDataPartWriterParquet::canStoreTypeInParquet(const DataTypePtr & type)
{
    return canStoreTypeInParquetImpl(*type);
}

MergeTreeDataPartWriterParquet::MergeTreeDataPartWriterParquet(
    const String & data_part_name_,
    const String & logger_name_,
    const SerializationByName & serializations_,
    MutableDataPartStoragePtr data_part_storage_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    const MergeTreeSettingsPtr & storage_settings_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    MergeTreeIndexGranularityPtr index_granularity_)
    : MergeTreeDataPartWriterOnDisk(
        data_part_name_, logger_name_, serializations_,
        data_part_storage_, index_granularity_info_, storage_settings_,
        columns_list_, metadata_snapshot_,
        indices_to_recalc_, marks_file_extension_,
        default_codec_, settings_, std::move(index_granularity_),
        static_cast<WrittenOffsetSubstreams *>(nullptr))
{
    /// The format of the file must not depend on the settings of a query, so the options are fixed here.
    file_options.output_string_as_string = true;
    file_options.output_fixed_string_as_fixed_byte_array = true;
    /// Geo types are stored as their underlying tuples and arrays, to be read back as is.
    file_options.write_geometadata = false;
    /// Many parts are written in parallel, so don't accumulate the bloom filters in memory.
    file_options.bloom_filter_flush_threshold_bytes = 0;

    Poco::JSON::Array::Ptr columns_metadata = new Poco::JSON::Array;

    auto & root = schema.emplace_back();
    root.__set_name("schema");
    root.__set_num_children(static_cast<Int32>(columns_list.size()));

    for (const auto & name_and_type : columns_list)
    {
        ColumnToWrite & column = columns_to_write.emplace_back();
        column.name_and_type = name_and_type;
        column.binary = !canStoreTypeInParquet(name_and_type.type);
        column.options = file_options;
        column.options.compression = CompressionMethod::None;

        /// The codec of a column applies to its values (e.g. to the nested column of `Nullable` or `Array`),
        /// like to the data substreams of the column in other formats.
        /// The codecs declared for subcolumns cannot be applied to the parts of a Parquet column, so the column codec
        /// is used, or the default codec of the part (which already accounts for `default_compression_codec` and
        /// `RECOMPRESS` TTL, see `MergeTreeData::getCompressionCodecForPart`).
        const ColumnDescription * column_desc = metadata_snapshot->columns.tryGet(name_and_type.name);
        if (!column_desc)
            column_desc = metadata_snapshot->virtuals.tryGetDescription(name_and_type.name, VirtualsKind::All, VirtualsMaterializationPlace::Reader);
        if (!column_desc)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column name: {}", name_and_type.name);
        ASTPtr codec_desc = column_desc->codec ? column_desc->codec : default_codec->getFullCodecDescription();
        /// For compound types (`Tuple`, `Map`) only the generic codecs are used.
        auto values_type = getInnermostPrimitiveType(name_and_type.type);
        auto codec = CompressionCodecFactory::instance().get(codec_desc, values_type.get(), default_codec, /*only_generic=*/ !values_type);
        applyCodec(*codec, column.binary ? std::make_shared<DataTypeString>() : name_and_type.type, name_and_type.name, column.options);

        /// The values in the binary format are not text.
        if (column.binary)
            column.options.output_string_as_string = false;

        DataTypePtr type_in_file = column.binary ? std::make_shared<DataTypeString>() : name_and_type.type;
        size_t schema_size_before = schema.size();
        Parquet::prepareColumnForWrite(type_in_file->createColumn(), type_in_file, name_and_type.name, column.options, nullptr, &schema);
        for (size_t i = schema_size_before; i < schema.size(); ++i)
            if (!schema[i].__isset.num_children)
                column.leaf_schema_indices.push_back(i);

        Poco::JSON::Object::Ptr column_metadata = new Poco::JSON::Object;
        column_metadata->set("name", name_and_type.name);
        column_metadata->set("type", name_and_type.type->getName());
        column_metadata->set("encoding", column.binary ? "binary" : "parquet");
        columns_metadata->add(column_metadata);
    }

    {
        Poco::JSON::Object::Ptr metadata = new Poco::JSON::Object;
        metadata->set("columns", columns_metadata);
        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Stringifier::stringify(metadata, oss);
        key_value_metadata.emplace_back(MergeTreeDataPartParquet::COLUMNS_METADATA_KEY, oss.str());
    }

    data_file = getDataPartStorage().writeFile(
        MergeTreeDataPartParquet::DATA_FILE_NAME_WITH_EXTENSION,
        settings.max_compress_block_size,
        settings_.query_write_settings);
    data_hashing = std::make_unique<HashingWriteBuffer>(*data_file);

    marks_file = getDataPartStorage().writeFile(
        MergeTreeDataPartParquet::DATA_FILE_NAME + marks_file_extension_,
        4096,
        settings_.query_write_settings);
    marks_file_hashing = std::make_unique<HashingWriteBuffer>(*marks_file);

    if (index_granularity_info.mark_type.compressed)
    {
        marks_compressor = std::make_unique<CompressedWriteBuffer>(
            *marks_file_hashing,
            CompressionCodecFactory::instance().get(settings_.marks_compression_codec),
            settings_.marks_compress_block_size);

        marks_source_hashing = std::make_unique<HashingWriteBuffer>(*marks_compressor);
    }

    if (settings.save_marks_in_cache)
        cached_marks[MergeTreeDataPartParquet::DATA_FILE_NAME] = std::make_unique<MarksInCompressedFile::PlainArray>();

    Parquet::writeFileHeader(file_state, *data_hashing);
}

namespace
{

/// Whether all values of a column of strings, written with the `STRING` logical type, are valid UTF-8.
bool isValidUTF8Strings(const IColumn & column)
{
    if (const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(&column))
        return isValidUTF8Strings(*low_cardinality->getDictionary().getNestedNotNullableColumn());

    const auto * string_column = typeid_cast<const ColumnString *>(&column);
    if (!string_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} with the STRING logical type", column.getName());

    const auto & chars = string_column->getChars();
    if (!UTF8::isValidUTF8(chars.data(), chars.size()))
        return false;

    /// The concatenation of the values is valid, so every value is valid if none of them starts in the middle
    /// of a character, i.e. with a continuation byte.
    const auto & offsets = string_column->getOffsets();
    for (size_t row = 0; row < offsets.size(); ++row)
    {
        size_t begin = offsets[row - 1];
        if (begin < offsets[row] && (chars[begin] & 0xC0) == 0x80)
            return false;
    }
    return true;
}

/// Get granules for block using index_granularity
Granules getGranulesToWrite(const MergeTreeIndexGranularity & index_granularity, size_t block_rows, size_t current_mark, bool last_block)
{
    if (current_mark >= index_granularity.getMarksCount())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Request to get granules from mark {} but index granularity size is {}",
                        current_mark, index_granularity.getMarksCount());

    Granules result;
    size_t current_row = 0;
    while (current_row < block_rows)
    {
        size_t expected_rows_in_mark = index_granularity.getMarkRows(current_mark);
        size_t rows_left_in_block = block_rows - current_row;
        if (rows_left_in_block < expected_rows_in_mark && !last_block)
        {
            /// Invariant: we always have equal amount of rows for block because we accumulate them in buffer.
            /// The only exclusion is the last block, when we cannot accumulate more rows.
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Required to write {} rows, but only {} rows was written for the non last granule",
                            expected_rows_in_mark, rows_left_in_block);
        }

        result.emplace_back(Granule{
            .start_row = current_row,
            .rows_to_write = std::min(rows_left_in_block, expected_rows_in_mark),
            .mark_number = current_mark,
            .mark_on_start = true,
            .is_complete = (rows_left_in_block >= expected_rows_in_mark)
        });
        current_row += result.back().rows_to_write;
        ++current_mark;
    }

    return result;
}

}

size_t MergeTreeDataPartWriterParquet::computeRowGroupGranularity(const Block & block) const
{
    size_t row_group_bytes = index_granularity_info.index_granularity_bytes;
    return DB::computeIndexGranularity(
        block.rows(),
        row_group_bytes ? getBlockSizeForGranularity(block) : 0,
        row_group_bytes,
        index_granularity_info.fixed_index_granularity,
        settings.blocks_are_granules_size,
        /// A zero limit for bytes means that the size of row groups is restricted only by rows.
        settings.can_use_adaptive_granularity && row_group_bytes != 0);
}

void MergeTreeDataPartWriterParquet::write(const Block & block, const IColumnPermutation * permutation, Block * /*permuted_columns_cache*/)
{
    /// Fill index granularity for this block
    /// if it's unknown (in case of insert data or horizontal merge,
    /// but not in case of vertical merge)
    if (compute_granularity)
    {
        size_t index_granularity_for_block = computeRowGroupGranularity(block);
        chassert(index_granularity_for_block >= 1);
        fillIndexGranularity(index_granularity_for_block, block.rows());
    }

    Block result_block = permuteBlockIfNeeded(block, permutation, nullptr);

    if (header.empty())
        header = result_block.cloneEmpty();

    size_t current_mark_rows = index_granularity->getMarkRows(getCurrentMark());
    Block flushed_block;
    if (buffered_rows == 0 && result_block.rows() >= current_mark_rows)
    {
        flushed_block = std::move(result_block);
    }
    else
    {
        size_t rows_in_block = result_block.rows();
        if (buffered_columns.empty())
        {
            buffered_columns = result_block.mutateColumns();
        }
        else
        {
            for (size_t i = 0; i < buffered_columns.size(); ++i)
            {
                /// Fix dynamic structure so it won't changed after insertion of new rows.
                buffered_columns[i]->fixDynamicStructure();
                const auto & column = result_block.getByPosition(i).column;
                buffered_columns[i]->insertRangeFrom(*column, 0, column->size());
            }
        }
        buffered_rows += rows_in_block;

        if (buffered_rows >= current_mark_rows)
        {
            flushed_block = header.cloneWithColumns(std::move(buffered_columns));
            buffered_columns.clear();
            buffered_rows = 0;
        }
    }

    if (!flushed_block.empty())
    {
        auto granules_to_write = getGranulesToWrite(*index_granularity, flushed_block.rows(), getCurrentMark(), /* last_block = */ false);
        writeDataBlockPrimaryIndexAndSkipIndices(flushed_block, granules_to_write);
        setCurrentMark(getCurrentMark() + granules_to_write.size());
    }
}

void MergeTreeDataPartWriterParquet::writeDataBlockPrimaryIndexAndSkipIndices(const Block & block, const Granules & granules_to_write)
{
    writeRowGroups(block, granules_to_write);

    if (settings.rewrite_primary_key)
    {
        Block primary_key_block = getIndexBlockAndPermute(block, metadata_snapshot->getPrimaryKeyColumns(), nullptr);
        calculateAndSerializePrimaryIndex(primary_key_block, granules_to_write);
    }

    Block skip_indices_block = getIndexBlockAndPermute(block, getSkipIndicesColumns(), nullptr);
    calculateAndSerializeSkipIndices(skip_indices_block, granules_to_write);
}

void MergeTreeDataPartWriterParquet::writeColumnChunks(size_t column_idx, const ColumnPtr & column, WriteBuffer & marks_out)
{
    const auto & column_to_write = columns_to_write[column_idx];
    const auto & [name, type] = column_to_write.name_and_type;

    ColumnPtr column_in_file = column;
    DataTypePtr type_in_file = type;
    if (column_to_write.binary)
    {
        auto full_column = column->convertToFullColumnIfReplicated()->convertToFullColumnIfSparse()->convertToFullColumnIfConst();
        auto serialization = type->getDefaultSerialization();
        auto binary_column = ColumnString::create();
        auto & chars = binary_column->getChars();
        auto & offsets = binary_column->getOffsets();
        offsets.reserve(full_column->size());
        {
            WriteBufferFromVector<ColumnString::Chars> buf(chars);
            FormatSettings format_settings;
            for (size_t row = 0; row < full_column->size(); ++row)
            {
                serialization->serializeBinary(*full_column, row, buf, format_settings);
                offsets.push_back(buf.count());
            }
            buf.finalize();
        }
        column_in_file = std::move(binary_column);
        type_in_file = std::make_shared<DataTypeString>();
    }

    size_t start = data_hashing->count();
    chassert(start == file_state.offset);

    Parquet::ColumnChunkWriteStates states;
    Parquet::prepareColumnForWrite(column_in_file, type_in_file, name, column_to_write.options, &states);

    if (states.size() != column_to_write.leaf_schema_indices.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Unexpected number of column chunks for column {}: {}, expected {}", name, states.size(), column_to_write.leaf_schema_indices.size());

    /// `String` can contain arbitrary bytes, but the `STRING` logical type requires UTF-8. The schema is written
    /// in the footer of the file, so the annotation is removed from it as soon as any value is not valid UTF-8,
    /// and external applications read such a column as binary. The statistics and bloom filters are the same.
    for (size_t i = 0; i < states.size(); ++i)
    {
        auto & element = schema[column_to_write.leaf_schema_indices[i]];
        if (element.__isset.logicalType && element.logicalType.__isset.STRING && !isValidUTF8Strings(*states[i].primitive_column))
        {
            element.__isset.logicalType = false;
            element.logicalType = {};
            element.__isset.converted_type = false;
        }
    }

    FormatSettings format_settings;
    for (auto & state : states)
    {
        Parquet::writeColumnChunkBody(state, column_to_write.options, format_settings, *data_hashing);
        Parquet::finalizeColumnChunkAndWriteFooter(std::move(state), file_state, *data_hashing);
    }

    size_t end = data_hashing->count();
    if (end != file_state.offset)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Unexpected offset in Parquet file after writing column {}: {}, expected {}", name, end, file_state.offset);

    columns_to_write[column_idx].compressed_bytes += end - start;

    MarkInCompressedFile mark{start, end - start};
    writeBinaryLittleEndian(mark.offset_in_compressed_file, marks_out);
    writeBinaryLittleEndian(mark.offset_in_decompressed_block, marks_out);

    if (!cached_marks.empty())
        cached_marks.begin()->second->push_back(mark);
}

void MergeTreeDataPartWriterParquet::writeRowGroups(const Block & block, const Granules & granules)
{
    WriteBuffer & marks_out = marks_source_hashing ? *marks_source_hashing : *marks_file_hashing;

    for (const auto & granule : granules)
    {
        for (size_t i = 0; i < columns_to_write.size(); ++i)
        {
            ColumnPtr column = block.getByName(columns_to_write[i].name_and_type.name).column;
            if (granule.start_row != 0 || granule.rows_to_write != column->size())
                column = column->cut(granule.start_row, granule.rows_to_write);

            columns_to_write[i].uncompressed_bytes += column->byteSize();
            writeColumnChunks(i, column, marks_out);
        }

        Parquet::finalizeRowGroup(file_state, granule.rows_to_write, file_options, *data_hashing);

        writeBinaryLittleEndian(granule.rows_to_write, marks_out);
        data_written = true;
    }
}

void MergeTreeDataPartWriterParquet::finalizeIndexGranularity()
{
    if (buffered_rows != 0)
    {
        auto block = header.cloneWithColumns(std::move(buffered_columns));
        buffered_columns.clear();
        buffered_rows = 0;

        auto granules_to_write = getGranulesToWrite(*index_granularity, block.rows(), getCurrentMark(), /*last_block=*/ true);
        if (!granules_to_write.back().is_complete)
        {
            /// Correct last mark as it should contain exact amount of rows.
            index_granularity->adjustLastMark(granules_to_write.back().rows_to_write);
        }
        writeDataBlockPrimaryIndexAndSkipIndices(block, granules_to_write);
    }

    WriteBuffer & marks_out = marks_source_hashing ? *marks_source_hashing : *marks_file_hashing;

    if (with_final_mark && data_written)
    {
        /// The final mark points to the end of the data, and there is no data after it.
        /// Instead of the size of the data, it has the uncompressed size of the column (in memory),
        /// see `MergeTreeDataPartParquet::calculateEachColumnSizes`.
        for (const auto & column : columns_to_write)
        {
            MarkInCompressedFile mark{data_hashing->count(), column.uncompressed_bytes};
            writeBinaryLittleEndian(mark.offset_in_compressed_file, marks_out);
            writeBinaryLittleEndian(mark.offset_in_decompressed_block, marks_out);

            if (!cached_marks.empty())
                cached_marks.begin()->second->push_back(mark);
        }

        writeBinaryLittleEndian(static_cast<UInt64>(0), marks_out);
    }
}

void MergeTreeDataPartWriterParquet::fillDataChecksums(MergeTreeDataPartChecksums & checksums)
{
    Parquet::writeFileFooter(file_state, std::move(schema), file_options, *data_hashing, header, key_value_metadata);

    data_hashing->finalize();
    data_file->next();

    if (marks_source_hashing)
        marks_source_hashing->finalize();
    if (marks_compressor)
        marks_compressor->finalize();

    marks_file_hashing->finalize();

    String data_file_name = MergeTreeDataPartParquet::DATA_FILE_NAME_WITH_EXTENSION;
    String marks_file_name = MergeTreeDataPartParquet::DATA_FILE_NAME + marks_file_extension;

    /// The Parquet file is not compressed as a whole, it has compression inside.
    checksums.files[data_file_name].file_size = data_hashing->count();
    checksums.files[data_file_name].file_hash = data_hashing->getHash();

    if (marks_compressor)
    {
        checksums.files[marks_file_name].is_compressed = true;
        checksums.files[marks_file_name].uncompressed_size = marks_source_hashing->count();
        checksums.files[marks_file_name].uncompressed_hash = marks_source_hashing->getHash();
    }

    checksums.files[marks_file_name].file_size = marks_file_hashing->count();
    checksums.files[marks_file_name].file_hash = marks_file_hashing->getHash();

    data_file->preFinalize();
    marks_file->preFinalize();
}

void MergeTreeDataPartWriterParquet::finishDataSerialization(bool sync)
{
    if (sync)
        parallelSyncFiles({data_file.get(), marks_file.get()});

    data_file->finalize();
    marks_file->finalize();

    /// Release the file descriptors before the part's temporary directory is renamed,
    /// see the comment in `MergeTreeDataPartWriterCompact::finishDataSerialization`.
    data_file = nullptr;
    marks_file = nullptr;
}

std::optional<std::unordered_map<String, ColumnSize>> MergeTreeDataPartWriterParquet::getWrittenColumnSizes() const
{
    std::unordered_map<String, ColumnSize> sizes;
    for (const auto & column : columns_to_write)
    {
        auto & size = sizes[column.name_and_type.name];
        size.data_compressed = column.compressed_bytes;
        size.data_uncompressed = column.uncompressed_bytes;
    }
    return sizes;
}

void MergeTreeDataPartWriterParquet::fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block)
{
    size_t index_offset = 0;
    if (index_granularity->getMarksCount() > getCurrentMark())
        index_offset = index_granularity->getMarkRows(getCurrentMark()) - buffered_rows;

    fillIndexGranularityForCompactPart(*index_granularity, index_offset, index_granularity_for_block, rows_in_block);
}

void MergeTreeDataPartWriterParquet::fillChecksums(MergeTreeDataPartChecksums & checksums, NameSet & /*checksums_to_remove*/)
{
    if (!columns_list.empty())
        fillDataChecksums(checksums);

    if (settings.rewrite_primary_key)
        fillPrimaryIndexChecksums(checksums);

    fillSkipIndicesChecksums(checksums);
}

void MergeTreeDataPartWriterParquet::finish(bool sync)
{
    if (!columns_list.empty())
        finishDataSerialization(sync);

    if (settings.rewrite_primary_key)
        finishPrimaryIndexSerialization(sync);

    finishSkipIndicesSerialization(sync);
}

void MergeTreeDataPartWriterParquet::cancel() noexcept
{
    MergeTreeDataPartWriterOnDisk::cancel();

    if (data_hashing)
        data_hashing->cancel();
    if (data_file)
        data_file->cancel();

    if (marks_source_hashing)
        marks_source_hashing->cancel();
    if (marks_compressor)
        marks_compressor->cancel();
    if (marks_file_hashing)
        marks_file_hashing->cancel();
    if (marks_file)
        marks_file->cancel();
}

}

#endif
