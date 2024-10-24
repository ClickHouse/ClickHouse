#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>
#include <Interpreters/Context.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Common/escapeForFileName.h>
#include <Columns/ColumnSparse.h>
#include <Common/logger_useful.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 max_file_name_length;
    extern const MergeTreeSettingsBool replace_long_file_name_to_hash;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_FILE_NAME;
}

namespace
{
    constexpr auto DATA_FILE_EXTENSION = ".bin";
}

namespace
{

/// Get granules for block using index_granularity
Granules getGranulesToWrite(const MergeTreeIndexGranularity & index_granularity, size_t block_rows, size_t current_mark, size_t rows_written_in_last_mark)
{
    if (current_mark >= index_granularity.getMarksCount())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Request to get granules from mark {} but index granularity size is {}",
                        current_mark, index_granularity.getMarksCount());

    Granules result;
    size_t current_row = 0;

    /// When our last mark is not finished yet and we have to write rows into it
    if (rows_written_in_last_mark > 0)
    {
        size_t rows_left_in_last_mark = index_granularity.getMarkRows(current_mark) - rows_written_in_last_mark;
        size_t rows_left_in_block = block_rows - current_row;
        result.emplace_back(Granule{
            .start_row = current_row,
            .rows_to_write = std::min(rows_left_in_block, rows_left_in_last_mark),
            .mark_number = current_mark,
            .mark_on_start = false, /// Don't mark this granule because we have already marked it
            .is_complete = (rows_left_in_block >= rows_left_in_last_mark),
        });
        current_row += result.back().rows_to_write;
        ++current_mark;
    }

    /// Calculating normal granules for block
    while (current_row < block_rows)
    {
        size_t expected_rows_in_mark = index_granularity.getMarkRows(current_mark);
        size_t rows_left_in_block  = block_rows - current_row;
        /// If we have less rows in block than expected in granularity
        /// save incomplete granule
        result.emplace_back(Granule{
            .start_row = current_row,
            .rows_to_write = std::min(rows_left_in_block, expected_rows_in_mark),
            .mark_number = current_mark,
            .mark_on_start = true,
            .is_complete = (rows_left_in_block >= expected_rows_in_mark),
        });
        current_row += result.back().rows_to_write;
        ++current_mark;
    }

    return result;
}

}

MergeTreeDataPartWriterWide::MergeTreeDataPartWriterWide(
    const String & data_part_name_,
    const String & logger_name_,
    const SerializationByName & serializations_,
    MutableDataPartStoragePtr data_part_storage_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    const MergeTreeSettingsPtr & storage_settings_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const VirtualsDescriptionPtr & virtual_columns_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc_,
    const ColumnsStatistics & stats_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    const MergeTreeIndexGranularity & index_granularity_)
    : MergeTreeDataPartWriterOnDisk(
            data_part_name_, logger_name_, serializations_,
            data_part_storage_, index_granularity_info_, storage_settings_,
            columns_list_, metadata_snapshot_, virtual_columns_,
            indices_to_recalc_, stats_to_recalc_, marks_file_extension_,
            default_codec_, settings_, index_granularity_)
{
    for (const auto & column : columns_list)
    {
        auto compression = getCodecDescOrDefault(column.name, default_codec);
        addStreams(column, nullptr, compression);
    }
}

void MergeTreeDataPartWriterWide::initDynamicStreamsIfNeeded(const DB::Block & block)
{
    if (is_dynamic_streams_initialized)
        return;

    is_dynamic_streams_initialized = true;
    block_sample = block.cloneEmpty();
    for (const auto & column : columns_list)
    {
        if (column.type->hasDynamicSubcolumns())
        {
            auto compression = getCodecDescOrDefault(column.name, default_codec);
            addStreams(column, block_sample.getByName(column.name).column, compression);
        }
    }
}

void MergeTreeDataPartWriterWide::addStreams(
    const NameAndTypePair & name_and_type,
    const ColumnPtr & column,
    const ASTPtr & effective_codec_desc)
{
    ISerialization::StreamCallback callback = [&](const auto & substream_path)
    {
        assert(!substream_path.empty());

        /// Don't create streams for ephemeral subcolumns that don't store any real data.
        if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
            return;

        auto full_stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

        String stream_name;
        if ((*storage_settings)[MergeTreeSetting::replace_long_file_name_to_hash] && full_stream_name.size() > (*storage_settings)[MergeTreeSetting::max_file_name_length])
            stream_name = sipHash128String(full_stream_name);
        else
            stream_name = full_stream_name;

        /// Shared offsets for Nested type.
        if (column_streams.contains(stream_name))
            return;

        auto it = stream_name_to_full_name.find(stream_name);
        if (it != stream_name_to_full_name.end() && it->second != full_stream_name)
            throw Exception(ErrorCodes::INCORRECT_FILE_NAME,
                "Stream with name {} already created (full stream name: {}). Current full stream name: {}."
                " It is a collision between a filename for one column and a hash of filename for another column or a bug",
                stream_name, it->second, full_stream_name);

        const auto & subtype = substream_path.back().data.type;
        CompressionCodecPtr compression_codec;

        /// If we can use special codec then just get it
        if (ISerialization::isSpecialCompressionAllowed(substream_path))
            compression_codec = CompressionCodecFactory::instance().get(effective_codec_desc, subtype.get(), default_codec);
        else /// otherwise return only generic codecs and don't use info about the` data_type
            compression_codec = CompressionCodecFactory::instance().get(effective_codec_desc, nullptr, default_codec, true);

        ParserCodec codec_parser;
        auto ast = parseQuery(codec_parser, "(" + Poco::toUpper(settings.marks_compression_codec) + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        CompressionCodecPtr marks_compression_codec = CompressionCodecFactory::instance().get(ast, nullptr);

        const auto column_desc = metadata_snapshot->columns.tryGetColumnDescription(GetColumnsOptions(GetColumnsOptions::AllPhysical), name_and_type.getNameInStorage());

        UInt64 max_compress_block_size = 0;
        if (column_desc)
            if (const auto * value = column_desc->settings.tryGet("max_compress_block_size"))
                max_compress_block_size = value->safeGet<UInt64>();
        if (!max_compress_block_size)
            max_compress_block_size = settings.max_compress_block_size;

        WriteSettings query_write_settings = settings.query_write_settings;
        query_write_settings.use_adaptive_write_buffer = settings.use_adaptive_write_buffer_for_dynamic_subcolumns && ISerialization::isDynamicSubcolumn(substream_path, substream_path.size());
        query_write_settings.adaptive_write_buffer_initial_size = settings.adaptive_write_buffer_initial_size;

        column_streams[stream_name] = std::make_unique<Stream<false>>(
            stream_name,
            data_part_storage,
            stream_name, DATA_FILE_EXTENSION,
            stream_name, marks_file_extension,
            compression_codec,
            max_compress_block_size,
            marks_compression_codec,
            settings.marks_compress_block_size,
            query_write_settings);

        full_name_to_stream_name.emplace(full_stream_name, stream_name);
        stream_name_to_full_name.emplace(stream_name, full_stream_name);
    };

    ISerialization::SubstreamPath path;
    getSerialization(name_and_type.name)->enumerateStreams(callback, name_and_type.type, column);
}

const String & MergeTreeDataPartWriterWide::getStreamName(
    const NameAndTypePair & column,
    const ISerialization::SubstreamPath & substream_path) const
{
    auto full_stream_name = ISerialization::getFileNameForStream(column, substream_path);
    return full_name_to_stream_name.at(full_stream_name);
}

ISerialization::OutputStreamGetter MergeTreeDataPartWriterWide::createStreamGetter(
        const NameAndTypePair & column, WrittenOffsetColumns & offset_columns) const
{
    return [&, this] (const ISerialization::SubstreamPath & substream_path) -> WriteBuffer *
    {
        /// Skip ephemeral subcolumns that don't store any real data.
        if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
            return nullptr;

        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;
        auto stream_name = getStreamName(column, substream_path);

        /// Don't write offsets more than one time for Nested type.
        if (is_offsets && offset_columns.contains(stream_name))
            return nullptr;

        return &column_streams.at(stream_name)->compressed_hashing;
    };
}


void MergeTreeDataPartWriterWide::shiftCurrentMark(const Granules & granules_written)
{
    auto last_granule = granules_written.back();
    /// If we didn't finished last granule than we will continue to write it from new block
    if (!last_granule.is_complete)
    {
        if (settings.can_use_adaptive_granularity && settings.blocks_are_granules_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Incomplete granules are not allowed while blocks are granules size. "
                "Mark number {} (rows {}), rows written in last mark {}, rows to write in last mark from block {} (from row {}), "
                "total marks currently {}", last_granule.mark_number, index_granularity.getMarkRows(last_granule.mark_number),
                rows_written_in_last_mark, last_granule.rows_to_write, last_granule.start_row, index_granularity.getMarksCount());

        /// Shift forward except last granule
        setCurrentMark(getCurrentMark() + granules_written.size() - 1);
        bool still_in_the_same_granule = granules_written.size() == 1;
        /// We wrote whole block in the same granule, but didn't finished it.
        /// So add written rows to rows written in last_mark
        if (still_in_the_same_granule)
            rows_written_in_last_mark += last_granule.rows_to_write;
        else
            rows_written_in_last_mark = last_granule.rows_to_write;
    }
    else
    {
        setCurrentMark(getCurrentMark() + granules_written.size());
        rows_written_in_last_mark = 0;
    }
}

void MergeTreeDataPartWriterWide::write(const Block & block, const IColumn::Permutation * permutation)
{
    /// On first block of data initialize streams for dynamic subcolumns.
    initDynamicStreamsIfNeeded(block);

    /// Fill index granularity for this block
    /// if it's unknown (in case of insert data or horizontal merge,
    /// but not in case of vertical part of vertical merge)
    if (compute_granularity)
    {
        size_t index_granularity_for_block = computeIndexGranularity(block);
        if (rows_written_in_last_mark > 0)
        {
            size_t rows_left_in_last_mark = index_granularity.getMarkRows(getCurrentMark()) - rows_written_in_last_mark;
            /// Previous granularity was much bigger than our new block's
            /// granularity let's adjust it, because we want add new
            /// heavy-weight blocks into small old granule.
            if (rows_left_in_last_mark > index_granularity_for_block)
            {
                /// We have already written more rows than granularity of our block.
                /// adjust last mark rows and flush to disk.
                if (rows_written_in_last_mark >= index_granularity_for_block)
                    adjustLastMarkIfNeedAndFlushToDisk(rows_written_in_last_mark);
                else /// We still can write some rows from new block into previous granule. So the granule size will be block granularity size.
                    adjustLastMarkIfNeedAndFlushToDisk(index_granularity_for_block);
            }
        }

        fillIndexGranularity(index_granularity_for_block, block.rows());
    }

    Block block_to_write = block;

    auto granules_to_write = getGranulesToWrite(index_granularity, block_to_write.rows(), getCurrentMark(), rows_written_in_last_mark);

    auto offset_columns = written_offset_columns ? *written_offset_columns : WrittenOffsetColumns{};
    Block primary_key_block;
    if (settings.rewrite_primary_key)
        primary_key_block = getIndexBlockAndPermute(block, metadata_snapshot->getPrimaryKeyColumns(), permutation);

    Block skip_indexes_block = getIndexBlockAndPermute(block, getSkipIndicesColumns(), permutation);

    auto it = columns_list.begin();
    for (size_t i = 0; i < columns_list.size(); ++i, ++it)
    {
        auto & column = block_to_write.getByName(it->name);

        if (getSerialization(it->name)->getKind() != ISerialization::Kind::SPARSE)
            column.column = recursiveRemoveSparse(column.column);

        if (permutation)
        {
            if (primary_key_block.has(it->name))
            {
                const auto & primary_column = *primary_key_block.getByName(it->name).column;
                writeColumn(*it, primary_column, offset_columns, granules_to_write);
            }
            else if (skip_indexes_block.has(it->name))
            {
                const auto & index_column = *skip_indexes_block.getByName(it->name).column;
                writeColumn(*it, index_column, offset_columns, granules_to_write);
            }
            else
            {
                /// We rearrange the columns that are not included in the primary key here; Then the result is released - to save RAM.
                ColumnPtr permuted_column = column.column->permute(*permutation, 0);
                writeColumn(*it, *permuted_column, offset_columns, granules_to_write);
            }
        }
        else
        {
            writeColumn(*it, *column.column, offset_columns, granules_to_write);
        }
    }

    if (settings.rewrite_primary_key)
        calculateAndSerializePrimaryIndex(primary_key_block, granules_to_write);

    calculateAndSerializeSkipIndices(skip_indexes_block, granules_to_write);
    calculateAndSerializeStatistics(block);

    shiftCurrentMark(granules_to_write);
}

void MergeTreeDataPartWriterWide::writeSingleMark(
    const NameAndTypePair & name_and_type,
    WrittenOffsetColumns & offset_columns,
    size_t number_of_rows)
{
    auto * sample_column = block_sample.findByName(name_and_type.name);
    StreamsWithMarks marks = getCurrentMarksForColumn(name_and_type, sample_column ? sample_column->column : nullptr, offset_columns);
    for (const auto & mark : marks)
        flushMarkToFile(mark, number_of_rows);
}

void MergeTreeDataPartWriterWide::flushMarkToFile(const StreamNameAndMark & stream_with_mark, size_t rows_in_mark)
{
    auto & stream = *column_streams[stream_with_mark.stream_name];
    WriteBuffer & marks_out = stream.compress_marks ? stream.marks_compressed_hashing : stream.marks_hashing;

    writeBinaryLittleEndian(stream_with_mark.mark.offset_in_compressed_file, marks_out);
    writeBinaryLittleEndian(stream_with_mark.mark.offset_in_decompressed_block, marks_out);
    if (settings.can_use_adaptive_granularity)
        writeBinaryLittleEndian(rows_in_mark, marks_out);
}

StreamsWithMarks MergeTreeDataPartWriterWide::getCurrentMarksForColumn(
    const NameAndTypePair & name_and_type,
    const ColumnPtr & column_sample,
    WrittenOffsetColumns & offset_columns)
{
    StreamsWithMarks result;
    const auto column_desc = metadata_snapshot->columns.tryGetColumnDescription(GetColumnsOptions(GetColumnsOptions::AllPhysical), name_and_type.getNameInStorage());
    UInt64 min_compress_block_size = 0;
    if (column_desc)
        if (const auto * value = column_desc->settings.tryGet("min_compress_block_size"))
            min_compress_block_size = value->safeGet<UInt64>();
    if (!min_compress_block_size)
        min_compress_block_size = settings.min_compress_block_size;
    getSerialization(name_and_type.name)->enumerateStreams([&] (const ISerialization::SubstreamPath & substream_path)
    {
       /// Skip ephemeral subcolumns that don't store any real data.
       if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
           return;

        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;
        auto stream_name = getStreamName(name_and_type, substream_path);

        /// Don't write offsets more than one time for Nested type.
        if (is_offsets && offset_columns.contains(stream_name))
            return;

        auto & stream = *column_streams[stream_name];

        /// There could already be enough data to compress into the new block.
        if (stream.compressed_hashing.offset() >= min_compress_block_size)
            stream.compressed_hashing.next();

        StreamNameAndMark stream_with_mark;
        stream_with_mark.stream_name = stream_name;
        stream_with_mark.mark.offset_in_compressed_file = stream.plain_hashing.count();
        stream_with_mark.mark.offset_in_decompressed_block = stream.compressed_hashing.offset();

        result.push_back(stream_with_mark);
    }, name_and_type.type, column_sample);

    return result;
}

void MergeTreeDataPartWriterWide::writeSingleGranule(
    const NameAndTypePair & name_and_type,
    const IColumn & column,
    WrittenOffsetColumns & offset_columns,
    ISerialization::SerializeBinaryBulkStatePtr & serialization_state,
    ISerialization::SerializeBinaryBulkSettings & serialize_settings,
    const Granule & granule)
{
    const auto & serialization = getSerialization(name_and_type.name);
    serialization->serializeBinaryBulkWithMultipleStreams(column, granule.start_row, granule.rows_to_write, serialize_settings, serialization_state);

    /// So that instead of the marks pointing to the end of the compressed block, there were marks pointing to the beginning of the next one.
    serialization->enumerateStreams([&] (const ISerialization::SubstreamPath & substream_path)
    {
        /// Skip ephemeral subcolumns that don't store any real data.
        if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
            return;

        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;
        auto stream_name = getStreamName(name_and_type, substream_path);

        /// Don't write offsets more than one time for Nested type.
        if (is_offsets && offset_columns.contains(stream_name))
            return;

        column_streams.at(stream_name)->compressed_hashing.nextIfAtEnd();
    }, name_and_type.type, column.getPtr());
}

/// Column must not be empty. (column.size() !== 0)
void MergeTreeDataPartWriterWide::writeColumn(
    const NameAndTypePair & name_and_type,
    const IColumn & column,
    WrittenOffsetColumns & offset_columns,
    const Granules & granules)
{
    if (granules.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty granules for column {}, current mark {}",
                        backQuoteIfNeed(name_and_type.name), getCurrentMark());

    const auto & [name, type] = name_and_type;
    auto [it, inserted] = serialization_states.emplace(name, nullptr);
    auto serialization = getSerialization(name_and_type.name);

    if (inserted)
    {
        ISerialization::SerializeBinaryBulkSettings serialize_settings;
        serialize_settings.use_compact_variant_discriminators_serialization = settings.use_compact_variant_discriminators_serialization;
        serialize_settings.getter = createStreamGetter(name_and_type, offset_columns);
        serialization->serializeBinaryBulkStatePrefix(column, serialize_settings, it->second);
    }

    ISerialization::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.getter = createStreamGetter(name_and_type, offset_columns);
    serialize_settings.low_cardinality_max_dictionary_size = settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = settings.low_cardinality_use_single_dictionary_for_part;
    serialize_settings.use_compact_variant_discriminators_serialization = settings.use_compact_variant_discriminators_serialization;

    for (const auto & granule : granules)
    {
        data_written = true;

        if (granule.mark_on_start)
        {
            if (last_non_written_marks.contains(name))
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "We have to add new mark for column, but already have non written mark. "
                                "Current mark {}, total marks {}, offset {}",
                                getCurrentMark(), index_granularity.getMarksCount(), rows_written_in_last_mark);
            last_non_written_marks[name] = getCurrentMarksForColumn(name_and_type, column.getPtr(), offset_columns);
        }

        writeSingleGranule(
           name_and_type,
           column,
           offset_columns,
           it->second,
           serialize_settings,
           granule
        );

        if (granule.is_complete)
        {
            auto marks_it = last_non_written_marks.find(name);
            if (marks_it == last_non_written_marks.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No mark was saved for incomplete granule for column {}", backQuoteIfNeed(name));

            for (const auto & mark : marks_it->second)
                flushMarkToFile(mark, index_granularity.getMarkRows(granule.mark_number));
            last_non_written_marks.erase(marks_it);
        }
    }

    serialization->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;
        if (is_offsets)
            offset_columns.insert(getStreamName(name_and_type, substream_path));
    }, name_and_type.type, column.getPtr());
}


void MergeTreeDataPartWriterWide::validateColumnOfFixedSize(const NameAndTypePair & name_type)
{
    const auto & [name, type] = name_type;
    const auto & serialization = getSerialization(name_type.name);

    if (!type->isValueRepresentedByNumber() || type->haveSubtypes() || serialization->getKind() != ISerialization::Kind::DEFAULT)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot validate column of non fixed type {}", type->getName());

    String escaped_name = escapeForFileName(name);
    String mrk_path = escaped_name + marks_file_extension;
    String bin_path = escaped_name + DATA_FILE_EXTENSION;

    /// Some columns may be removed because of ttl. Skip them.
    if (!getDataPartStorage().existsFile(mrk_path))
        return;

    auto mrk_file_in = getDataPartStorage().readFile(mrk_path, {}, std::nullopt, std::nullopt);
    std::unique_ptr<ReadBuffer> mrk_in;
    if (index_granularity_info.mark_type.compressed)
        mrk_in = std::make_unique<CompressedReadBufferFromFile>(std::move(mrk_file_in));
    else
        mrk_in = std::move(mrk_file_in);

    DB::CompressedReadBufferFromFile bin_in(getDataPartStorage().readFile(bin_path, {}, std::nullopt, std::nullopt));
    bool must_be_last = false;
    UInt64 offset_in_compressed_file = 0;
    UInt64 offset_in_decompressed_block = 0;
    UInt64 index_granularity_rows = index_granularity_info.fixed_index_granularity;

    size_t mark_num;

    for (mark_num = 0; !mrk_in->eof(); ++mark_num)
    {
        if (mark_num > index_granularity.getMarksCount())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Incorrect number of marks in memory {}, on disk (at least) {}",
                            index_granularity.getMarksCount(), mark_num + 1);

        readBinaryLittleEndian(offset_in_compressed_file, *mrk_in);
        readBinaryLittleEndian(offset_in_decompressed_block, *mrk_in);
        if (settings.can_use_adaptive_granularity)
            readBinaryLittleEndian(index_granularity_rows, *mrk_in);
        else
            index_granularity_rows = index_granularity_info.fixed_index_granularity;

        if (must_be_last)
        {
            if (index_granularity_rows != 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "We ran out of binary data but still have non empty mark #{} with rows number {}",
                                mark_num, index_granularity_rows);

            if (!mrk_in->eof())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Mark #{} must be last, but we still have some to read", mark_num);

            break;
        }

        if (index_granularity_rows == 0)
        {
            auto column = type->createColumn();

            serialization->deserializeBinaryBulk(*column, bin_in, 1000000000, 0.0);

            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Still have {} rows in bin stream, last mark #{}"
                            " index granularity size {}, last rows {}",
                            column->size(), mark_num, index_granularity.getMarksCount(), index_granularity_rows);
        }

        if (index_granularity_rows != index_granularity.getMarkRows(mark_num))
        {
            throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Incorrect mark rows for part {} for mark #{}"
                            " (compressed offset {}, decompressed offset {}), in-memory {}, on disk {}, total marks {}",
                            getDataPartStorage().getFullPath(),
                            mark_num, offset_in_compressed_file, offset_in_decompressed_block,
                            index_granularity.getMarkRows(mark_num), index_granularity_rows,
                            index_granularity.getMarksCount());
        }

        auto column = type->createColumn();

        serialization->deserializeBinaryBulk(*column, bin_in, index_granularity_rows, 0.0);

        if (bin_in.eof())
        {
            must_be_last = true;
        }

        /// Now they must be equal
        if (column->size() != index_granularity_rows)
        {

            if (must_be_last)
            {
                /// The only possible mark after bin.eof() is final mark. When we
                /// cannot use adaptive granularity we cannot have last mark.
                /// So finish validation.
                if (!settings.can_use_adaptive_granularity)
                    break;

                /// If we don't compute granularity then we are not responsible
                /// for last mark (for example we mutating some column from part
                /// with fixed granularity where last mark is not adjusted)
                if (!compute_granularity)
                    continue;
            }

            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Incorrect mark rows for mark #{} (compressed offset {}, decompressed offset {}), "
                "actually in bin file {}, in mrk file {}, total marks {}",
                mark_num, offset_in_compressed_file, offset_in_decompressed_block, column->size(),
                index_granularity.getMarkRows(mark_num), index_granularity.getMarksCount());
        }
    }

    if (!mrk_in->eof())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Still have something in marks stream, last mark #{}"
                        " index granularity size {}, last rows {}",
                        mark_num, index_granularity.getMarksCount(), index_granularity_rows);
    if (!bin_in.eof())
    {
        auto column = type->createColumn();

        serialization->deserializeBinaryBulk(*column, bin_in, 1000000000, 0.0);

        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Still have {} rows in bin stream, last mark #{}"
                            " index granularity size {}, last rows {}",
                            column->size(), mark_num, index_granularity.getMarksCount(), index_granularity_rows);
    }
}

void MergeTreeDataPartWriterWide::fillDataChecksums(MergeTreeDataPartChecksums & checksums, NameSet & checksums_to_remove)
{
    ISerialization::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.low_cardinality_max_dictionary_size = settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = settings.low_cardinality_use_single_dictionary_for_part;
    serialize_settings.use_compact_variant_discriminators_serialization = settings.use_compact_variant_discriminators_serialization;
    WrittenOffsetColumns offset_columns;
    if (rows_written_in_last_mark > 0)
    {
        if (settings.can_use_adaptive_granularity && settings.blocks_are_granules_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Incomplete granule is not allowed while blocks are granules size even for last granule. "
                            "Mark number {} (rows {}), rows written for last mark {}, total marks {}",
                            getCurrentMark(), index_granularity.getMarkRows(getCurrentMark()),
                            rows_written_in_last_mark, index_granularity.getMarksCount());

        adjustLastMarkIfNeedAndFlushToDisk(rows_written_in_last_mark);
    }

    bool write_final_mark = (with_final_mark && data_written);

    {
        auto it = columns_list.begin();
        for (size_t i = 0; i < columns_list.size(); ++i, ++it)
        {
            if (!serialization_states.empty())
            {
                serialize_settings.getter = createStreamGetter(*it, written_offset_columns ? *written_offset_columns : offset_columns);
                serialize_settings.object_and_dynamic_write_statistics = ISerialization::SerializeBinaryBulkSettings::ObjectAndDynamicStatisticsMode::SUFFIX;
                getSerialization(it->name)->serializeBinaryBulkStateSuffix(serialize_settings, serialization_states[it->name]);
            }

            if (write_final_mark)
                writeFinalMark(*it, offset_columns);
        }
    }

    for (auto & [stream_name, stream] : column_streams)
    {
        /// Remove checksums for old stream name if file was
        /// renamed due to replacing the name to the hash of name.
        const auto & full_stream_name = stream_name_to_full_name.at(stream_name);
        if (stream_name != full_stream_name)
        {
            checksums_to_remove.insert(full_stream_name + stream->data_file_extension);
            checksums_to_remove.insert(full_stream_name + stream->marks_file_extension);
        }

        stream->preFinalize();
        stream->addToChecksums(checksums);
    }
}

void MergeTreeDataPartWriterWide::finishDataSerialization(bool sync)
{
    for (auto & stream : column_streams)
    {
        stream.second->finalize();
        if (sync)
            stream.second->sync();
    }

    column_streams.clear();
    serialization_states.clear();

#ifndef NDEBUG
    /// Heavy weight validation of written data. Checks that we are able to read
    /// data according to marks. Otherwise throws LOGICAL_ERROR (equal to abort in debug mode)
    for (const auto & column : columns_list)
    {
        if (column.type->isValueRepresentedByNumber()
            && !column.type->haveSubtypes()
            && getSerialization(column.name)->getKind() == ISerialization::Kind::DEFAULT)
        {
            validateColumnOfFixedSize(column);
        }
    }
#endif

}

void MergeTreeDataPartWriterWide::fillChecksums(MergeTreeDataPartChecksums & checksums, NameSet & checksums_to_remove)
{
    // If we don't have anything to write, skip finalization.
    if (!columns_list.empty())
        fillDataChecksums(checksums, checksums_to_remove);

    if (settings.rewrite_primary_key)
        fillPrimaryIndexChecksums(checksums);

    fillSkipIndicesChecksums(checksums);

    fillStatisticsChecksums(checksums);
}

void MergeTreeDataPartWriterWide::finish(bool sync)
{
    // If we don't have anything to write, skip finalization.
    if (!columns_list.empty())
        finishDataSerialization(sync);

    if (settings.rewrite_primary_key)
        finishPrimaryIndexSerialization(sync);

    finishSkipIndicesSerialization(sync);

    finishStatisticsSerialization(sync);
}

void MergeTreeDataPartWriterWide::writeFinalMark(
    const NameAndTypePair & name_and_type,
    WrittenOffsetColumns & offset_columns)
{
    writeSingleMark(name_and_type, offset_columns, 0);
    /// Memoize information about offsets
    getSerialization(name_and_type.name)->enumerateStreams([&] (const ISerialization::SubstreamPath & substream_path)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;
        if (is_offsets)
            offset_columns.insert(getStreamName(name_and_type, substream_path));
    }, name_and_type.type, block_sample.getByName(name_and_type.name).column);
}

static void fillIndexGranularityImpl(
    MergeTreeIndexGranularity & index_granularity,
    size_t index_offset,
    size_t index_granularity_for_block,
    size_t rows_in_block)
{
    for (size_t current_row = index_offset; current_row < rows_in_block; current_row += index_granularity_for_block)
        index_granularity.appendMark(index_granularity_for_block);
}

void MergeTreeDataPartWriterWide::fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block)
{
    if (getCurrentMark() < index_granularity.getMarksCount() && getCurrentMark() != index_granularity.getMarksCount() - 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to add marks, while current mark {}, but total marks {}",
                        getCurrentMark(), index_granularity.getMarksCount());

    size_t index_offset = 0;
    if (rows_written_in_last_mark != 0)
        index_offset = index_granularity.getLastMarkRows() - rows_written_in_last_mark;

    fillIndexGranularityImpl(
        index_granularity,
        index_offset,
        index_granularity_for_block,
        rows_in_block);
}


void MergeTreeDataPartWriterWide::adjustLastMarkIfNeedAndFlushToDisk(size_t new_rows_in_last_mark)
{
    /// We don't want to split already written granules to smaller
    if (rows_written_in_last_mark > new_rows_in_last_mark)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tryin to make mark #{} smaller ({} rows) then it already has {}",
                        getCurrentMark(), new_rows_in_last_mark, rows_written_in_last_mark);

    /// We can adjust marks only if we computed granularity for blocks.
    /// Otherwise we cannot change granularity because it will differ from
    /// other columns
    if (compute_granularity && settings.can_use_adaptive_granularity)
    {
        if (getCurrentMark() != index_granularity.getMarksCount() - 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Non last mark {} (with {} rows) having rows offset {}, total marks {}",
                            getCurrentMark(), index_granularity.getMarkRows(getCurrentMark()),
                            rows_written_in_last_mark, index_granularity.getMarksCount());

        index_granularity.popMark();
        index_granularity.appendMark(new_rows_in_last_mark);
    }

    /// Last mark should be filled, otherwise it's a bug
    if (last_non_written_marks.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No saved marks for last mark {} having rows offset {}, total marks {}",
                        getCurrentMark(), rows_written_in_last_mark, index_granularity.getMarksCount());

    if (rows_written_in_last_mark == new_rows_in_last_mark)
    {
        for (const auto & [name, marks] : last_non_written_marks)
        {
            for (const auto & mark : marks)
                flushMarkToFile(mark, index_granularity.getMarkRows(getCurrentMark()));
        }

        last_non_written_marks.clear();

        if (compute_granularity && settings.can_use_adaptive_granularity)
        {
            /// Also we add mark to each skip index because all of them
            /// already accumulated all rows from current adjusting mark
            for (size_t i = 0; i < skip_indices.size(); ++i)
                ++skip_index_accumulated_marks[i];

            /// This mark completed, go further
            setCurrentMark(getCurrentMark() + 1);
            /// Without offset
            rows_written_in_last_mark = 0;
        }
    }
}

}
