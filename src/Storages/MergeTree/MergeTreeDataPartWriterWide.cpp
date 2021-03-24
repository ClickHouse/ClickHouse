#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>
#include <Interpreters/Context.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <DataTypes/DataTypeFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Request to get granules from mark {} but index granularity size is {}", current_mark, index_granularity.getMarksCount());

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
        current_mark++;
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
        current_mark++;
    }

    return result;
}

}

MergeTreeDataPartWriterWide::MergeTreeDataPartWriterWide(
    const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    const MergeTreeIndexGranularity & index_granularity_)
    : MergeTreeDataPartWriterOnDisk(data_part_, columns_list_, metadata_snapshot_,
           indices_to_recalc_, marks_file_extension_,
           default_codec_, settings_, index_granularity_)
{
    for (const auto & it : columns_list)
        addStreams(it, nullptr);
}


void MergeTreeDataPartWriterWide::addStreams(
    const NameAndTypePair & name_type,
    const ColumnPtr column)
{
    size_t written_rows = 0;
    for (const auto & granules : written_granules)
    {
        for (const auto & granule : granules)
        {
            written_rows += granule.rows_to_write;
        }
    }

    const auto & columns = metadata_snapshot->getColumns();
    const ASTPtr & effective_codec_desc = columns.getCodecDescOrDefault(name_type.name, default_codec);

    IDataType::StreamCallback callback = [&] (const IDataType::SubstreamPath & substream_path, const IDataType & substream_type)
    {
        String stream_name = IDataType::getFileNameForStream(name_type, substream_path);
        /// Shared offsets for Nested type.
        if (column_streams.count(stream_name))
            return;

        CompressionCodecPtr compression_codec;
        /// If we can use special codec then just get it
        if (IDataType::isSpecialCompressionAllowed(substream_path))
            compression_codec = CompressionCodecFactory::instance().get(effective_codec_desc, &substream_type, default_codec);
        else /// otherwise return only generic codecs and don't use info about the data_type
            compression_codec = CompressionCodecFactory::instance().get(effective_codec_desc, nullptr, default_codec, true);

        column_streams[stream_name] = std::make_unique<Stream>(
            stream_name,
            data_part->volume->getDisk(),
            part_path + stream_name, DATA_FILE_EXTENSION,
            part_path + stream_name, marks_file_extension,
            compression_codec,
            settings.max_compress_block_size);

        if (written_rows != 0)
        {
            // Refers to MergeTreeDataPartWriterWide::writeColumn().
            ColumnPtr cp = substream_type.createColumnConstWithDefaultValue(written_rows);
            auto offset_columns = written_offset_columns ? *written_offset_columns : WrittenOffsetColumns{};
            NameAndTypePair name_and_type;
            name_and_type.name = stream_name;
            name_and_type.type = DataTypeFactory::instance().get(substream_type.getName());
            const auto & [name, type] = name_and_type;
            auto [it, inserted] = serialization_states.emplace(stream_name, nullptr);

            const auto & global_settings = storage.global_context.getSettingsRef();
            IDataType::SerializeBinaryBulkSettings serialize_settings;
            serialize_settings.getter = createStreamGetter(name_and_type, offset_columns);
            serialize_settings.low_cardinality_max_dictionary_size = global_settings.low_cardinality_max_dictionary_size;
            serialize_settings.low_cardinality_use_single_dictionary_for_part = global_settings.low_cardinality_use_single_dictionary_for_part != 0;
            if (inserted)
            {
                type->serializeBinaryBulkStatePrefix(serialize_settings, it->second);
            }

            for (const auto & granules : written_granules)
            {
                for (const auto & granule : granules)
                {
                    if (granule.mark_on_start)
                    {
                        if (last_non_written_marks.count(name))
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "We have to add new mark for column, but already have non written mark.");
                        last_non_written_marks[stream_name] = getCurrentMarksForColumn(name_and_type, offset_columns, serialize_settings.path);
                    }
                    writeSingleGranule(
                        name_and_type,
                        *cp,
                        offset_columns,
                        it->second,
                        serialize_settings,
                        granule
                    );
                    if (granule.is_complete)
                    {
                        auto marks_it = last_non_written_marks.find(stream_name);
                        if (marks_it == last_non_written_marks.end())
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "No mark was saved for incomplete granule for column {}", backQuoteIfNeed(stream_name));
                        for (const auto & mark : marks_it->second)
                            flushMarkToFile(mark, index_granularity.getMarkRows(granule.mark_number));
                        last_non_written_marks.erase(marks_it);
                    }
                }
            }
        }
    };

    IDataType::SubstreamPath stream_path;
    name_type.type->enumerateStreams(callback, stream_path);
    if (column != nullptr)
    {
        name_type.type->enumerateDynamicStreams(*column, callback, stream_path);
    }
}


IDataType::OutputStreamGetter MergeTreeDataPartWriterWide::createStreamGetter(
        const NameAndTypePair & column, WrittenOffsetColumns & offset_columns) const
{
    return [&, this] (const IDataType::SubstreamPath & substream_path) -> WriteBuffer *
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;

        String stream_name = IDataType::getFileNameForStream(column, substream_path);

        /// Don't write offsets more than one time for Nested type.
        if (is_offsets && offset_columns.count(stream_name))
            return nullptr;

        return &column_streams.at(stream_name)->compressed;
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
                "Mark number {} (rows {}), rows written in last mark {}, rows to write in last mark from block {} (from row {}), total marks currently {}",
                last_granule.mark_number, index_granularity.getMarkRows(last_granule.mark_number), rows_written_in_last_mark,
                last_granule.rows_to_write, last_granule.start_row, index_granularity.getMarksCount());

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

    auto granules_to_write = getGranulesToWrite(index_granularity, block.rows(), getCurrentMark(), rows_written_in_last_mark);

    auto offset_columns = written_offset_columns ? *written_offset_columns : WrittenOffsetColumns{};
    Block primary_key_block;
    if (settings.rewrite_primary_key)
        primary_key_block = getBlockAndPermute(block, metadata_snapshot->getPrimaryKeyColumns(), permutation);

    Block skip_indexes_block = getBlockAndPermute(block, getSkipIndicesColumns(), permutation);

    auto it = columns_list.begin();
    for (size_t i = 0; i < columns_list.size(); ++i, ++it)
    {
        ColumnPtr column;
        if (permutation)
        {
            if (primary_key_block.has(it->name))
            {
                column = primary_key_block.getByName(it->name).column;
            }
            else if (skip_indexes_block.has(it->name))
            {
                column = skip_indexes_block.getByName(it->name).column;
            }
            else
            {
                /// We rearrange the columns that are not included in the primary key here; Then the result is released - to save RAM.
                column = block.getByName(it->name).column->permute(*permutation, 0);
            }
        }
        else
        {
            column = block.getByName(it->name).column;
        }

        addStreams(*it, column);
        writeColumn(*it, *column, offset_columns, granules_to_write);
    }

    if (settings.rewrite_primary_key)
        calculateAndSerializePrimaryIndex(primary_key_block, granules_to_write);

    calculateAndSerializeSkipIndices(skip_indexes_block, granules_to_write);

    shiftCurrentMark(granules_to_write);
    written_granules.push_back(std::move(granules_to_write));
}

void MergeTreeDataPartWriterWide::writeSingleMark(
    const NameAndTypePair & column,
    WrittenOffsetColumns & offset_columns,
    size_t number_of_rows,
    DB::IDataType::SubstreamPath & path)
{
    StreamsWithMarks marks = getCurrentMarksForColumn(column, offset_columns, path);
    for (const auto & mark : marks)
        flushMarkToFile(mark, number_of_rows);
}

void MergeTreeDataPartWriterWide::flushMarkToFile(const StreamNameAndMark & stream_with_mark, size_t rows_in_mark)
{
    Stream & stream = *column_streams[stream_with_mark.stream_name];
    writeIntBinary(stream_with_mark.mark.offset_in_compressed_file, stream.marks);
    writeIntBinary(stream_with_mark.mark.offset_in_decompressed_block, stream.marks);
    if (settings.can_use_adaptive_granularity)
        writeIntBinary(rows_in_mark, stream.marks);
}

StreamsWithMarks MergeTreeDataPartWriterWide::getCurrentMarksForColumn(
    const NameAndTypePair & column,
    WrittenOffsetColumns & offset_columns,
    DB::IDataType::SubstreamPath & path)
{
    StreamsWithMarks result;
    column.type->enumerateStreams([&] (const IDataType::SubstreamPath & substream_path, const IDataType & /* substream_type */)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;

        String stream_name = IDataType::getFileNameForStream(column, substream_path);

        /// Don't write offsets more than one time for Nested type.
        if (is_offsets && offset_columns.count(stream_name))
            return;

        Stream & stream = *column_streams[stream_name];

        /// There could already be enough data to compress into the new block.
        if (stream.compressed.offset() >= settings.min_compress_block_size)
            stream.compressed.next();

        StreamNameAndMark stream_with_mark;
        stream_with_mark.stream_name = stream_name;
        stream_with_mark.mark.offset_in_compressed_file = stream.plain_hashing.count();
        stream_with_mark.mark.offset_in_decompressed_block = stream.compressed.offset();

        result.push_back(stream_with_mark);
    }, path);

    return result;
}

void MergeTreeDataPartWriterWide::writeSingleGranule(
    const NameAndTypePair & name_and_type,
    const IColumn & column,
    WrittenOffsetColumns & offset_columns,
    IDataType::SerializeBinaryBulkStatePtr & serialization_state,
    IDataType::SerializeBinaryBulkSettings & serialize_settings,
    const Granule & granule)
{
    name_and_type.type->serializeBinaryBulkWithMultipleStreams(column, granule.start_row, granule.rows_to_write, serialize_settings, serialization_state);

    /// So that instead of the marks pointing to the end of the compressed block, there were marks pointing to the beginning of the next one.
    name_and_type.type->enumerateStreams([&] (const IDataType::SubstreamPath & substream_path, const IDataType & /* substream_type */)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;

        String stream_name = IDataType::getFileNameForStream(name_and_type, substream_path);

        /// Don't write offsets more than one time for Nested type.
        if (is_offsets && offset_columns.count(stream_name))
            return;

        column_streams[stream_name]->compressed.nextIfAtEnd();
    }, serialize_settings.path);
}

/// Column must not be empty. (column.size() !== 0)
void MergeTreeDataPartWriterWide::writeColumn(
    const NameAndTypePair & name_and_type,
    const IColumn & column,
    WrittenOffsetColumns & offset_columns,
    const Granules & granules)
{
    if (granules.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty granules for column {}, current mark {}", backQuoteIfNeed(name_and_type.name), getCurrentMark());

    const auto & [name, type] = name_and_type;
    auto [it, inserted] = serialization_states.emplace(name, nullptr);

    if (inserted)
    {
        IDataType::SerializeBinaryBulkSettings serialize_settings;
        serialize_settings.getter = createStreamGetter(name_and_type, offset_columns);
        type->serializeBinaryBulkStatePrefix(serialize_settings, it->second);
    }

    const auto & global_settings = storage.global_context.getSettingsRef();
    IDataType::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.getter = createStreamGetter(name_and_type, offset_columns);
    serialize_settings.low_cardinality_max_dictionary_size = global_settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = global_settings.low_cardinality_use_single_dictionary_for_part != 0;

    for (const auto & granule : granules)
    {
        data_written = true;

        if (granule.mark_on_start)
        {
            if (last_non_written_marks.count(name))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "We have to add new mark for column, but already have non written mark. Current mark {}, total marks {}, offset {}", getCurrentMark(), index_granularity.getMarksCount(), rows_written_in_last_mark);
            last_non_written_marks[name] = getCurrentMarksForColumn(name_and_type, offset_columns, serialize_settings.path);
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

    name_and_type.type->enumerateStreams([&] (const IDataType::SubstreamPath & substream_path, const IDataType & /* substream_type */)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;
        if (is_offsets)
        {
            String stream_name = IDataType::getFileNameForStream(name_and_type, substream_path);
            offset_columns.insert(stream_name);
        }
    }, serialize_settings.path);
}


void MergeTreeDataPartWriterWide::validateColumnOfFixedSize(const String & name, const IDataType & type)
{
    if (!type.isValueRepresentedByNumber() || type.haveSubtypes())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot validate column of non fixed type {}", type.getName());

    auto disk = data_part->volume->getDisk();
    String mrk_path = fullPath(disk, part_path + name + marks_file_extension);
    String bin_path = fullPath(disk, part_path + name + DATA_FILE_EXTENSION);
    DB::ReadBufferFromFile mrk_in(mrk_path);
    DB::CompressedReadBufferFromFile bin_in(bin_path, 0, 0, 0);
    bool must_be_last = false;
    UInt64 offset_in_compressed_file = 0;
    UInt64 offset_in_decompressed_block = 0;
    UInt64 index_granularity_rows = data_part->index_granularity_info.fixed_index_granularity;

    size_t mark_num;

    for (mark_num = 0; !mrk_in.eof(); ++mark_num)
    {
        if (mark_num > index_granularity.getMarksCount())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect number of marks in memory {}, on disk (at least) {}", index_granularity.getMarksCount(), mark_num + 1);

        DB::readBinary(offset_in_compressed_file, mrk_in);
        DB::readBinary(offset_in_decompressed_block, mrk_in);
        if (settings.can_use_adaptive_granularity)
            DB::readBinary(index_granularity_rows, mrk_in);
        else
            index_granularity_rows = data_part->index_granularity_info.fixed_index_granularity;

        if (must_be_last)
        {
            if (index_granularity_rows != 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "We ran out of binary data but still have non empty mark #{} with rows number {}", mark_num, index_granularity_rows);

            if (!mrk_in.eof())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Mark #{} must be last, but we still have some to read", mark_num);

            break;
        }

        if (index_granularity_rows == 0)
        {
            auto column = type.createColumn();

            type.deserializeBinaryBulk(*column, bin_in, 1000000000, 0.0);

            throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Still have {} rows in bin stream, last mark #{} index granularity size {}, last rows {}", column->size(), mark_num, index_granularity.getMarksCount(), index_granularity_rows);
        }

        if (index_granularity_rows > data_part->index_granularity_info.fixed_index_granularity)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Mark #{} has {} rows, but max fixed granularity is {}, index granularity size {}",
                            mark_num, index_granularity_rows, data_part->index_granularity_info.fixed_index_granularity, index_granularity.getMarksCount());
        }

        if (index_granularity_rows != index_granularity.getMarkRows(mark_num))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Incorrect mark rows for part {} for mark #{} (compressed offset {}, decompressed offset {}), in-memory {}, on disk {}, total marks {}",
                data_part->getFullPath(), mark_num, offset_in_compressed_file, offset_in_decompressed_block, index_granularity.getMarkRows(mark_num), index_granularity_rows, index_granularity.getMarksCount());

        auto column = type.createColumn();

        type.deserializeBinaryBulk(*column, bin_in, index_granularity_rows, 0.0);

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
                ErrorCodes::LOGICAL_ERROR, "Incorrect mark rows for mark #{} (compressed offset {}, decompressed offset {}), actually in bin file {}, in mrk file {}, total marks {}",
                mark_num, offset_in_compressed_file, offset_in_decompressed_block, column->size(), index_granularity.getMarkRows(mark_num), index_granularity.getMarksCount());
        }
    }

    if (!mrk_in.eof())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Still have something in marks stream, last mark #{} index granularity size {}, last rows {}", mark_num, index_granularity.getMarksCount(), index_granularity_rows);
    if (!bin_in.eof())
    {
        auto column = type.createColumn();

        type.deserializeBinaryBulk(*column, bin_in, 1000000000, 0.0);

        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Still have {} rows in bin stream, last mark #{} index granularity size {}, last rows {}", column->size(), mark_num, index_granularity.getMarksCount(), index_granularity_rows);
    }

}

void MergeTreeDataPartWriterWide::finishDataSerialization(IMergeTreeDataPart::Checksums & checksums, bool sync)
{
    const auto & global_settings = storage.global_context.getSettingsRef();
    IDataType::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.low_cardinality_max_dictionary_size = global_settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = global_settings.low_cardinality_use_single_dictionary_for_part != 0;
    WrittenOffsetColumns offset_columns;
    if (rows_written_in_last_mark > 0)
    {
        if (settings.can_use_adaptive_granularity && settings.blocks_are_granules_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Incomplete granule is not allowed while blocks are granules size even for last granule. "
                "Mark number {} (rows {}), rows written for last mark {}, total marks {}",
                getCurrentMark(), index_granularity.getMarkRows(getCurrentMark()), rows_written_in_last_mark, index_granularity.getMarksCount());

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
                it->type->serializeBinaryBulkStateSuffix(serialize_settings, serialization_states[it->name]);
            }

            if (write_final_mark)
                writeFinalMark(*it, offset_columns, serialize_settings.path);
        }
    }
    for (auto & stream : column_streams)
    {
        stream.second->finalize();
        stream.second->addToChecksums(checksums);
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
        if (column.type->isValueRepresentedByNumber() && !column.type->haveSubtypes())
            validateColumnOfFixedSize(column.name, *column.type);
    }
#endif

}

void MergeTreeDataPartWriterWide::finish(IMergeTreeDataPart::Checksums & checksums, bool sync)
{
    finishDataSerialization(checksums, sync);
    if (settings.rewrite_primary_key)
        finishPrimaryIndexSerialization(checksums, sync);

    finishSkipIndicesSerialization(checksums, sync);
}

void MergeTreeDataPartWriterWide::writeFinalMark(
    const NameAndTypePair & column,
    WrittenOffsetColumns & offset_columns,
    DB::IDataType::SubstreamPath & path)
{
    writeSingleMark(column, offset_columns, 0, path);
    /// Memoize information about offsets
    column.type->enumerateStreams([&] (const IDataType::SubstreamPath & substream_path, const IDataType & /* substream_type */)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;
        if (is_offsets)
        {
            String stream_name = IDataType::getFileNameForStream(column, substream_path);
            offset_columns.insert(stream_name);
        }
    }, path);
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to add marks, while current mark {}, but total marks {}", getCurrentMark(), index_granularity.getMarksCount());

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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Non last mark {} (with {} rows) having rows offset {}, total marks {}",
                            getCurrentMark(), index_granularity.getMarkRows(getCurrentMark()), rows_written_in_last_mark, index_granularity.getMarksCount());

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
