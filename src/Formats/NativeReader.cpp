#include <Core/Defines.h>
#include <Core/ProtocolDefines.h>

#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <Compression/CompressedReadBufferFromFile.h>

#include <DataTypes/DataTypeFactory.h>
#include <Columns/ColumnLazy.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <base/range.h>
#include <Common/typeid_cast.h>

#include <Formats/NativeReader.h>
#include <Formats/FormatSettings.h>
#include <Formats/insertNullAsDefaultIfNeeded.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <DataTypes/DataTypeAggregateFunction.h>

#include <Interpreters/castColumn.h>

#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_INDEX;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int INCORRECT_DATA;
    extern const int TOO_LARGE_ARRAY_SIZE;
}


NativeReader::NativeReader(
    ReadBuffer & istr_,
    UInt64 server_revision_,
    std::optional<FormatSettings> format_settings_,
    ISerialization::KindSet allowed_kinds_,
    bool with_defaults_masks_)
    : istr(istr_)
    , server_revision(server_revision_)
    , format_settings(format_settings_ ? std::make_shared<const FormatSettings>(std::move(*format_settings_)) : nullptr)
    , allowed_kinds(allowed_kinds_)
    , with_defaults_masks(with_defaults_masks_)
{
}

NativeReader::NativeReader(ReadBuffer & istr_, UInt64 server_revision_)
    : NativeReader(istr_, server_revision_, std::nullopt)
{
}

NativeReader::NativeReader(ReadBuffer & istr_, const Block & header_, UInt64 server_revision_)
    : NativeReader(istr_, header_, server_revision_, std::nullopt)
{
}

NativeReader::NativeReader(
    ReadBuffer & istr_,
    const Block & header_,
    UInt64 server_revision_,
    std::optional<FormatSettings> format_settings_,
    BlockMissingValues * block_missing_values_,
    NativeReaderTypeCache * type_cache_,
    ISerialization::KindSet allowed_kinds_,
    bool with_defaults_masks_)
    : istr(istr_)
    , header(header_)
    , server_revision(server_revision_)
    , format_settings(format_settings_ ? std::make_shared<const FormatSettings>(std::move(*format_settings_)) : nullptr)
    , block_missing_values(block_missing_values_)
    , type_cache(type_cache_)
    , allowed_kinds(allowed_kinds_)
    , with_defaults_masks(with_defaults_masks_)
{
}

NativeReader::NativeReader(ReadBuffer & istr_, UInt64 server_revision_,
    IndexForNativeFormat::Blocks::const_iterator index_block_it_,
    IndexForNativeFormat::Blocks::const_iterator index_block_end_)
    : istr(istr_), server_revision(server_revision_),
    use_index(true), index_block_it(index_block_it_), index_block_end(index_block_end_)
{
    istr_concrete = typeid_cast<CompressedReadBufferFromFile *>(&istr);
    if (!istr_concrete)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "When need to use index for NativeReader, istr must be CompressedReadBufferFromFile.");

    if (index_block_it == index_block_end)
        return;

    index_column_it = index_block_it->columns.begin();

    /// Initialize header from the index.
    for (const auto & column : index_block_it->columns)
    {
        auto type = DataTypeFactory::instance().get(column.type);
        header.insert(ColumnWithTypeAndName{ type, column.name });
    }
}

void NativeReader::resetParser()
{
    istr_concrete = nullptr;
    use_index = false;
}

void NativeReader::readData(
    const ISerialization & serialization,
    IColumn & column,
    ReadBuffer & istr,
    const FormatSettings * format_settings,
    size_t rows,
    const NameAndTypePair * name_and_type,
    ValueSizeMap * avg_value_size_hints_)
{
    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &istr; };
    settings.position_independent_encoding = false;
    settings.native_format = true;
    settings.format_settings = format_settings;

    if (name_and_type != nullptr && avg_value_size_hints_ != nullptr)
    {
        settings.get_avg_value_size_hint_callback = [&](const ISerialization::SubstreamPath & substream_path) -> double
        {
            auto stream_name = ISerialization::getFileNameForStream(*name_and_type, substream_path, {});
            return (*avg_value_size_hints_)[stream_name];
        };

        settings.update_avg_value_size_hint_callback = [&](const ISerialization::SubstreamPath & substream_path, const IColumn & column_)
        {
            auto stream_name = ISerialization::getFileNameForStream(*name_and_type, substream_path, {});
            IDataType::updateAvgValueSizeHint(column_, (*avg_value_size_hints_)[stream_name]);
        };
    }

    ISerialization::DeserializeBinaryBulkStatePtr state;

    serialization.deserializeBinaryBulkStatePrefix(settings, state, nullptr);
    serialization.deserializeBinaryBulkWithMultipleStreams(column, rows, settings, state, nullptr);

    if (column.size() != rows)
        throw Exception(
            ErrorCodes::CANNOT_READ_ALL_DATA,
            "Cannot read all data in NativeReader. Rows read: {}. Rows expected: {}",
            column.size(),
            rows);
}

/// Reads the marker byte and, if it is 1, the defaults mask of a column.
/// Returns the number of rows that take the default, and leaves the mask empty if there are none.
static size_t readDefaultsMask(ReadBuffer & istr, const String & column_name, size_t rows, IColumn::Filter & defaults_mask)
{
    UInt8 has_defaults_mask = 0;
    readBinary(has_defaults_mask, istr);
    if (has_defaults_mask == 0)
        return 0;

    if (has_defaults_mask != 1)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Unexpected defaults mask marker {} of column {}, expected 0 or 1",
            static_cast<UInt32>(has_defaults_mask),
            column_name);

    /// `rows` comes from the block header, so the mask grows only as its bytes arrive.
    while (defaults_mask.size() < rows)
    {
        const size_t offset = defaults_mask.size();
        const size_t size = std::min<size_t>(rows - offset, DEFAULT_INSERT_BLOCK_SIZE);
        defaults_mask.resize(offset + size);
        istr.readStrict(reinterpret_cast<char *>(defaults_mask.data() + offset), size);
    }

    const size_t num_defaults = countBytesInFilter(defaults_mask);
    if (num_defaults == 0)
        defaults_mask.clear();
    return num_defaults;
}

/// `column` has the rows where `defaults_mask` is zero, or all `rows` rows if the mask is empty.
/// Removes the rows that are NULL from it and sets them in the mask instead.
static void moveNullsToDefaultsMask(ColumnWithTypeAndName & column, size_t rows, IColumn::Filter & defaults_mask)
{
    const IColumn & values = *column.column;
    IColumn::Filter not_null(values.size());
    for (size_t i = 0; i < values.size(); ++i)
        not_null[i] = !values.isNullAt(i);

    const size_t num_not_null = countBytesInFilter(not_null);
    if (num_not_null == values.size())
        return;

    if (defaults_mask.empty())
        defaults_mask.resize_fill(rows, 0);

    size_t values_row = 0;
    for (auto & is_default : defaults_mask)
    {
        if (is_default)
            continue;
        is_default = !not_null[values_row];
        ++values_row;
    }

    column.column = values.filter(not_null, num_not_null);
}

/// `values` has the rows where `defaults_mask` is zero. Returns all rows, with the default of `type` where the mask is set.
static ColumnPtr expandByDefaultsMask(const IColumn & values, const IDataType & type, const IColumn::Filter & defaults_mask)
{
    auto res = values.cloneEmpty();
    res->reserve(defaults_mask.size());

    size_t values_row = 0;
    for (size_t row = 0; row < defaults_mask.size();)
    {
        const bool is_default = defaults_mask[row] != 0;
        size_t end = row + 1;
        while (end < defaults_mask.size() && (defaults_mask[end] != 0) == is_default)
            ++end;

        if (!is_default)
        {
            res->insertRangeFrom(values, values_row, end - row);
            values_row += end - row;
        }
        else if (type.isDefaultInsertTrivial())
            res->insertManyDefaults(end - row);
        else
            type.insertManyDefaultsInto(*res, end - row);

        row = end;
    }

    return res;
}


Block NativeReader::getHeader() const
{
    return header;
}

DataTypePtr NativeReader::getTypeByName(const String & type_name)
{
    if (type_cache)
    {
        if (auto it = type_cache->find(type_name); it != type_cache->end())
            return it->second;
    }

    auto type = DataTypeFactory::instance().get(type_name);
    setVersionToAggregateFunctions(type, true, server_revision);
    if (type_cache)
        type_cache->emplace(type_name, type);
    return type;
}

Block NativeReader::read()
{
    Block res;

    if (use_index && index_block_it == index_block_end)
        return res;

    if (istr.eof())
    {
        if (use_index)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Input doesn't contain all data for index.");

        return res;
    }

    /// Additional information about the block.
    if (server_revision > 0)
        res.info.read(istr, server_revision);

    /// Dimensions
    size_t columns = 0;
    size_t rows = 0;

    if (!use_index)
    {
        readVarUInt(columns, istr);
        readVarUInt(rows, istr);

        if (columns > DEFAULT_NATIVE_BINARY_MAX_NUM_COLUMNS)
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Suspiciously many columns in Native format: {}", columns);
        if (rows > DEFAULT_NATIVE_BINARY_MAX_NUM_ROWS)
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Suspiciously many rows in Native format: {}", rows);
    }
    else
    {
        columns = index_block_it->num_columns;
        rows = index_block_it->num_rows;
    }

    if (columns == 0 && header.empty() && rows != 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Zero columns but {} rows in Native format.", rows);

    /// `rows` comes from the block header, and the limit it is checked against is deliberately
    /// generous, so it must not be used to preallocate the columns: a header declaring a huge row
    /// count would reserve that much per column before a single byte of column data is read.
    /// Reserving is only an optimization here - deserialization appends to the column anyway - so
    /// bound it by a plausible block size and let the column grow past that on its own.
    const size_t rows_to_reserve = std::min<size_t>(rows, DEFAULT_INSERT_BLOCK_SIZE);

    for (size_t i = 0; i < columns; ++i)
    {
        if (use_index)
        {
            /// If the current position is what is required, the real seek does not occur.
            istr_concrete->seek(index_column_it->location.offset_in_compressed_file, index_column_it->location.offset_in_decompressed_block);
        }

        ColumnWithTypeAndName column;

        /// Name
        readStringBinaryGrowing(column.name, istr);

        /// Type
        String type_name;
        if (format_settings && format_settings->native.decode_types_in_binary_format)
        {
            column.type = decodeDataType(istr, format_settings->binary.max_binary_type_complexity);
            type_name = column.type->getName();
            setVersionToAggregateFunctions(column.type, true, server_revision);
        }
        else
        {
            readStringBinaryGrowing(type_name, istr);
            column.type = getTypeByName(type_name);
        }

        SerializationPtr serialization;
        MutableColumnPtr read_column;

        if (server_revision >= DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION)
        {
            /// NativeReader must enable all supported serializations (e.g. nullable sparse) here. Since it operates on
            /// in-memory state, it should be able to handle all possible serialization variants.
            auto info = column.type->createSerializationInfo(SerializationInfoSettings::enableAllSupportedSerializations(
                server_revision >= DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION));

            UInt8 has_custom = 0;
            readBinary(has_custom, istr);
            if (has_custom)
                info->deserializeFromKindsBinary(istr, allowed_kinds);

            serialization = column.type->getSerialization(*info);
            auto new_column = column.type->createColumn(*serialization);
            new_column->reserve(rows_to_reserve);
            read_column = std::move(new_column);
        }
        else
        {
            serialization = column.type->getDefaultSerialization();
            auto new_column = column.type->createColumn(*serialization);
            new_column->reserve(rows_to_reserve);
            read_column = std::move(new_column);
        }

        if (use_index)
        {
            /// Index allows to do more checks.
            if (index_column_it->name != column.name)
                throw Exception(ErrorCodes::INCORRECT_INDEX, "Index points to a column with a wrong name ({} instead of {}): corrupted index or data", index_column_it->name, column.name);
            /// Note: we can't compare data types as strings, compatible data types may differ in parameters.
        }

        /// Empty if no row of the column takes the default.
        IColumn::Filter defaults_mask;
        size_t rows_with_values = rows;
        if (with_defaults_masks)
            rows_with_values -= readDefaultsMask(istr, column.name, rows, defaults_mask);

        /// If no rows, nothing to read.
        if (rows_with_values)
        {
            const auto * format = format_settings.get();
            NameAndTypePair name_and_type = {column.name, column.type};
            readData(*serialization, *read_column, istr, format, rows_with_values, &name_and_type, &avg_value_size_hints);
        }

        column.column = std::move(read_column);

        bool use_in_result = true;
        std::optional<size_t> header_position;
        if (!header.empty())
        {
            if (header.has(column.name))
            {
                auto & header_column = header.getByName(column.name);
                header_position = header.getPositionByName(column.name);

                if (format_settings && format_settings->null_as_default)
                {
                    if (with_defaults_masks)
                    {
                        /// A NULL that the target column cannot hold takes the default like a row of the defaults mask:
                        /// it is not converted, and gets the default of the target type. The rows of the mask are marked
                        /// as missing after the expansion below.
                        if (isNullableOrLowCardinalityNullable(column.type) && !isNullableOrLowCardinalityNullable(header_column.type))
                            moveNullsToDefaultsMask(column, rows, defaults_mask);
                        insertNullAsDefaultIfNeeded(column, header_column, *header_position, nullptr);
                    }
                    else
                        insertNullAsDefaultIfNeeded(column, header_column, *header_position, block_missing_values);
                }

                if (!header_column.type->equals(*column.type))
                {
                    /// In the event of the same aggregate function but of a different variant (e.g. Window vs Aggregate),
                    /// we should try to convert and read, since the difference in `Window` vs `Aggregate` is not a
                    /// user-facing type difference but rather an internal implementation detail.
                    /// This can happen when external sort spills blocks to disk: the header carries the Window variant from the query plan,
                    /// but `NativeReader` deserializes the type name and resolves it via `AggregateFunctionFactory`, which always produces the
                    /// Aggregation variant.
                    const auto * header_agg_type = typeid_cast<const DataTypeAggregateFunction *>(header_column.type.get());
                    bool convertible_agg_variant = header_agg_type && header_agg_type->equalsIgnoringVariant(*column.type);

                    if ((format_settings && format_settings->native.allow_types_conversion) || convertible_agg_variant)
                    {
                        try
                        {
                            column.column = castColumn(column, header_column.type);
                        }
                        catch (Exception & e)
                        {
                            e.addMessage(fmt::format(
                                "while converting column \"{}\" from type {} to type {}",
                                column.name,
                                column.type->getName(),
                                header_column.type->getName()));
                            throw;
                        }
                    }
                    else
                    {
                        /// Support insert from old clients without low cardinality type.
                        column.column = recursiveLowCardinalityTypeConversion(column.column, column.type, header_column.type);
                    }

                    column.type = header_column.type;
                }
                else if (!haveSameAggregateStateVersions(*header_column.type, *column.type))
                {
                    /// `equals` ignores the state version of `AggregateFunction` types, e.g. a version `0` state
                    /// from an older writer read into a `AggregateFunction(1, uniq, UInt64)` header, where the query
                    /// expects the version of the header. So relabel the column with the header type.
                    column.column = relabelAggregateStateVersions(column.column, header_column.type);
                    column.type = header_column.type;
                }
            }
            else
            {
                if (format_settings && !format_settings->skip_unknown_fields)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown column with name {} found while reading data in Native format", column.name);
                use_in_result = false;
            }
        }

        if (use_in_result && !defaults_mask.empty())
        {
            /// Expanded only after the conversion to the type of the header: the rows that take the default have no value
            /// to convert, and must get the default of the target type rather than a converted default of the source type.
            column.column = expandByDefaultsMask(*column.column, *column.type, defaults_mask);

            if (block_missing_values && header_position)
                block_missing_values->setBitsFromNullMap(*header_position, defaults_mask);
        }

        if (use_in_result)
            res.insert(std::move(column));

        if (use_index)
            ++index_column_it;
    }

    if (use_index)
    {
        if (index_column_it != index_block_it->columns.end())
            throw Exception(ErrorCodes::INCORRECT_INDEX, "Inconsistent index: not all columns were read");

        ++index_block_it;
        if (index_block_it != index_block_end)
            index_column_it = index_block_it->columns.begin();
    }

    if (rows && !header.empty())
    {
        /// Allow to skip columns. We will fill them with default values later.
        Block tmp_res;

        for (size_t column_i = 0; column_i != header.columns(); ++column_i)
        {
            auto & col = header.getByPosition(column_i);
            if (res.has(col.name))
            {
                tmp_res.insert(res.getByName(col.name));
            }
            else
            {
                MutableColumnPtr column;
                if (col.type->isDefaultInsertTrivial())
                    column = col.type->createColumn()->cloneResized(rows);
                else
                {
                    column = col.type->createColumn();
                    col.type->insertManyDefaultsInto(*column, rows);
                }
                tmp_res.insert({std::move(column), col.type, col.name});
                if (block_missing_values)
                    block_missing_values->setBits(column_i, rows);
            }
        }
        tmp_res.info = res.info;

        res.swap(tmp_res);
    }

    if (res.rows() != rows)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Row count mismatch after deserialization, got: {}, expected: {}", res.rows(), rows);

    return res;
}

}
