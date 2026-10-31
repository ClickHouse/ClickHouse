#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/Serializations/SerializationCompactNullMap.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/Serializations/SerializationTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NullableUtils.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Core/Field.h>
#include <Formats/FormatSettings.h>
#include <Formats/ParseError.h>

#include <optional>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/PeekableReadBuffer.h>
#include <Common/assert_cast.h>
#include <base/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

UInt128 SerializationNullable::getHash(const SerializationPtr & nested_, bool use_default_null_map_, bool compact_null_map_)
{
    SipHash hash;
    hash.update("Nullable");
    hash.update(nested_->getHash());
    hash.update(use_default_null_map_);
    hash.update(compact_null_map_);
    return hash.get128();
}

namespace
{

/// The state of the compact null map and the state of the nested serialization.
struct DeserializeBinaryBulkStateNullableWithCompactNullMap : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr null_map_state;
    ISerialization::DeserializeBinaryBulkStatePtr nested_state;

    ISerialization::DeserializeBinaryBulkStatePtr cloneImpl(ISerialization::DeserializeBinaryBulkStateCloneContext & context) const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateNullableWithCompactNullMap>();
        new_state->null_map_state = null_map_state ? null_map_state->clone(context) : nullptr;
        new_state->nested_state = nested_state ? nested_state->clone(context) : nullptr;
        return new_state;
    }
};

}

void SerializationNullable::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    const auto * type_nullable = data.type ? &assert_cast<const DataTypeNullable &>(*data.type) : nullptr;
    const auto * column_nullable = data.column ? &assert_cast<const ColumnNullable &>(*data.column) : nullptr;
    /// nullptr if the column has no null map, which means it has no NULLs.
    auto column_null_map = column_nullable ? column_nullable->tryGetNullMapColumnPtr() : nullptr;

    if (!use_default_null_map)
    {
        auto null_map_serialization = SerializationNamed::create(
            compact_null_map ? SerializationCompactNullMap::create() : SerializationNumber<UInt8>::create(), "null", SubstreamType::NamedNullMap);

        bool hide_null_map_subcolumn = type_nullable ? type_nullable->getNestedType()->hasSubcolumn("null") : false;
        settings.path.push_back(hide_null_map_subcolumn ? Substream::NullMapHidden : Substream::NullMap);
        auto null_map_data = SubstreamData(null_map_serialization)
                                 .withType(type_nullable ? std::make_shared<DataTypeUInt8>() : nullptr)
                                 .withColumn(column_null_map)
                                 .withSerializationInfo(data.serialization_info);

        /// The null map subcolumn is provided even if the column has no null map.
        /// The zero null map is created only if the subcolumn is actually requested.
        /// Do not create the null map in the column itself, because the column may be shared.
        if (column_nullable && !column_null_map)
        {
            null_map_data.withLazyColumnCreator([size = column_nullable->size()]() -> ColumnPtr
            {
                return ColumnUInt8::create(size, static_cast<UInt8>(0));
            });
        }

        settings.path.back().data = null_map_data;
        callback(settings.path);
        settings.path.pop_back();
    }

    settings.path.push_back(Substream::NullableElements);
    if (type_nullable && type_nullable->getNestedType()->canBeInsideNullable())
        settings.path.back().creator = std::make_shared<NullableSubcolumnCreator>(column_null_map, compact_null_map);
    settings.path.back().data = data;

    auto next_data = SubstreamData(nested)
                         .withType(type_nullable ? type_nullable->getNestedType() : nullptr)
                         .withColumn(column_nullable ? column_nullable->getNestedColumnPtr() : nullptr)
                         .withSerializationInfo(data.serialization_info);

    nested->enumerateStreams(settings, callback, next_data);
    settings.path.pop_back();
}

void SerializationNullable::serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::NullableElements);
    const auto & column_nullable = assert_cast<const ColumnNullable &>(column);
    nested->serializeBinaryBulkStatePrefix(column_nullable.getNestedColumn(), settings, state);
    settings.path.pop_back();
}


void SerializationNullable::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::NullableElements);
    nested->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}


void SerializationNullable::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    if (!compact_null_map)
    {
        settings.path.push_back(Substream::NullableElements);
        nested->deserializeBinaryBulkStatePrefix(settings, state, cache);
        settings.path.pop_back();
        return;
    }

    auto nullable_state = std::make_shared<DeserializeBinaryBulkStateNullableWithCompactNullMap>();

    settings.path.push_back(Substream::NullMap);
    nullable_state->null_map_state = SerializationCompactNullMap::deserializeStatePrefix(settings, cache);
    settings.path.pop_back();

    settings.path.push_back(Substream::NullableElements);
    nested->deserializeBinaryBulkStatePrefix(settings, nullable_state->nested_state, cache);
    settings.path.pop_back();

    state = std::move(nullable_state);
}


namespace
{

void writeZeros(WriteBuffer & ostr, size_t size)
{
    static constexpr char zeros[4096]{};
    while (size)
    {
        size_t chunk = std::min(size, sizeof(zeros));
        ostr.write(zeros, chunk);
        size -= chunk;
    }
}

}

void SerializationNullable::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);
    col.checkConsistency();

    if (!use_default_null_map)
    {
        /// First serialize null map.
        settings.path.push_back(Substream::NullMap);
        if (auto * stream = settings.getter(settings.path))
        {
            if (compact_null_map)
            {
                if (const size_t size = col.size(); limit == 0 || offset + limit > size)
                    limit = size - offset;
                SerializationCompactNullMap::serializeGranule(col.tryGetNullMapData(), offset, limit, *stream);
            }
            else if (const auto * null_map = col.tryGetNullMapData())
            {
                SerializationNumber<UInt8>::serializeBinaryBulk(*null_map, *stream, offset, limit);
            }
            else
            {
                /// The column has no NULLs, write the zero-filled null map without creating it.
                if (const size_t size = col.size(); limit == 0 || offset + limit > size)
                    limit = size - offset;
                writeZeros(*stream, limit);
            }
        }
        settings.path.pop_back();
    }

    /// Then serialize contents of elements.
    settings.path.push_back(Substream::NullableElements);
    nested->serializeBinaryBulkWithMultipleStreams(col.getNestedColumn(), offset, limit, settings, state);
    settings.path.pop_back();
}


void SerializationNullable::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);

    DeserializeBinaryBulkStateNullableWithCompactNullMap * nullable_state = nullptr;
    if (compact_null_map)
        nullable_state = checkAndGetState<DeserializeBinaryBulkStateNullableWithCompactNullMap>(state);
    DeserializeBinaryBulkStatePtr & nested_state = nullable_state ? nullable_state->nested_state : state;

    /// Read into a column without a null map while the data has no NULLs.
    if (col.empty())
        col.dropNullMapIfNoNulls();

    /// The number of rows read into the null map, if the column has no null map.
    size_t num_rows_without_null_map = 0;

    /// Append the read rows of the null map to the column. `null_map` is nullptr if they have no NULLs.
    auto append_null_map = [&](size_t num_rows, ColumnUInt8::MutablePtr null_map)
    {
        if (null_map && memoryIsZero(null_map->getData().data(), 0, null_map->size()))
            null_map = nullptr;

        if (!null_map)
        {
            if (col.hasNullMap())
                col.getNullMapData().resize_fill(col.getNullMapData().size() + num_rows, 0);
            else
                num_rows_without_null_map += num_rows;
        }
        else if (col.empty())
        {
            col.getNullMapColumnPtr() = std::move(null_map);
        }
        else
        {
            col.materializeNullMap();
            col.getNullMapColumn().insertRangeFrom(*null_map, 0, num_rows);
        }
    };

    if (!use_default_null_map)
    {
        settings.path.push_back(Substream::NullMap);
        if (auto cached = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
        {
            /// Data was read by another reader of this stream.
            const auto & [cached_column, num_rows] = *cached;
            auto null_map = ColumnUInt8::create();
            null_map->insertRangeFrom(*cached_column, cached_column->size() - num_rows, num_rows);
            append_null_map(num_rows, std::move(null_map));
        }
        else if (auto * stream = settings.getter(settings.path))
        {
            ColumnUInt8::MutablePtr null_map;
            size_t num_rows = 0;
            if (compact_null_map)
            {
                auto & null_map_state = *checkAndGetState<SerializationCompactNullMap::DeserializeBinaryBulkStateCompactNullMap>(
                    nullable_state->null_map_state);
                std::tie(num_rows, null_map) = SerializationCompactNullMap::deserializeRows(null_map_state, *stream, limit, settings.continuous_reading);
            }
            else
            {
                null_map = ColumnUInt8::create();
                SerializationNumber<UInt8>::create()->deserializeBinaryBulk(*null_map, *stream, limit, 0);
                num_rows = null_map->size();
            }

            if (cache)
            {
                /// The other readers of this stream expect the read rows of the null map.
                ColumnPtr cached_null_map = null_map ? ColumnPtr(null_map->getPtr()) : ColumnPtr(ColumnUInt8::create(num_rows, static_cast<UInt8>(0)));
                addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, cached_null_map, num_rows);
            }

            append_null_map(num_rows, std::move(null_map));
        }
        settings.path.pop_back();
    }

    /// Do not create the null map, the rows inserted into the nested column of a column without a null map are not NULL.
    IColumn & nested_column = col.getNestedColumnNoMaterialize();
    const size_t prev_nested_size = nested_column.size();

    settings.path.push_back(Substream::NullableElements);
    nested->deserializeBinaryBulkWithMultipleStreams(nested_column, limit, settings, nested_state, cache);
    settings.path.pop_back();

    if (use_default_null_map && col.hasNullMap())
        col.getNullMapData().resize_fill(nested_column.size());

    size_t null_map_size = 0;
    if (const auto * null_map = col.tryGetNullMapColumn())
        null_map_size = null_map->size();
    else if (!use_default_null_map)
        null_map_size = prev_nested_size + num_rows_without_null_map;
    else
        null_map_size = nested_column.size();

    if (null_map_size != nested_column.size())
        throw Exception(
            settings.native_format ? ErrorCodes::INCORRECT_DATA : ErrorCodes::LOGICAL_ERROR,
            "Sizes of nested column and null map of Nullable column are not equal after deserialization (null map size = {}, nested "
            "column size = {})",
            null_map_size,
            nested_column.size());
}


void SerializationNullable::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (field.isNull())
    {
        writeBinary(true, ostr);
    }
    else
    {
        writeBinary(false, ostr);
        nested->serializeBinary(field, ostr, settings);
    }
}

void SerializationNullable::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    bool is_null = false;
    readBinary(is_null, istr);
    if (!is_null)
    {
        nested->deserializeBinary(field, istr, settings);
    }
    else
    {
        field = Null();
    }
}

void SerializationNullable::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    bool is_null = col.isNullAt(row_num);
    writeBinary(is_null, ostr);
    if (!is_null)
        nested->serializeBinary(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::serializeForHashCalculation(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    bool is_null = col.isNullAt(row_num);
    writeBinary(is_null, ostr);
    if (!is_null)
        nested->serializeForHashCalculation(col.getNestedColumn(), row_num, ostr);
}

template <typename ReturnType>
ReturnType safeAppendToNullMap(ColumnNullable & column, bool is_null)
{
    /// The row is already inserted into the nested column.
    /// A column without a null map has no NULLs, so the map is created only on the first NULL.
    if (!column.hasNullMap() && !is_null)
        return ReturnType(true);

    try
    {
        if (column.hasNullMap())
        {
            column.getNullMapData().push_back(is_null);
        }
        else
        {
            /// The created map already has the size of the nested column, including the inserted row.
            column.materializeNullMap();
            column.getNullMapData().back() = 1;
        }
    }
    catch (...)
    {
        column.getNestedColumnNoMaterialize().popBack(1);
        if constexpr (std::is_same_v<ReturnType, void>)
            throw;
        /// Other errors (e.g. MEMORY_LIMIT_EXCEEDED) must propagate, not be reported as a failed parse.
        rethrowIfNotParseError();
        return ReturnType(false);
    }

    return ReturnType(true);
}

/// Deserialize value into non-nullable column. In case of NULL, insert default and set is_null to true.
/// If ReturnType is bool, return true if parsing was successful and false in case of any error.
template <typename ReturnType = void, typename CheckForNull, typename DeserializeNested>
static ReturnType deserializeImpl(IColumn & column, ReadBuffer & buf, CheckForNull && check_for_null, DeserializeNested && deserialize_nested, bool & is_null)
{
    is_null = check_for_null(buf);
    if (is_null)
    {
        column.insertDefault();
    }
    else
    {
        if constexpr (std::is_same_v<ReturnType, void>)
            deserialize_nested(column, buf);
        else if (!deserialize_nested(column, buf))
            return ReturnType(false);
    }

    return ReturnType(true);
}


void SerializationNullable::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null = false;
    auto check_for_null = [](ReadBuffer & buf)
    {
        bool is_null_ = false;
        readBinary(is_null_, buf);
        return is_null_;
    };
    auto deserialize_nested = [this, &settings] (IColumn & nested_column, ReadBuffer & buf) { nested->deserializeBinary(nested_column, buf, settings); };
    deserializeImpl(col.getNestedColumnNoMaterialize(), istr, check_for_null, deserialize_nested, is_null);
    safeAppendToNullMap<void>(col, is_null);
}


void SerializationNullable::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        serializeNullEscaped(ostr, settings);
    else
        nested->serializeTextEscaped(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::serializeNullEscaped(DB::WriteBuffer & ostr, const DB::FormatSettings & settings)
{
    writeString(settings.tsv.null_representation, ostr);
}

bool SerializationNullable::tryDeserializeNullEscaped(DB::ReadBuffer & istr, const DB::FormatSettings & settings)
{
    return checkString(settings.tsv.null_representation, istr);
}

void SerializationNullable::serializeTextRaw(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        serializeNullRaw(ostr, settings);
    else
        nested->serializeTextRaw(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::serializeNullRaw(DB::WriteBuffer & ostr, const DB::FormatSettings & settings)
{
    writeString(settings.tsv.null_representation, ostr);
}

bool SerializationNullable::tryDeserializeNullRaw(DB::ReadBuffer & istr, const DB::FormatSettings & settings)
{
    return checkString(settings.tsv.null_representation, istr);
}

template<typename ReturnType, bool escaped>
ReturnType  deserializeTextEscapedAndRawImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization, bool & is_null)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    const String & null_representation = settings.tsv.null_representation;
    auto deserialize_nested = [&nested_serialization, &settings] (IColumn & nested_column, ReadBuffer & buf_)
    {
        if constexpr (throw_exception)
        {
            if constexpr (escaped)
                nested_serialization->deserializeTextEscaped(nested_column, buf_, settings);
            else
                nested_serialization->deserializeTextRaw(nested_column, buf_, settings);
        }
        else
        {
            if constexpr (escaped)
                return nested_serialization->tryDeserializeTextEscaped(nested_column, buf_, settings);
            else
                return nested_serialization->tryDeserializeTextRaw(nested_column, buf_, settings);
        }
    };

    /// Some data types can deserialize absence of data (e.g. empty string), so eof is ok.
    if (istr.eof() || (!null_representation.empty() && *istr.position() != null_representation[0]))
    {
        /// This is not null, surely.
        return deserializeImpl<ReturnType>(column, istr, [](ReadBuffer &){ return false; }, deserialize_nested, is_null);
    }

    /// Check if we have enough data in buffer to check if it's a null.
    if (istr.available() > null_representation.size())
    {
        auto check_for_null = [&null_representation, &settings](ReadBuffer & buf)
        {
            auto * pos = buf.position();
            if (checkString(null_representation, buf) && (*buf.position() == '\t' || *buf.position() == '\n' || (settings.tsv.crlf_end_of_line_input && *buf.position() == '\r')))
                return true;
            buf.position() = pos;
            return false;
        };
        return deserializeImpl<ReturnType>(column, istr, check_for_null, deserialize_nested, is_null);
    }

    /// We don't have enough data in buffer to check if it's a null.
    /// Use PeekableReadBuffer to make a checkpoint before checking null
    /// representation and rollback if check was failed.
    PeekableReadBuffer peekable_buf(istr);
    auto check_for_null = [&null_representation, &settings](ReadBuffer & buf_)
    {
        auto & buf = assert_cast<PeekableReadBuffer &>(buf_);
        buf.setCheckpoint();
        SCOPE_EXIT(buf.dropCheckpoint());

        if (checkString(null_representation, buf) && (buf.eof() || *buf.position() == '\t' || *buf.position() == '\n' || (settings.tsv.crlf_end_of_line_input && *buf.position() == '\r')))
            return true;
        buf.rollbackToCheckpoint();
        return false;
    };

    auto deserialize_nested_with_check = [&deserialize_nested, &nested_serialization, &settings, &null_representation, &istr] (IColumn & nested_column, ReadBuffer & buf_)
    {
        auto & buf = assert_cast<PeekableReadBuffer &>(buf_);
        auto * pos = buf.position();
        if constexpr (throw_exception)
            deserialize_nested(nested_column, buf);
        else if (!deserialize_nested(nested_column, buf))
            return ReturnType(false);

        /// Check that we don't have any unread data in PeekableReadBuffer own memory.
        if (likely(!buf.hasUnreadData()))
            return ReturnType(true);

        /// We have some unread data in PeekableReadBuffer own memory.
        /// It can happen only if there is a string instead of a number
        /// or if someone uses tab or LF in TSV null_representation.
        /// In the first case we cannot continue reading anyway. The second case seems to be unlikely.
        /// We also should delete incorrectly deserialized value from nested column.
        nested_column.popBack(1);

        if constexpr (!throw_exception)
            return ReturnType(false);

        if (null_representation.contains('\t') || null_representation.contains('\n'))
            throw DB::Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "TSV custom null representation "
                "containing '\\t' or '\\n' may not work correctly for large input.");
        if (settings.tsv.crlf_end_of_line_input && null_representation.contains('\r'))
            throw DB::Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "TSV custom null representation "
                "containing '\\r' may not work correctly for large input.");

        WriteBufferFromOwnString parsed_value;
        if constexpr (escaped)
            nested_serialization->serializeTextEscaped(nested_column, nested_column.size() - 1, parsed_value, settings);
        else
            nested_serialization->serializeTextRaw(nested_column, nested_column.size() - 1, parsed_value, settings);
        throw DB::Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while parsing \"{}{}\" as Nullable"
                                   " at position {}: got \"{}\", which was deserialized as \"{}\". "
                                   "It seems that input data is ill-formatted.",
                                   std::string(pos, buf.buffer().end()),
                                   std::string(istr.position(), std::min(size_t(10), istr.available())),
                                   istr.count(), std::string(pos, buf.position() - pos), parsed_value.str());
    };

    return deserializeImpl<ReturnType>(column, peekable_buf, check_for_null, deserialize_nested_with_check, is_null);
}

void SerializationNullable::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null = false;
    deserializeTextEscapedAndRawImpl<void, true>(col.getNestedColumnNoMaterialize(), istr, settings, nested, is_null);
    safeAppendToNullMap<void>(col, is_null);
}

bool SerializationNullable::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null = false;
    return deserializeTextEscapedAndRawImpl<bool, true>(col.getNestedColumnNoMaterialize(), istr, settings, nested, is_null) && safeAppendToNullMap<bool>(col, is_null);
}

bool SerializationNullable::deserializeNullAsDefaultOrNestedTextEscaped(IColumn & nested_column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization)
{
    bool is_null = false;
    deserializeTextEscapedAndRawImpl<void, true>(nested_column, istr, settings, nested_serialization, is_null);
    return !is_null;
}

bool SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextEscaped(IColumn & nested_column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization)
{
    bool is_null = false;
    return deserializeTextEscapedAndRawImpl<bool, true>(nested_column, istr, settings, nested_serialization, is_null);
}

void SerializationNullable::deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null = false;
    deserializeTextEscapedAndRawImpl<void, false>(col.getNestedColumnNoMaterialize(), istr, settings, nested, is_null);
    safeAppendToNullMap<void>(col, is_null);
}

bool SerializationNullable::tryDeserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null = false;
    return deserializeTextEscapedAndRawImpl<bool, false>(col.getNestedColumnNoMaterialize(), istr, settings, nested, is_null) && safeAppendToNullMap<bool>(col, is_null);
}

bool SerializationNullable::deserializeNullAsDefaultOrNestedTextRaw(IColumn & nested_column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization)
{
    bool is_null = false;
    deserializeTextEscapedAndRawImpl<void, false>(nested_column, istr, settings, nested_serialization, is_null);
    return !is_null;
}

bool SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextRaw(IColumn & nested_column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization)
{
    bool is_null = false;
    return deserializeTextEscapedAndRawImpl<bool, false>(nested_column, istr, settings, nested_serialization, is_null);
}

void SerializationNullable::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        serializeNullQuoted(ostr);
    else
        nested->serializeTextQuoted(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::serializeNullQuoted(DB::WriteBuffer & ostr)
{
    writeCString("NULL", ostr);
}

bool SerializationNullable::tryDeserializeNullQuoted(DB::ReadBuffer & istr)
{
    return checkStringCaseInsensitive("NULL", istr);
}

template<typename ReturnType>
ReturnType deserializeTextQuotedImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested, bool & is_null)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto deserialize_nested = [&nested, &settings] (IColumn & nested_column, ReadBuffer & buf)
    {
        if constexpr (!throw_exception)
            return nested->tryDeserializeTextQuoted(nested_column, buf, settings);
        nested->deserializeTextQuoted(nested_column, buf, settings);
    };

    if (istr.eof() || (*istr.position() != 'N' && *istr.position() != 'n'))
    {
        /// This is not null, surely.
        return deserializeImpl<ReturnType>(column, istr, [](ReadBuffer &){ return false; }, deserialize_nested, is_null);
    }

    /// Check if we have enough data in buffer to check if it's a null.
    if (istr.available() >= 4)
    {
        auto check_for_null = [](ReadBuffer & buf)
        {
            auto * pos = buf.position();
            if (checkStringCaseInsensitive("NULL", buf))
                return true;
            buf.position() = pos;
            return false;
        };
        return deserializeImpl<ReturnType>(column, istr, check_for_null, deserialize_nested, is_null);
    }

    /// We don't have enough data in buffer to check if it's a NULL
    /// and we cannot check it just by one symbol (otherwise we won't be able
    /// to differentiate for example NULL and NaN for float)
    /// Use PeekableReadBuffer to make a checkpoint before checking
    /// null and rollback if the check was failed.
    PeekableReadBuffer peekable_buf(istr);
    auto check_for_null = [](ReadBuffer & buf_)
    {
        auto & buf = assert_cast<PeekableReadBuffer &>(buf_);
        buf.setCheckpoint();
        SCOPE_EXIT(buf.dropCheckpoint());
        if (checkStringCaseInsensitive("NULL", buf))
            return true;

        buf.rollbackToCheckpoint();
        return false;
    };

    auto deserialize_nested_with_check = [&deserialize_nested] (IColumn & nested_column, ReadBuffer & buf_)
    {
        auto & buf = assert_cast<PeekableReadBuffer &>(buf_);

        if constexpr (throw_exception)
            deserialize_nested(nested_column, buf);
        else if (!deserialize_nested(nested_column, buf))
            return false;

        /// Check that we don't have any unread data in PeekableReadBuffer own memory.
        if (likely(!buf.hasUnreadData()))
            return ReturnType(true);

        /// We have some unread data in PeekableReadBuffer own memory.
        /// It can happen only if there is an unquoted string instead of a number.
        /// We also should delete incorrectly deserialized value from nested column.
        nested_column.popBack(1);

        if constexpr (!throw_exception)
            return ReturnType(false);

        throw DB::Exception(
            ErrorCodes::CANNOT_READ_ALL_DATA,
            "Error while parsing Nullable: got an unquoted string {} instead of a number",
            String(buf.position(), std::min(10ul, buf.available())));
    };

    return deserializeImpl<ReturnType>(column, peekable_buf, check_for_null, deserialize_nested_with_check, is_null);
}


void SerializationNullable::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null = false;
    deserializeTextQuotedImpl<void>(col.getNestedColumnNoMaterialize(), istr, settings, nested, is_null);
    safeAppendToNullMap<void>(col, is_null);
}

bool SerializationNullable::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null = false;
    return deserializeTextQuotedImpl<bool>(col.getNestedColumnNoMaterialize(), istr, settings, nested, is_null) && safeAppendToNullMap<bool>(col, is_null);
}

bool SerializationNullable::deserializeNullAsDefaultOrNestedTextQuoted(DB::IColumn & nested_column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, const DB::SerializationPtr & nested_serialization)
{
    bool is_null = false;
    deserializeTextQuotedImpl<void>(nested_column, istr, settings, nested_serialization, is_null);
    return !is_null;
}

bool SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextQuoted(DB::IColumn & nested_column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, const DB::SerializationPtr & nested_serialization)
{
    bool is_null = false;
    return deserializeTextQuotedImpl<bool>(nested_column, istr, settings, nested_serialization, is_null);
}

template <typename ReturnType>
ReturnType deserializeWholeTextImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested, bool & is_null)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    PeekableReadBuffer peekable_buf(istr);
    auto check_for_null = [](ReadBuffer & buf_)
    {
        auto & buf = assert_cast<PeekableReadBuffer &>(buf_);
        buf.setCheckpoint();
        SCOPE_EXIT(buf.dropCheckpoint());

        if (checkStringCaseInsensitive("NULL", buf) && buf.eof())
            return true;

        buf.rollbackToCheckpoint();
        if (checkStringCaseInsensitive("ᴺᵁᴸᴸ", buf) && buf.eof())
            return true;

        buf.rollbackToCheckpoint();
        return false;
    };

    auto deserialize_nested = [&nested, &settings] (IColumn & nested_column, ReadBuffer & buf_)
    {
        auto & buf = assert_cast<PeekableReadBuffer &>(buf_);
        if constexpr (!throw_exception)
            return nested->tryDeserializeWholeText(nested_column, buf, settings);

        nested->deserializeWholeText(nested_column, buf, settings);
        chassert(!buf.hasUnreadData());
    };

    return deserializeImpl<ReturnType>(column, peekable_buf, check_for_null, deserialize_nested, is_null);
}

void SerializationNullable::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null = false;
    deserializeWholeTextImpl<void>(col.getNestedColumnNoMaterialize(), istr, settings, nested, is_null);
    safeAppendToNullMap<void>(col, is_null);
}

bool SerializationNullable::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null = false;
    return deserializeWholeTextImpl<bool>(col.getNestedColumnNoMaterialize(), istr, settings, nested, is_null) && safeAppendToNullMap<bool>(col, is_null);
}

bool SerializationNullable::deserializeNullAsDefaultOrNestedWholeText(DB::IColumn & nested_column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, const DB::SerializationPtr & nested_serialization)
{
    bool is_null = false;
    deserializeWholeTextImpl<void>(nested_column, istr, settings, nested_serialization, is_null);
    return !is_null;
}

bool SerializationNullable::tryDeserializeNullAsDefaultOrNestedWholeText(DB::IColumn & nested_column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, const DB::SerializationPtr & nested_serialization)
{
    bool is_null = false;
    return deserializeWholeTextImpl<bool>(nested_column, istr, settings, nested_serialization, is_null);
}

void SerializationNullable::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
    {
        writeString(settings.csv.null_representation, ostr);
        return;
    }

    /// A NULL value of a Nullable(Tuple) occupies a single \\N field, but a non-null tuple value
    /// serialized into separate columns would occupy several fields, making the per-row width
    /// row-dependent and breaking CSVWithNames(AndTypes). Keep a Nullable(Tuple) as a single quoted
    /// CSV value so every row (NULL or not) is exactly one field (issue #107342).
    if (settings.csv.serialize_tuple_into_separate_columns && typeid_cast<const SerializationTuple *>(nested.get()))
    {
        FormatSettings tuple_as_single_field = settings;
        tuple_as_single_field.csv.serialize_tuple_into_separate_columns = false;
        nested->serializeTextCSV(col.getNestedColumn(), row_num, ostr, tuple_as_single_field);
        return;
    }

    nested->serializeTextCSV(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::serializeNullCSV(DB::WriteBuffer & ostr, const DB::FormatSettings & settings)
{
    writeString(settings.csv.null_representation, ostr);
}

bool SerializationNullable::tryDeserializeNullCSV(DB::ReadBuffer & istr, const DB::FormatSettings & settings)
{
    return checkString(settings.csv.null_representation, istr);
}

template<typename ReturnType>
ReturnType deserializeTextCSVImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested_serialization, bool & is_null)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto deserialize_nested = [&nested_serialization, &settings] (IColumn & nested_column, ReadBuffer & buf)
    {
        if constexpr (!throw_exception)
            return nested_serialization->tryDeserializeTextCSV(nested_column, buf, settings);
        nested_serialization->deserializeTextCSV(nested_column, buf, settings);
    };

    const String & null_representation = settings.csv.null_representation;
    if (istr.eof() || (!null_representation.empty() && *istr.position() != null_representation[0]))
    {
        /// This is not null, surely.
        return deserializeImpl<ReturnType>(column, istr, [](ReadBuffer &){ return false; }, deserialize_nested, is_null);
    }

    /// Check if we have enough data in buffer to check if it's a null.
    if (settings.csv.custom_delimiter.empty() && istr.available() > null_representation.size())
    {
        auto check_for_null = [&null_representation, &settings](ReadBuffer & buf)
        {
            auto * pos = buf.position();
            if (checkString(null_representation, buf) && (*buf.position() == settings.csv.delimiter || *buf.position() == '\r' || *buf.position() == '\n'))
                return true;
            buf.position() = pos;
            return false;
        };
        return deserializeImpl<ReturnType>(column, istr, check_for_null, deserialize_nested, is_null);
    }

    /// We don't have enough data in buffer to check if it's a null.
    /// Use PeekableReadBuffer to make a checkpoint before checking null
    /// representation and rollback if the check was failed.
    PeekableReadBuffer peekable_buf(istr);
    auto check_for_null = [&null_representation, &settings](ReadBuffer & buf_)
    {
        auto & buf = assert_cast<PeekableReadBuffer &>(buf_);
        buf.setCheckpoint();
        SCOPE_EXIT(buf.dropCheckpoint());
        if (checkString(null_representation, buf))
        {
            if (!settings.csv.custom_delimiter.empty())
            {
                if (checkString(settings.csv.custom_delimiter, buf))
                {
                    /// Rollback to the beginning of custom delimiter.
                    buf.rollbackToCheckpoint();
                    assertString(null_representation, buf);
                    return true;
                }
            }
            else if (buf.eof() || *buf.position() == settings.csv.delimiter || *buf.position() == '\r' || *buf.position() == '\n')
                return true;
        }

        buf.rollbackToCheckpoint();
        return false;
    };

    auto deserialize_nested_with_check = [&deserialize_nested, &nested_serialization, &settings, &null_representation, &istr] (IColumn & nested_column, ReadBuffer & buf_)
    {
        auto & buf = assert_cast<PeekableReadBuffer &>(buf_);
        auto * pos = buf.position();
        if constexpr (throw_exception)
            deserialize_nested(nested_column, buf);
        else if (!deserialize_nested(nested_column, buf))
            return ReturnType(false);

        /// Check that we don't have any unread data in PeekableReadBuffer own memory.
        if (likely(!buf.hasUnreadData()))
            return ReturnType(true);

        /// We have some unread data in PeekableReadBuffer own memory.
        /// It can happen only if there is an unquoted string instead of a number
        /// or if someone uses csv delimiter, LF or CR in CSV null representation.
        /// In the first case we cannot continue reading anyway. The second case seems to be unlikely.
        /// We also should delete incorrectly deserialized value from nested column.
        nested_column.popBack(1);

        if constexpr (!throw_exception)
            return ReturnType(false);

        if (null_representation.contains(settings.csv.delimiter) || null_representation.contains('\r') || null_representation.contains('\n'))
            throw DB::Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "CSV custom null representation containing "
                                       "format_csv_delimiter, '\\r' or '\\n' may not work correctly for large input.");

        WriteBufferFromOwnString parsed_value;
        nested_serialization->serializeTextCSV(nested_column, nested_column.size() - 1, parsed_value, settings);
        throw DB::Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while parsing \"{}{}\" as Nullable"
                                   " at position {}: got \"{}\", which was deserialized as \"{}\". "
                                   "It seems that input data is ill-formatted.",
                                   std::string(pos, buf.buffer().end()),
                                   std::string(istr.position(), std::min(size_t(10), istr.available())),
                                   istr.count(), std::string(pos, buf.position() - pos), parsed_value.str());
    };

    return deserializeImpl<ReturnType>(column, peekable_buf, check_for_null, deserialize_nested_with_check, is_null);
}

/// A Nullable(Tuple) value is written as a single CSV field (see serializeTextCSV), so it must be
/// read back as one field too. Disable separate-columns parsing for the nested tuple to keep the
/// input symmetric with the output (issue #107342).
static void adjustCSVSettingsForNullableTuple(
    const SerializationPtr & nested, const FormatSettings & settings, std::optional<FormatSettings> & storage)
{
    if (settings.csv.deserialize_separate_columns_into_tuple && typeid_cast<const SerializationTuple *>(nested.get()))
    {
        storage = settings;
        storage->csv.deserialize_separate_columns_into_tuple = false;
    }
}

void SerializationNullable::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null = false;
    std::optional<FormatSettings> settings_storage;
    adjustCSVSettingsForNullableTuple(nested, settings, settings_storage);
    const FormatSettings & effective_settings = settings_storage ? *settings_storage : settings;
    deserializeTextCSVImpl<void>(col.getNestedColumnNoMaterialize(), istr, effective_settings, nested, is_null);
    safeAppendToNullMap<void>(col, is_null);
}

bool SerializationNullable::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null = false;
    std::optional<FormatSettings> settings_storage;
    adjustCSVSettingsForNullableTuple(nested, settings, settings_storage);
    const FormatSettings & effective_settings = settings_storage ? *settings_storage : settings;
    return deserializeTextCSVImpl<bool>(col.getNestedColumnNoMaterialize(), istr, effective_settings, nested, is_null) && safeAppendToNullMap<bool>(col, is_null);
}

bool SerializationNullable::deserializeNullAsDefaultOrNestedTextCSV(DB::IColumn & nested_column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, const DB::SerializationPtr & nested_serialization)
{
    bool is_null = false;
    deserializeTextCSVImpl<void>(nested_column, istr, settings, nested_serialization, is_null);
    return !is_null;
}

bool SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextCSV(DB::IColumn & nested_column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, const DB::SerializationPtr & nested_serialization)
{
    bool is_null = false;
    return deserializeTextCSVImpl<bool>(nested_column, istr, settings, nested_serialization, is_null);
}

void SerializationNullable::serializeTextHive(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        /// Hive's LazySimpleSerDe uses `\N` as its default null sequence (serialization.null.format).
        /// Write it directly instead of reusing format_csv_null_representation, whose custom value would
        /// otherwise leak into HiveText output and no longer be recognized by Hive as null.
        writeCString("\\N", ostr);
    else
        nested->serializeTextHive(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    /// In simple text format (like 'Pretty' format) (these formats are suitable only for output and cannot be parsed back),
    ///  data is printed without escaping.
    /// It makes theoretically impossible to distinguish between NULL and some string value, regardless on how do we print NULL.
    /// For this reason, we output NULL in a bit strange way.
    /// This assumes UTF-8 and proper font support. This is Ok, because Pretty formats are "presentational", not for data exchange.

    if (col.isNullAt(row_num))
        serializeNullText(ostr, settings);
    else
        nested->serializeText(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::serializeNullText(DB::WriteBuffer & ostr, const DB::FormatSettings & settings)
{
    if (settings.pretty.charset == FormatSettings::Pretty::Charset::UTF8)
        writeCString("ᴺᵁᴸᴸ", ostr);
    else
        writeCString("NULL", ostr);
}

bool SerializationNullable::tryDeserializeNullText(DB::ReadBuffer & istr)
{
    if (checkCharCaseInsensitive('N', istr))
        return checkStringCaseInsensitive("ULL", istr);
    return checkStringCaseInsensitive("ᴺᵁᴸᴸ", istr);
}

void SerializationNullable::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        serializeNullJSON(ostr);
    else
        nested->serializeTextJSON(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        serializeNullJSON(ostr);
    else
        nested->serializeTextJSONPretty(col.getNestedColumn(), row_num, ostr, settings, indent);
}

void SerializationNullable::serializeNullJSON(DB::WriteBuffer & ostr)
{
    writeCString("null", ostr);
}

bool SerializationNullable::tryDeserializeNullJSON(DB::ReadBuffer & istr)
{
    return checkString("null", istr);
}

template<typename ReturnType>
ReturnType deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const SerializationPtr & nested, bool & is_null)
{
    auto check_for_null = [](ReadBuffer & buf){ return checkStringByFirstCharacterAndAssertTheRest("null", buf); };
    auto deserialize_nested = [&nested, &settings](IColumn & nested_column, ReadBuffer & buf)
    {
        if constexpr (std::is_same_v<ReturnType, bool>)
            return nested->tryDeserializeTextJSON(nested_column, buf, settings);
        nested->deserializeTextJSON(nested_column, buf, settings);
    };

    return deserializeImpl<ReturnType>(column, istr, check_for_null, deserialize_nested, is_null);
}

void SerializationNullable::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null = false;
    deserializeTextJSONImpl<void>(col.getNestedColumnNoMaterialize(), istr, settings, nested, is_null);
    safeAppendToNullMap<void>(col, is_null);
}

bool SerializationNullable::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnNullable & col = assert_cast<ColumnNullable &>(column);
    bool is_null = false;
    return deserializeTextJSONImpl<bool>(col.getNestedColumnNoMaterialize(), istr, settings, nested, is_null) && safeAppendToNullMap<bool>(col, is_null);
}

bool SerializationNullable::deserializeNullAsDefaultOrNestedTextJSON(DB::IColumn & nested_column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, const DB::SerializationPtr & nested_serialization)
{
    bool is_null = false;
    deserializeTextJSONImpl<void>(nested_column, istr, settings, nested_serialization, is_null);
    return !is_null;
}

bool SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextJSON(DB::IColumn & nested_column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, const DB::SerializationPtr & nested_serialization)
{
    bool is_null = false;
    return deserializeTextJSONImpl<bool>(nested_column, istr, settings, nested_serialization, is_null);
}

void SerializationNullable::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);

    if (col.isNullAt(row_num))
        writeCString("\\N", ostr);
    else
        nested->serializeTextXML(col.getNestedColumn(), row_num, ostr, settings);
}

void SerializationNullable::serializeNullXML(DB::WriteBuffer & ostr)
{
    writeCString("\\N", ostr);
}

SerializationPtr SerializationNullable::create(const SerializationPtr & nested_, bool use_default_null_map_, bool compact_null_map_)
{
    if (!nested_->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationNullable(nested_, use_default_null_map_, compact_null_map_));
    return ISerialization::pooled(
        getHash(nested_, use_default_null_map_, compact_null_map_),
        [&] { return new SerializationNullable(nested_, use_default_null_map_, compact_null_map_); });
}

bool SerializationNullable::isNullMapSubcolumn(const SubstreamPath & path)
{
    return !path.empty() && (path.back().type == Substream::NullMap || path.back().type == Substream::SparseNullMap);
}

}
