#include <DataTypes/Serializations/SerializationTuple.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/Serializations/SerializationInfoTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <Core/Field.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int INCORRECT_DATA;
}


static inline IColumn & extractElementColumn(IColumn & column, size_t idx)
{
    return assert_cast<ColumnTuple &>(column).getColumn(idx);
}

static inline const IColumn & extractElementColumn(const IColumn & column, size_t idx)
{
    return assert_cast<const ColumnTuple &>(column).getColumn(idx);
}

void SerializationTuple::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & tuple = field.get<const Tuple &>();
    for (size_t element_index = 0; element_index < elems.size(); ++element_index)
    {
        const auto & serialization = elems[element_index];
        serialization->serializeBinary(tuple[element_index], ostr, settings);
    }
}

void SerializationTuple::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    const size_t size = elems.size();

    field = Tuple();
    Tuple & tuple = field.get<Tuple &>();
    tuple.reserve(size);
    for (size_t i = 0; i < size; ++i)
        elems[i]->deserializeBinary(tuple.emplace_back(), istr, settings);
}

void SerializationTuple::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    for (size_t element_index = 0; element_index < elems.size(); ++element_index)
    {
        const auto & serialization = elems[element_index];
        serialization->serializeBinary(extractElementColumn(column, element_index), row_num, ostr, settings);
    }
}


template <typename ReturnType, typename F>
static ReturnType addElementSafe(size_t num_elems, IColumn & column, F && impl)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    /// We use the assumption that tuples of zero size do not exist.
    size_t old_size = column.size();

    auto restore_elements = [&]()
    {
        for (size_t i = 0; i < num_elems; ++i)
        {
            auto & element_column = extractElementColumn(column, i);
            if (element_column.size() > old_size)
            {
                chassert(element_column.size() - old_size == 1);
                element_column.popBack(1);
            }
        }
    };

    try
    {
        if (!impl())
        {
            restore_elements();
            return ReturnType(false);
        }

        // Check that all columns now have the same size.
        size_t new_size = column.size();
        for (size_t i = 1; i < num_elems; ++i)
        {
            const auto & element_column = extractElementColumn(column, i);
            if (element_column.size() != new_size)
            {
                // This is not a logical error because it may work with
                // user-supplied data.
                if constexpr (throw_exception)
                    throw Exception(ErrorCodes::SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH,
                        "Cannot read a tuple because not all elements are present");
                restore_elements();
                return ReturnType(false);
            }
        }
    }
    catch (...)
    {
        restore_elements();
        if constexpr (throw_exception)
            throw;
        return ReturnType(false);
    }

    return ReturnType(true);
}

void SerializationTuple::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    addElementSafe<void>(elems.size(), column, [&]
    {
        for (size_t i = 0; i < elems.size(); ++i)
            elems[i]->deserializeBinary(extractElementColumn(column, i), istr, settings);
        return true;
    });
}

void SerializationTuple::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('(', ostr);
    for (size_t i = 0; i < elems.size(); ++i)
    {
        if (i != 0)
            writeChar(',', ostr);
        elems[i]->serializeTextQuoted(extractElementColumn(column, i), row_num, ostr, settings);
    }
    writeChar(')', ostr);
}

template <typename ReturnType>
ReturnType SerializationTuple::deserializeTextImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    const size_t size = elems.size();
    if constexpr (throw_exception)
        assertChar('(', istr);
    else if (!checkChar('(', istr))
        return ReturnType(false);

    auto impl = [&]()
    {
        for (size_t i = 0; i < size; ++i)
        {
            skipWhitespaceIfAny(istr);
            if (i != 0)
            {
                if constexpr (throw_exception)
                    assertChar(',', istr);
                else if (!checkChar(',', istr))
                    return false;

                skipWhitespaceIfAny(istr);
            }

            auto & element_column = extractElementColumn(column, i);
            if constexpr (throw_exception)
            {
                if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(element_column))
                    SerializationNullable::deserializeNullAsDefaultOrNestedTextQuoted(element_column, istr, settings, elems[i]);
                else
                    elems[i]->deserializeTextQuoted(element_column, istr, settings);
            }
            else
            {
                bool ok;
                if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(element_column))
                    ok = SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextQuoted(element_column, istr, settings, elems[i]);
                else
                    ok = elems[i]->tryDeserializeTextQuoted(element_column, istr, settings);

                if (!ok)
                    return false;
            }
        }

        // Special format for one element tuple (1,)
        if (1 == elems.size())
        {
            skipWhitespaceIfAny(istr);
            // Allow both (1) and (1,)
            checkChar(',', istr);
        }

        skipWhitespaceIfAny(istr);
        if constexpr (throw_exception)
            assertChar(')', istr);
        else if (!checkChar(')', istr))
            return false;

        if (whole && !istr.eof())
        {
            if constexpr (throw_exception)
                throwUnexpectedDataAfterParsedValue(column, istr, settings, "Tuple");
            return false;
        }

        return true;
    };

    return addElementSafe<ReturnType>(elems.size(), column, impl);
}

void SerializationTuple::deserializeText(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, bool whole) const
{
    deserializeTextImpl(column, istr, settings, whole);
}

bool SerializationTuple::tryDeserializeText(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings, bool whole) const
{
    return deserializeTextImpl<bool>(column, istr, settings, whole);
}

void SerializationTuple::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (settings.json.write_named_tuples_as_objects
        && have_explicit_names)
    {
        writeChar('{', ostr);

        bool first = true;
        for (size_t i = 0; i < elems.size(); ++i)
        {
            const auto & element_column = extractElementColumn(column, i);
            if (settings.json.skip_null_value_in_named_tuples && element_column.isNullAt(row_num))
                continue;

            if (!first)
                writeChar(',', ostr);

            writeJSONString(elems[i]->getElementName(), ostr, settings);
            writeChar(':', ostr);
            elems[i]->serializeTextJSON(element_column, row_num, ostr, settings);
            first = false;
        }

        writeChar('}', ostr);
    }
    else
    {
        writeChar('[', ostr);
        for (size_t i = 0; i < elems.size(); ++i)
        {
            if (i != 0)
                writeChar(',', ostr);
            elems[i]->serializeTextJSON(extractElementColumn(column, i), row_num, ostr, settings);
        }
        writeChar(']', ostr);
    }
}

void SerializationTuple::serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const
{
    if (settings.json.write_named_tuples_as_objects
        && have_explicit_names)
    {
        writeCString("{\n", ostr);

        bool first = true;
        for (size_t i = 0; i < elems.size(); ++i)
        {
            const auto & element_column = extractElementColumn(column, i);
            if (settings.json.skip_null_value_in_named_tuples && element_column.isNullAt(row_num))
                continue;

            if (!first)
                writeCString(",\n", ostr);

            writeChar(' ', (indent + 1) * 4, ostr);
            writeJSONString(elems[i]->getElementName(), ostr, settings);
            writeCString(": ", ostr);
            elems[i]->serializeTextJSONPretty(extractElementColumn(column, i), row_num, ostr, settings, indent + 1);
            first = false;
        }

        writeChar('\n', ostr);
        writeChar(' ', indent * 4, ostr);
        writeChar('}', ostr);
    }
    else
    {
        writeCString("[\n", ostr);
        for (size_t i = 0; i < elems.size(); ++i)
        {
            if (i != 0)
                writeCString(",\n", ostr);
            writeChar(' ', (indent + 1) * 4, ostr);
            elems[i]->serializeTextJSONPretty(extractElementColumn(column, i), row_num, ostr, settings, indent + 1);
        }
        writeChar('\n', ostr);
        writeChar(' ', indent * 4, ostr);
        writeChar(']', ostr);
    }
}

template <typename ReturnType>
ReturnType SerializationTuple::deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto deserialize_element = [&](IColumn & element_column, size_t element_pos)
    {
        if constexpr (throw_exception)
        {
            if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(element_column))
                SerializationNullable::deserializeNullAsDefaultOrNestedTextJSON(element_column, istr, settings, elems[element_pos]);
            else
                elems[element_pos]->deserializeTextJSON(element_column, istr, settings);
            return true;
        }
        else
        {
            if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(element_column))
                return SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextJSON(element_column, istr, settings, elems[element_pos]);
            return elems[element_pos]->tryDeserializeTextJSON(element_column, istr, settings);
        }
    };

    if (settings.json.read_named_tuples_as_objects
        && have_explicit_names)
    {
        skipWhitespaceIfAny(istr);
        if constexpr (throw_exception)
            assertChar('{', istr);
        else if (!checkChar('{', istr))
            return ReturnType(false);
        skipWhitespaceIfAny(istr);

        auto impl = [&]()
        {
            std::vector<UInt8> seen_elements(elems.size(), 0);
            size_t processed = 0;
            size_t skipped = 0;
            while (!istr.eof() && *istr.position() != '}')
            {
                if (!settings.json.ignore_unknown_keys_in_named_tuple && processed == elems.size())
                {
                    if constexpr (throw_exception)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected number of elements in named tuple. Expected no more than {} (consider enabling input_format_json_ignore_unknown_keys_in_named_tuple setting)", elems.size());
                    return false;
                }

                if (processed + skipped > 0)
                {
                    if constexpr (throw_exception)
                        assertChar(',', istr);
                    else if (!checkChar(',', istr))
                        return false;
                    skipWhitespaceIfAny(istr);
                }

                std::string name;
                if constexpr (throw_exception)
                    readDoubleQuotedString(name, istr);
                else if (!tryReadDoubleQuotedString(name, istr))
                    return false;

                skipWhitespaceIfAny(istr);
                if constexpr (throw_exception)
                    assertChar(':', istr);
                else if (!checkChar(':', istr))
                    return false;
                skipWhitespaceIfAny(istr);

                const size_t element_pos = getPositionByName(name);
                if (element_pos == std::numeric_limits<size_t>::max())
                {
                    if (settings.json.ignore_unknown_keys_in_named_tuple)
                    {
                        if constexpr (throw_exception)
                            skipJSONField(istr, name);
                        else if (!trySkipJSONField(istr, name))
                            return false;

                        skipWhitespaceIfAny(istr);
                        ++skipped;
                        continue;
                    }
                    else
                    {
                        if constexpr (throw_exception)
                            throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "Tuple doesn't have element with name '{}', enable setting input_format_json_ignore_unknown_keys_in_named_tuple", name);
                        return false;
                    }
                }

                seen_elements[element_pos] = 1;
                auto & element_column = extractElementColumn(column, element_pos);

                if constexpr (throw_exception)
                {
                    try
                    {
                        deserialize_element(element_column, element_pos);
                    }
                    catch (Exception & e)
                    {
                        e.addMessage("(while reading the value of nested key " + name + ")");
                        throw;
                    }
                }
                else
                {
                    if (!deserialize_element(element_column, element_pos))
                        return false;
                }

                skipWhitespaceIfAny(istr);
                ++processed;
            }

            if constexpr (throw_exception)
                assertChar('}', istr);
            else if (!checkChar('}', istr))
                return false;

            /// Check if we have missing elements.
            if (processed != elems.size())
            {
                for (size_t element_pos = 0; element_pos != seen_elements.size(); ++element_pos)
                {
                    if (seen_elements[element_pos])
                        continue;

                    if (!settings.json.defaults_for_missing_elements_in_named_tuple)
                    {
                        if constexpr (throw_exception)
                            throw Exception(
                                ErrorCodes::INCORRECT_DATA,
                                "JSON object doesn't contain tuple element {}. If you want to insert defaults in case of missing elements, "
                                "enable setting input_format_json_defaults_for_missing_elements_in_named_tuple",
                                elems[element_pos]->getElementName());
                        return false;
                    }

                    auto & element_column = extractElementColumn(column, element_pos);
                    element_column.insertDefault();
                }
            }

            return true;
        };

        return addElementSafe<ReturnType>(elems.size(), column, impl);
    }
    else
    {
        skipWhitespaceIfAny(istr);
        if constexpr (throw_exception)
            assertChar('[', istr);
        else if (!checkChar('[', istr))
            return false;
        skipWhitespaceIfAny(istr);

        auto impl = [&]()
        {
            for (size_t i = 0; i < elems.size(); ++i)
            {
                skipWhitespaceIfAny(istr);
                if (i != 0)
                {
                    if constexpr (throw_exception)
                        assertChar(',', istr);
                    else if (!checkChar(',', istr))
                        return false;
                    skipWhitespaceIfAny(istr);
                }

                auto & element_column = extractElementColumn(column, i);

                if constexpr (throw_exception)
                    deserialize_element(element_column, i);
                else if (!deserialize_element(element_column, i))
                    return false;
            }

            skipWhitespaceIfAny(istr);
            if constexpr (throw_exception)
                assertChar(']', istr);
            else if (!checkChar(']', istr))
                return false;

            return true;
        };

        return addElementSafe<ReturnType>(elems.size(), column, impl);
    }
}

void SerializationTuple::deserializeTextJSON(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    deserializeTextJSONImpl(column, istr, settings);
}

bool SerializationTuple::tryDeserializeTextJSON(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    return deserializeTextJSONImpl<bool>(column, istr, settings);
}


void SerializationTuple::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeCString("<tuple>", ostr);
    for (size_t i = 0; i < elems.size(); ++i)
    {
        writeCString("<elem>", ostr);
        elems[i]->serializeTextXML(extractElementColumn(column, i), row_num, ostr, settings);
        writeCString("</elem>", ostr);
    }
    writeCString("</tuple>", ostr);
}

void SerializationTuple::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    for (size_t i = 0; i < elems.size(); ++i)
    {
        if (i != 0)
            writeChar(settings.csv.tuple_delimiter, ostr);
        elems[i]->serializeTextCSV(extractElementColumn(column, i), row_num, ostr, settings);
    }
}

void SerializationTuple::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    addElementSafe<void>(elems.size(), column, [&]
    {
        const size_t size = elems.size();
        for (size_t i = 0; i < size; ++i)
        {
            if (i != 0)
            {
                skipWhitespaceIfAny(istr);
                assertChar(settings.csv.tuple_delimiter, istr);
                skipWhitespaceIfAny(istr);
            }

            auto & element_column = extractElementColumn(column, i);
            if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(element_column))
                SerializationNullable::deserializeNullAsDefaultOrNestedTextCSV(element_column, istr, settings, elems[i]);
            else
                elems[i]->deserializeTextCSV(element_column, istr, settings);
        }
        return true;
    });
}

bool SerializationTuple::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return addElementSafe<bool>(elems.size(), column, [&]
    {
        const size_t size = elems.size();
        for (size_t i = 0; i < size; ++i)
        {
            if (i != 0)
            {
               skipWhitespaceIfAny(istr);
               if (!checkChar(settings.csv.tuple_delimiter, istr))
                   return false;
               skipWhitespaceIfAny(istr);
            }

            auto & element_column = extractElementColumn(column, i);
            if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(element_column))
            {
               if (!SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextCSV(element_column, istr, settings, elems[i]))
                   return false;
            }
            else
            {
               if (!elems[i]->tryDeserializeTextCSV(element_column, istr, settings))
                   return false;
            }
        }

        return true;
    });
}

void SerializationTuple::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * type_tuple = data.type ? &assert_cast<const DataTypeTuple &>(*data.type) : nullptr;
    const auto * column_tuple = data.column ? &assert_cast<const ColumnTuple &>(*data.column) : nullptr;
    const auto * info_tuple = data.serialization_info ? &assert_cast<const SerializationInfoTuple &>(*data.serialization_info) : nullptr;

    for (size_t i = 0; i < elems.size(); ++i)
    {
        auto next_data = SubstreamData(elems[i])
            .withType(type_tuple ? type_tuple->getElement(i) : nullptr)
            .withColumn(column_tuple ? column_tuple->getColumnPtr(i) : nullptr)
            .withSerializationInfo(info_tuple ? info_tuple->getElementInfo(i) : nullptr);

        elems[i]->enumerateStreams(settings, callback, next_data);
    }
}

struct SerializeBinaryBulkStateTuple : public ISerialization::SerializeBinaryBulkState
{
    std::vector<ISerialization::SerializeBinaryBulkStatePtr> states;
};

struct DeserializeBinaryBulkStateTuple : public ISerialization::DeserializeBinaryBulkState
{
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> states;
};


void SerializationTuple::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto tuple_state = std::make_shared<SerializeBinaryBulkStateTuple>();
    tuple_state->states.resize(elems.size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->serializeBinaryBulkStatePrefix(extractElementColumn(column, i), settings, tuple_state->states[i]);

    state = std::move(tuple_state);
}

void SerializationTuple::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * tuple_state = checkAndGetState<SerializeBinaryBulkStateTuple>(state);

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->serializeBinaryBulkStateSuffix(settings, tuple_state->states[i]);
}

void SerializationTuple::deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state) const
{
    auto tuple_state = std::make_shared<DeserializeBinaryBulkStateTuple>();
    tuple_state->states.resize(elems.size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->deserializeBinaryBulkStatePrefix(settings, tuple_state->states[i]);

    state = std::move(tuple_state);
}

void SerializationTuple::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * tuple_state = checkAndGetState<SerializeBinaryBulkStateTuple>(state);

    for (size_t i = 0; i < elems.size(); ++i)
    {
        const auto & element_col = extractElementColumn(column, i);
        elems[i]->serializeBinaryBulkWithMultipleStreams(element_col, offset, limit, settings, tuple_state->states[i]);
    }
}

void SerializationTuple::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * tuple_state = checkAndGetState<DeserializeBinaryBulkStateTuple>(state);

    auto mutable_column = column->assumeMutable();
    auto & column_tuple = assert_cast<ColumnTuple &>(*mutable_column);

    settings.avg_value_size_hint = 0;
    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->deserializeBinaryBulkWithMultipleStreams(column_tuple.getColumnPtr(i), limit, settings, tuple_state->states[i], cache);
}

size_t SerializationTuple::getPositionByName(const String & name) const
{
    size_t size = elems.size();
    for (size_t i = 0; i < size; ++i)
        if (elems[i]->getElementName() == name)
            return i;
    return std::numeric_limits<size_t>::max();
}

}
