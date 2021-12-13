#include <common/map.h>
#include <common/range.h>
#include <DataTypes/Serializations/SerializationTuple.h>
#include <Core/Field.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int LOGICAL_ERROR;
}


static inline IColumn & extractElementColumn(IColumn & column, size_t idx)
{
    return assert_cast<ColumnTuple &>(column).getColumn(idx);
}

static inline const IColumn & extractElementColumn(const IColumn & column, size_t idx)
{
    return assert_cast<const ColumnTuple &>(column).getColumn(idx);
}

void SerializationTuple::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const auto & tuple = get<const Tuple &>(field);
    for (size_t element_index = 0; element_index < elems.size(); ++element_index)
    {
        const auto & serialization = elems[element_index];
        serialization->serializeBinary(tuple[element_index], ostr);
    }
}

void SerializationTuple::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    const size_t size = elems.size();

    Tuple tuple(size);
    for (const auto i : collections::range(0, size))
        elems[i]->deserializeBinary(tuple[i], istr);

    field = tuple;
}

void SerializationTuple::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    for (size_t element_index = 0; element_index < elems.size(); ++element_index)
    {
        const auto & serialization = elems[element_index];
        serialization->serializeBinary(extractElementColumn(column, element_index), row_num, ostr);
    }
}


template <typename F>
static void addElementSafe(size_t num_elems, IColumn & column, F && impl)
{
    /// We use the assumption that tuples of zero size do not exist.
    size_t old_size = column.size();

    try
    {
        impl();

        // Check that all columns now have the same size.
        size_t new_size = column.size();
        for (auto i : collections::range(1, num_elems))
        {
            const auto & element_column = extractElementColumn(column, i);
            if (element_column.size() != new_size)
            {
                // This is not a logical error because it may work with
                // user-supplied data.
                throw Exception(ErrorCodes::SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH,
                    "Cannot read a tuple because not all elements are present");
            }
        }
    }
    catch (...)
    {
        for (const auto & i : collections::range(0, num_elems))
        {
            auto & element_column = extractElementColumn(column, i);
            if (element_column.size() > old_size)
                element_column.popBack(1);
        }

        throw;
    }
}

void SerializationTuple::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    addElementSafe(elems.size(), column, [&]
    {
        for (const auto & i : collections::range(0, elems.size()))
            elems[i]->deserializeBinary(extractElementColumn(column, i), istr);
    });
}

void SerializationTuple::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('(', ostr);
    for (const auto i : collections::range(0, elems.size()))
    {
        if (i != 0)
            writeChar(',', ostr);
        elems[i]->serializeTextQuoted(extractElementColumn(column, i), row_num, ostr, settings);
    }
    writeChar(')', ostr);
}

void SerializationTuple::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    const size_t size = elems.size();
    assertChar('(', istr);

    addElementSafe(elems.size(), column, [&]
    {
        for (const auto i : collections::range(0, size))
        {
            skipWhitespaceIfAny(istr);
            if (i != 0)
            {
                assertChar(',', istr);
                skipWhitespaceIfAny(istr);
            }
            elems[i]->deserializeTextQuoted(extractElementColumn(column, i), istr, settings);
        }
    });

    // Special format for one element tuple (1,)
    if (1 == elems.size())
    {
        skipWhitespaceIfAny(istr);
        // Allow both (1) and (1,)
        checkChar(',', istr);
    }
    skipWhitespaceIfAny(istr);
    assertChar(')', istr);
}

void SerializationTuple::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (settings.json.named_tuples_as_objects
        && have_explicit_names)
    {
        writeChar('{', ostr);
        for (const auto i : collections::range(0, elems.size()))
        {
            if (i != 0)
            {
                writeChar(',', ostr);
            }
            writeJSONString(elems[i]->getElementName(), ostr, settings);
            writeChar(':', ostr);
            elems[i]->serializeTextJSON(extractElementColumn(column, i), row_num, ostr, settings);
        }
        writeChar('}', ostr);
    }
    else
    {
        writeChar('[', ostr);
        for (const auto i : collections::range(0, elems.size()))
        {
            if (i != 0)
                writeChar(',', ostr);
            elems[i]->serializeTextJSON(extractElementColumn(column, i), row_num, ostr, settings);
        }
        writeChar(']', ostr);
    }
}

void SerializationTuple::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (settings.json.named_tuples_as_objects
        && have_explicit_names)
    {
        skipWhitespaceIfAny(istr);
        assertChar('{', istr);
        skipWhitespaceIfAny(istr);

        addElementSafe(elems.size(), column, [&]
        {
            // Require all elements but in arbitrary order.
            for (auto i : collections::range(0, elems.size()))
            {
                if (i > 0)
                {
                    skipWhitespaceIfAny(istr);
                    assertChar(',', istr);
                    skipWhitespaceIfAny(istr);
                }

                std::string name;
                readDoubleQuotedString(name, istr);
                skipWhitespaceIfAny(istr);
                assertChar(':', istr);
                skipWhitespaceIfAny(istr);

                const size_t element_pos = getPositionByName(name);
                auto & element_column = extractElementColumn(column, element_pos);
                elems[element_pos]->deserializeTextJSON(element_column, istr, settings);
            }
        });

        skipWhitespaceIfAny(istr);
        assertChar('}', istr);
    }
    else
    {
        const size_t size = elems.size();
        assertChar('[', istr);

        addElementSafe(elems.size(), column, [&]
        {
            for (const auto i : collections::range(0, size))
            {
                skipWhitespaceIfAny(istr);
                if (i != 0)
                {
                    assertChar(',', istr);
                    skipWhitespaceIfAny(istr);
                }
                elems[i]->deserializeTextJSON(extractElementColumn(column, i), istr, settings);
            }
        });

        skipWhitespaceIfAny(istr);
        assertChar(']', istr);
    }
}

void SerializationTuple::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeCString("<tuple>", ostr);
    for (const auto i : collections::range(0, elems.size()))
    {
        writeCString("<elem>", ostr);
        elems[i]->serializeTextXML(extractElementColumn(column, i), row_num, ostr, settings);
        writeCString("</elem>", ostr);
    }
    writeCString("</tuple>", ostr);
}

void SerializationTuple::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    for (const auto i : collections::range(0, elems.size()))
    {
        if (i != 0)
            writeChar(',', ostr);
        elems[i]->serializeTextCSV(extractElementColumn(column, i), row_num, ostr, settings);
    }
}

void SerializationTuple::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    addElementSafe(elems.size(), column, [&]
    {
        const size_t size = elems.size();
        for (const auto i : collections::range(0, size))
        {
            if (i != 0)
            {
                skipWhitespaceIfAny(istr);
                assertChar(settings.csv.delimiter, istr);
                skipWhitespaceIfAny(istr);
            }
            elems[i]->deserializeTextCSV(extractElementColumn(column, i), istr, settings);
        }
    });
}

void SerializationTuple::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    for (const auto & elem : elems)
        elem->enumerateStreams(callback, path);
}

struct SerializeBinaryBulkStateTuple : public ISerialization::SerializeBinaryBulkState
{
    std::vector<ISerialization::SerializeBinaryBulkStatePtr> states;
};

struct DeserializeBinaryBulkStateTuple : public ISerialization::DeserializeBinaryBulkState
{
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> states;
};

static SerializeBinaryBulkStateTuple * checkAndGetTupleSerializeState(ISerialization::SerializeBinaryBulkStatePtr & state)
{
    if (!state)
        throw Exception("Got empty state for DataTypeTuple.", ErrorCodes::LOGICAL_ERROR);

    auto * tuple_state = typeid_cast<SerializeBinaryBulkStateTuple *>(state.get());
    if (!tuple_state)
    {
        auto & state_ref = *state;
        throw Exception("Invalid SerializeBinaryBulkState for DataTypeTuple. Expected: "
                        + demangle(typeid(SerializeBinaryBulkStateTuple).name()) + ", got "
                        + demangle(typeid(state_ref).name()), ErrorCodes::LOGICAL_ERROR);
    }

    return tuple_state;
}

static DeserializeBinaryBulkStateTuple * checkAndGetTupleDeserializeState(ISerialization::DeserializeBinaryBulkStatePtr & state)
{
    if (!state)
        throw Exception("Got empty state for DataTypeTuple.", ErrorCodes::LOGICAL_ERROR);

    auto * tuple_state = typeid_cast<DeserializeBinaryBulkStateTuple *>(state.get());
    if (!tuple_state)
    {
        auto & state_ref = *state;
        throw Exception("Invalid DeserializeBinaryBulkState for DataTypeTuple. Expected: "
                        + demangle(typeid(DeserializeBinaryBulkStateTuple).name()) + ", got "
                        + demangle(typeid(state_ref).name()), ErrorCodes::LOGICAL_ERROR);
    }

    return tuple_state;
}

void SerializationTuple::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto tuple_state = std::make_shared<SerializeBinaryBulkStateTuple>();
    tuple_state->states.resize(elems.size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->serializeBinaryBulkStatePrefix(settings, tuple_state->states[i]);

    state = std::move(tuple_state);
}

void SerializationTuple::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * tuple_state = checkAndGetTupleSerializeState(state);

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
    auto * tuple_state = checkAndGetTupleSerializeState(state);

    for (const auto i : collections::range(0, elems.size()))
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
    auto * tuple_state = checkAndGetTupleDeserializeState(state);

    auto mutable_column = column->assumeMutable();
    auto & column_tuple = assert_cast<ColumnTuple &>(*mutable_column);

    settings.avg_value_size_hint = 0;
    for (const auto i : collections::range(0, elems.size()))
        elems[i]->deserializeBinaryBulkWithMultipleStreams(column_tuple.getColumnPtr(i), limit, settings, tuple_state->states[i], cache);
}

size_t SerializationTuple::getPositionByName(const String & name) const
{
    size_t size = elems.size();
    for (size_t i = 0; i < size; ++i)
        if (elems[i]->getElementName() == name)
            return i;
    throw Exception("Tuple doesn't have element with name '" + name + "'", ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
}

}
