#include <Common/StringUtils/StringUtils.h>
#include <Columns/ColumnTuple.h>
#include <Core/Field.h>
#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTNameTypePair.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <ext/map.h>
#include <ext/enumerate.h>
#include <ext/range.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int EMPTY_DATA_PASSED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int DUPLICATE_COLUMN;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
}


DataTypeTuple::DataTypeTuple(const DataTypes & elems_)
    : elems(elems_), have_explicit_names(false)
{
    /// Automatically assigned names in form of '1', '2', ...
    size_t size = elems.size();
    names.resize(size);
    for (size_t i = 0; i < size; ++i)
        names[i] = toString(i + 1);
}


DataTypeTuple::DataTypeTuple(const DataTypes & elems_, const Strings & names_)
    : elems(elems_), names(names_), have_explicit_names(true)
{
    size_t size = elems.size();
    if (names.size() != size)
        throw Exception("Wrong number of names passed to constructor of DataTypeTuple", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    std::unordered_set<String> names_set;
    for (size_t i = 0; i < size; ++i)
    {
        if (names[i].empty())
            throw Exception("Names of tuple elements cannot be empty", ErrorCodes::BAD_ARGUMENTS);

        if (isNumericASCII(names[i][0]))
            throw Exception("Explicitly specified names of tuple elements cannot start with digit", ErrorCodes::BAD_ARGUMENTS);

        if (!names_set.insert(names[i]).second)
            throw Exception("Names of tuple elements must be unique", ErrorCodes::DUPLICATE_COLUMN);
    }
}


std::string DataTypeTuple::doGetName() const
{
    size_t size = elems.size();
    WriteBufferFromOwnString s;

    s << "Tuple(";
    for (size_t i = 0; i < size; ++i)
    {
        if (i != 0)
            s << ", ";

        if (have_explicit_names)
            s << backQuoteIfNeed(names[i]) << ' ';

        s << elems[i]->getName();
    }
    s << ")";

    return s.str();
}


static inline IColumn & extractElementColumn(IColumn & column, size_t idx)
{
    return assert_cast<ColumnTuple &>(column).getColumn(idx);
}

static inline const IColumn & extractElementColumn(const IColumn & column, size_t idx)
{
    return assert_cast<const ColumnTuple &>(column).getColumn(idx);
}


void DataTypeTuple::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const auto & tuple = get<const Tuple &>(field);
    for (const auto idx_elem : ext::enumerate(elems))
        idx_elem.second->serializeBinary(tuple[idx_elem.first], ostr);
}

void DataTypeTuple::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    const size_t size = elems.size();

    Tuple tuple(size);
    for (const auto i : ext::range(0, size))
        elems[i]->deserializeBinary(tuple[i], istr);

    field = tuple;
}

void DataTypeTuple::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    for (const auto idx_elem : ext::enumerate(elems))
        idx_elem.second->serializeBinary(extractElementColumn(column, idx_elem.first), row_num, ostr);
}


template <typename F>
static void addElementSafe(const DataTypes & elems, IColumn & column, F && impl)
{
    /// We use the assumption that tuples of zero size do not exist.
    size_t old_size = column.size();

    try
    {
        impl();
    }
    catch (...)
    {
        for (const auto & i : ext::range(0, ext::size(elems)))
        {
            auto & element_column = extractElementColumn(column, i);
            if (element_column.size() > old_size)
                element_column.popBack(1);
        }

        throw;
    }
}


void DataTypeTuple::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    addElementSafe(elems, column, [&]
    {
        for (const auto & i : ext::range(0, ext::size(elems)))
            elems[i]->deserializeBinary(extractElementColumn(column, i), istr);
    });
}

void DataTypeTuple::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('(', ostr);
    for (const auto i : ext::range(0, ext::size(elems)))
    {
        if (i != 0)
            writeChar(',', ostr);
        elems[i]->serializeAsTextQuoted(extractElementColumn(column, i), row_num, ostr, settings);
    }
    writeChar(')', ostr);
}

void DataTypeTuple::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    const size_t size = elems.size();
    assertChar('(', istr);

    addElementSafe(elems, column, [&]
    {
        for (const auto i : ext::range(0, size))
        {
            skipWhitespaceIfAny(istr);
            if (i != 0)
            {
                assertChar(',', istr);
                skipWhitespaceIfAny(istr);
            }
            elems[i]->deserializeAsTextQuoted(extractElementColumn(column, i), istr, settings);
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

void DataTypeTuple::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeChar('[', ostr);
    for (const auto i : ext::range(0, ext::size(elems)))
    {
        if (i != 0)
            writeChar(',', ostr);
        elems[i]->serializeAsTextJSON(extractElementColumn(column, i), row_num, ostr, settings);
    }
    writeChar(']', ostr);
}

void DataTypeTuple::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    const size_t size = elems.size();
    assertChar('[', istr);

    addElementSafe(elems, column, [&]
    {
        for (const auto i : ext::range(0, size))
        {
            skipWhitespaceIfAny(istr);
            if (i != 0)
            {
                assertChar(',', istr);
                skipWhitespaceIfAny(istr);
            }
            elems[i]->deserializeAsTextJSON(extractElementColumn(column, i), istr, settings);
        }
    });

    skipWhitespaceIfAny(istr);
    assertChar(']', istr);
}

void DataTypeTuple::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeCString("<tuple>", ostr);
    for (const auto i : ext::range(0, ext::size(elems)))
    {
        writeCString("<elem>", ostr);
        elems[i]->serializeAsTextXML(extractElementColumn(column, i), row_num, ostr, settings);
        writeCString("</elem>", ostr);
    }
    writeCString("</tuple>", ostr);
}

void DataTypeTuple::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    for (const auto i : ext::range(0, ext::size(elems)))
    {
        if (i != 0)
            writeChar(',', ostr);
        elems[i]->serializeAsTextCSV(extractElementColumn(column, i), row_num, ostr, settings);
    }
}

void DataTypeTuple::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    addElementSafe(elems, column, [&]
    {
        const size_t size = elems.size();
        for (const auto i : ext::range(0, size))
        {
            if (i != 0)
            {
                skipWhitespaceIfAny(istr);
                assertChar(settings.csv.delimiter, istr);
                skipWhitespaceIfAny(istr);
            }
            elems[i]->deserializeAsTextCSV(extractElementColumn(column, i), istr, settings);
        }
    });
}

void DataTypeTuple::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    path.push_back(Substream::TupleElement);
    for (const auto i : ext::range(0, ext::size(elems)))
    {
        path.back().tuple_element_name = names[i];
        elems[i]->enumerateStreams(callback, path);
    }
    path.pop_back();
}

struct SerializeBinaryBulkStateTuple : public IDataType::SerializeBinaryBulkState
{
    std::vector<IDataType::SerializeBinaryBulkStatePtr> states;
};

struct DeserializeBinaryBulkStateTuple : public IDataType::DeserializeBinaryBulkState
{
    std::vector<IDataType::DeserializeBinaryBulkStatePtr> states;
};

static SerializeBinaryBulkStateTuple * checkAndGetTupleSerializeState(IDataType::SerializeBinaryBulkStatePtr & state)
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

static DeserializeBinaryBulkStateTuple * checkAndGetTupleDeserializeState(IDataType::DeserializeBinaryBulkStatePtr & state)
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

void DataTypeTuple::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto tuple_state = std::make_shared<SerializeBinaryBulkStateTuple>();
    tuple_state->states.resize(elems.size());

    settings.path.push_back(Substream::TupleElement);
    for (size_t i = 0; i < elems.size(); ++i)
    {
        settings.path.back().tuple_element_name = names[i];
        elems[i]->serializeBinaryBulkStatePrefix(settings, tuple_state->states[i]);
    }
    settings.path.pop_back();

    state = std::move(tuple_state);
}

void DataTypeTuple::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * tuple_state = checkAndGetTupleSerializeState(state);

    settings.path.push_back(Substream::TupleElement);
    for (size_t i = 0; i < elems.size(); ++i)
    {
        settings.path.back().tuple_element_name = names[i];
        elems[i]->serializeBinaryBulkStateSuffix(settings, tuple_state->states[i]);
    }
    settings.path.pop_back();
}

void DataTypeTuple::deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state) const
{
    auto tuple_state = std::make_shared<DeserializeBinaryBulkStateTuple>();
    tuple_state->states.resize(elems.size());

    settings.path.push_back(Substream::TupleElement);
    for (size_t i = 0; i < elems.size(); ++i)
    {
        settings.path.back().tuple_element_name = names[i];
        elems[i]->deserializeBinaryBulkStatePrefix(settings, tuple_state->states[i]);
    }
    settings.path.pop_back();

    state = std::move(tuple_state);
}

void DataTypeTuple::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * tuple_state = checkAndGetTupleSerializeState(state);

    settings.path.push_back(Substream::TupleElement);
    for (const auto i : ext::range(0, ext::size(elems)))
    {
        settings.path.back().tuple_element_name = names[i];
        const auto & element_col = extractElementColumn(column, i);
        elems[i]->serializeBinaryBulkWithMultipleStreams(element_col, offset, limit, settings, tuple_state->states[i]);
    }
    settings.path.pop_back();
}

void DataTypeTuple::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    auto * tuple_state = checkAndGetTupleDeserializeState(state);

    settings.path.push_back(Substream::TupleElement);
    settings.avg_value_size_hint = 0;
    for (const auto i : ext::range(0, ext::size(elems)))
    {
        settings.path.back().tuple_element_name = names[i];
        auto & element_col = extractElementColumn(column, i);
        elems[i]->deserializeBinaryBulkWithMultipleStreams(element_col, limit, settings, tuple_state->states[i]);
    }
    settings.path.pop_back();
}

void DataTypeTuple::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const
{
    for (; value_index < elems.size(); ++value_index)
    {
        size_t stored = 0;
        elems[value_index]->serializeProtobuf(extractElementColumn(column, value_index), row_num, protobuf, stored);
        if (!stored)
            break;
    }
}

void DataTypeTuple::deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const
{
    row_added = false;
    bool all_elements_get_row = true;
    addElementSafe(elems, column, [&]
    {
        for (const auto & i : ext::range(0, ext::size(elems)))
        {
            bool element_row_added;
            elems[i]->deserializeProtobuf(extractElementColumn(column, i), protobuf, allow_add_row, element_row_added);
            all_elements_get_row &= element_row_added;
        }
    });
    row_added = all_elements_get_row;
}

MutableColumnPtr DataTypeTuple::createColumn() const
{
    size_t size = elems.size();
    MutableColumns tuple_columns(size);
    for (size_t i = 0; i < size; ++i)
        tuple_columns[i] = elems[i]->createColumn();
    return ColumnTuple::create(std::move(tuple_columns));
}

Field DataTypeTuple::getDefault() const
{
    return Tuple(ext::map<Tuple>(elems, [] (const DataTypePtr & elem) { return elem->getDefault(); }));
}

void DataTypeTuple::insertDefaultInto(IColumn & column) const
{
    addElementSafe(elems, column, [&]
    {
        for (const auto & i : ext::range(0, ext::size(elems)))
            elems[i]->insertDefaultInto(extractElementColumn(column, i));
    });
}

bool DataTypeTuple::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    const DataTypeTuple & rhs_tuple = static_cast<const DataTypeTuple &>(rhs);

    size_t size = elems.size();
    if (size != rhs_tuple.elems.size())
        return false;

    for (size_t i = 0; i < size; ++i)
        if (!elems[i]->equals(*rhs_tuple.elems[i]))
            return false;

    return true;
}


size_t DataTypeTuple::getPositionByName(const String & name) const
{
    size_t size = elems.size();
    for (size_t i = 0; i < size; ++i)
        if (names[i] == name)
            return i;
    throw Exception("Tuple doesn't have element with name '" + name + "'", ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);
}


bool DataTypeTuple::textCanContainOnlyValidUTF8() const
{
    return std::all_of(elems.begin(), elems.end(), [](auto && elem) { return elem->textCanContainOnlyValidUTF8(); });
}

bool DataTypeTuple::haveMaximumSizeOfValue() const
{
    return std::all_of(elems.begin(), elems.end(), [](auto && elem) { return elem->haveMaximumSizeOfValue(); });
}

bool DataTypeTuple::isComparable() const
{
    return std::all_of(elems.begin(), elems.end(), [](auto && elem) { return elem->isComparable(); });
}

size_t DataTypeTuple::getMaximumSizeOfValueInMemory() const
{
    size_t res = 0;
    for (const auto & elem : elems)
        res += elem->getMaximumSizeOfValueInMemory();
    return res;
}

size_t DataTypeTuple::getSizeOfValueInMemory() const
{
    size_t res = 0;
    for (const auto & elem : elems)
        res += elem->getSizeOfValueInMemory();
    return res;
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        throw Exception("Tuple cannot be empty", ErrorCodes::EMPTY_DATA_PASSED);

    DataTypes nested_types;
    nested_types.reserve(arguments->children.size());

    Strings names;
    names.reserve(arguments->children.size());

    for (const ASTPtr & child : arguments->children)
    {
        if (const auto * name_and_type_pair = child->as<ASTNameTypePair>())
        {
            nested_types.emplace_back(DataTypeFactory::instance().get(name_and_type_pair->type));
            names.emplace_back(name_and_type_pair->name);
        }
        else
            nested_types.emplace_back(DataTypeFactory::instance().get(child));
    }

    if (names.empty())
        return std::make_shared<DataTypeTuple>(nested_types);
    else if (names.size() != nested_types.size())
        throw Exception("Names are specified not for all elements of Tuple type", ErrorCodes::BAD_ARGUMENTS);
    else
        return std::make_shared<DataTypeTuple>(nested_types, names);
}


void registerDataTypeTuple(DataTypeFactory & factory)
{
    factory.registerDataType("Tuple", create);
}

void registerDataTypeNested(DataTypeFactory & factory)
{
    /// Nested(...) data type is just a sugar for Array(Tuple(...))
    factory.registerDataType("Nested", [&factory](const ASTPtr & arguments)
    {
        return std::make_shared<DataTypeArray>(factory.get("Tuple", arguments));
    });
}

}
