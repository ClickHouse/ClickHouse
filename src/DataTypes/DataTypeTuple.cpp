#include <base/map.h>
#include <base/range.h>
#include <Common/StringUtils/StringUtils.h>
#include <Columns/ColumnTuple.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationTuple.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/NestedUtils.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTNameTypePair.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DUPLICATE_COLUMN;
    extern const int EMPTY_DATA_PASSED;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH;
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

static std::optional<Exception> checkTupleNames(const Strings & names)
{
    std::unordered_set<String> names_set;
    for (const auto & name : names)
    {
        if (name.empty())
            return Exception("Names of tuple elements cannot be empty", ErrorCodes::BAD_ARGUMENTS);

        if (isNumericASCII(name[0]))
            return Exception("Explicitly specified names of tuple elements cannot start with digit", ErrorCodes::BAD_ARGUMENTS);

        if (!names_set.insert(name).second)
            return Exception("Names of tuple elements must be unique", ErrorCodes::DUPLICATE_COLUMN);
    }

    return {};
}

DataTypeTuple::DataTypeTuple(const DataTypes & elems_, const Strings & names_, bool serialize_names_)
    : elems(elems_), names(names_), have_explicit_names(true), serialize_names(serialize_names_)
{
    size_t size = elems.size();
    if (names.size() != size)
        throw Exception("Wrong number of names passed to constructor of DataTypeTuple", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (auto exception = checkTupleNames(names))
        throw std::move(*exception);
}

bool DataTypeTuple::canBeCreatedWithNames(const Strings & names)
{
    return checkTupleNames(names) == std::nullopt;
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

        if (have_explicit_names && serialize_names)
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

template <typename F>
static void addElementSafe(const DataTypes & elems, IColumn & column, F && impl)
{
    /// We use the assumption that tuples of zero size do not exist.
    size_t old_size = column.size();

    try
    {
        impl();

        // Check that all columns now have the same size.
        size_t new_size = column.size();

        for (auto i : collections::range(0, elems.size()))
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
        for (const auto & i : collections::range(0, elems.size()))
        {
            auto & element_column = extractElementColumn(column, i);

            if (element_column.size() > old_size)
                element_column.popBack(1);
        }

        throw;
    }
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
    return Tuple(collections::map<Tuple>(elems, [] (const DataTypePtr & elem) { return elem->getDefault(); }));
}

void DataTypeTuple::insertDefaultInto(IColumn & column) const
{
    addElementSafe(elems, column, [&]
    {
        for (const auto & i : collections::range(0, elems.size()))
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

SerializationPtr DataTypeTuple::doGetDefaultSerialization() const
{
    SerializationTuple::ElementSerializations serializations(elems.size());
    bool use_explicit_names = have_explicit_names && serialize_names;
    for (size_t i = 0; i < elems.size(); ++i)
    {
        String elem_name = use_explicit_names ? names[i] : toString(i + 1);
        auto serialization = elems[i]->getDefaultSerialization();
        serializations[i] = std::make_shared<SerializationNamed>(serialization, elem_name);
    }

    return std::make_shared<SerializationTuple>(std::move(serializations), use_explicit_names);
}

SerializationPtr DataTypeTuple::getSerialization(const String & column_name, const StreamExistenceCallback & callback) const
{
    SerializationTuple::ElementSerializations serializations(elems.size());
    bool use_explicit_names = have_explicit_names && serialize_names;
    for (size_t i = 0; i < elems.size(); ++i)
    {
        String elem_name = use_explicit_names ? names[i] : toString(i + 1);
        auto subcolumn_name = Nested::concatenateName(column_name, elem_name);
        auto serializaion = elems[i]->getSerialization(subcolumn_name, callback);
        serializations[i] = std::make_shared<SerializationNamed>(serializaion, elem_name);
    }

    return std::make_shared<SerializationTuple>(std::move(serializations), use_explicit_names);
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

}
