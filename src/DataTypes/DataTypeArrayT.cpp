#include <base/map.h>
#include <base/range.h>
#include <Common/StringUtils.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeArrayT.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <DataTypes/Serializations/SerializationTuple.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/Serializations/SerializationWrapper.h>
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
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}

DataTypeArrayT::DataTypeArrayT(const DataTypes & elems_) : DataTypeTuple(elems_) { }

DataTypeArrayT::DataTypeArrayT(const DataTypes & elems_, const Strings & names_) : DataTypeTuple(elems_, names_) { }

std::string DataTypeArrayT::doGetName() const
{
    size_t size = elems.size();
    WriteBufferFromOwnString s;

    s << "ArrayT(";
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

std::string DataTypeArrayT::doGetPrettyName(size_t indent) const
{
    size_t size = elems.size();
    WriteBufferFromOwnString s;

    /// If the ArrayT is named, we will output it in multiple lines with indentation.
    if (have_explicit_names)
    {
        s << "ArrayT(\n";

        for (size_t i = 0; i != size; ++i)
        {
            if (i != 0)
                s << ",\n";

            s << fourSpaceIndent(indent + 1)
                << backQuoteIfNeed(names[i]) << ' '
                << elems[i]->getPrettyName(indent + 1);
        }

        s << ')';
    }
    else
    {
        s << "ArrayT(";

        for (size_t i = 0; i != size; ++i)
        {
            if (i != 0)
                s << ", ";
            s << elems[i]->getPrettyName(indent);
        }

        s << ')';
    }

    return s.str();
}

DataTypePtr DataTypeArrayT::getNormalizedType() const
{
    DataTypes normalized_elems;
    normalized_elems.reserve(elems.size());
    for (const auto & elem : elems)
        normalized_elems.emplace_back(elem->getNormalizedType());
    return std::make_shared<DataTypeArrayT>(normalized_elems);
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

MutableColumnPtr DataTypeArrayT::createColumn() const
{
    if (elems.empty())
        return ColumnTuple::create(0);

    size_t size = elems.size();
    MutableColumns tuple_columns(size);
    for (size_t i = 0; i < size; ++i)
        tuple_columns[i] = elems[i]->createColumn();
    return ColumnTuple::create(std::move(tuple_columns));
}

MutableColumnPtr DataTypeArrayT::createColumn(const ISerialization & serialization) const
{
    /// If we read subcolumn of nested Tuple or this Tuple is a subcolumn, it may be wrapped to SerializationWrapper
    /// several times to allow to reconstruct the substream path name.
    /// Here we don't need substream path name, so we drop first several wrapper serializations.
    const auto * current_serialization = &serialization;
    while (const auto * serialization_wrapper = dynamic_cast<const SerializationWrapper *>(current_serialization))
        current_serialization = serialization_wrapper->getNested().get();

    const auto * serialization_tuple = typeid_cast<const SerializationTuple *>(current_serialization);
    if (!serialization_tuple)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected serialization to create column of type Tuple");

    if (elems.empty())
        return IDataType::createColumn(serialization);

    const auto & element_serializations = serialization_tuple->getElementsSerializations();

    size_t size = elems.size();
    assert(element_serializations.size() == size);
    MutableColumns tuple_columns(size);
    for (size_t i = 0; i < size; ++i)
        tuple_columns[i] = elems[i]->createColumn(*element_serializations[i]->getNested());

    return ColumnTuple::create(std::move(tuple_columns));
}

void DataTypeArrayT::insertDefaultInto(IColumn & column) const
{
    if (elems.empty())
    {
        column.insertDefault();
        return;
    }

    addElementSafe(elems, column, [&]
    {
        for (const auto & i : collections::range(0, elems.size()))
            elems[i]->insertDefaultInto(extractElementColumn(column, i));
    });
}

// TODO Replace DataTypeTuple
static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        return std::make_shared<DataTypeTuple>(DataTypes{});

    DataTypes nested_types;
    nested_types.reserve(arguments->children.size());

    Strings names;
    names.reserve(arguments->children.size());

    return std::make_shared<DataTypeArray>(DataTypeFactory::instance().get(arguments->children[0]));

    //
    // for (const ASTPtr & child : arguments->children)
    // {
    //     if (const auto * name_and_type_pair = child->as<ASTNameTypePair>())
    //     {
    //         nested_types.emplace_back(DataTypeFactory::instance().get(name_and_type_pair->type));
    //         names.emplace_back(name_and_type_pair->name);
    //     }
    //     else
    //         nested_types.emplace_back(DataTypeFactory::instance().get(child));
    // }
    //
    // if (names.empty())
    //     return std::make_shared<DataTypeTuple>(nested_types);
    // if (names.size() != nested_types.size())
    //     throw Exception(ErrorCodes::BAD_ARGUMENTS, "Names are specified not for all elements of Tuple type");
    // return std::make_shared<DataTypeTuple>(nested_types, names);
}


void registerDataTypeArrayT(DataTypeFactory & factory)
{
    factory.registerDataType("ArrayT", create);
}

}
