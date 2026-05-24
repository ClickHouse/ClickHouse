#include <DataTypes/DataTypeRow.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationRow.h>
#include <Columns/ColumnTuple.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <Core/Field.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTNameTypePair.h>

#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DUPLICATE_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


DataTypeRow::DataTypeRow(const DataTypes & elems_, const Strings & names_)
    : elems(elems_), names(names_)
{
    if (elems.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Row data type must have at least one field");

    if (elems.size() != names.size())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of types ({}) does not match number of names ({}) in Row", elems.size(), names.size());

    std::unordered_set<String> seen;
    for (const auto & name : names)
    {
        if (name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Row field names cannot be empty");
        if (!seen.insert(name).second)
            throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Duplicate field name in Row: {}", name);
    }
}

std::string DataTypeRow::doGetName() const
{
    WriteBufferFromOwnString s;
    s << "Row(";
    for (size_t i = 0; i < elems.size(); ++i)
    {
        if (i != 0)
            s << ", ";
        s << backQuoteIfNeed(names[i]) << ' ' << elems[i]->getName();
    }
    s << ")";
    return s.str();
}

MutableColumnPtr DataTypeRow::createColumn() const
{
    MutableColumns inner(elems.size());
    for (size_t i = 0; i < elems.size(); ++i)
        inner[i] = elems[i]->createColumn();
    return ColumnTuple::create(std::move(inner));
}

Field DataTypeRow::getDefault() const
{
    Tuple t;
    t.reserve(elems.size());
    for (const auto & e : elems)
        t.push_back(e->getDefault());
    return Field(std::move(t));
}

void DataTypeRow::insertDefaultInto(IColumn & column) const
{
    auto & tuple = assert_cast<ColumnTuple &>(column);
    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->insertDefaultInto(tuple.getColumn(i));
}

bool DataTypeRow::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;
    const auto & r = static_cast<const DataTypeRow &>(rhs);
    if (elems.size() != r.elems.size())
        return false;
    for (size_t i = 0; i < elems.size(); ++i)
        if (names[i] != r.names[i] || !elems[i]->equals(*r.elems[i]))
            return false;
    return true;
}

bool DataTypeRow::textCanContainOnlyValidUTF8() const
{
    for (const auto & e : elems)
        if (!e->textCanContainOnlyValidUTF8())
            return false;
    return true;
}

bool DataTypeRow::haveMaximumSizeOfValue() const
{
    for (const auto & e : elems)
        if (!e->haveMaximumSizeOfValue())
            return false;
    return true;
}

size_t DataTypeRow::getMaximumSizeOfValueInMemory() const
{
    size_t r = 0;
    for (const auto & e : elems)
        r += e->getMaximumSizeOfValueInMemory();
    return r;
}

size_t DataTypeRow::getSizeOfValueInMemory() const
{
    size_t r = 0;
    for (const auto & e : elems)
        r += e->getSizeOfValueInMemory();
    return r;
}

SerializationPtr DataTypeRow::doGetSerialization(const SerializationInfoSettings & settings) const
{
    Serializations field_serializations;
    field_serializations.reserve(elems.size());
    for (const auto & e : elems)
        field_serializations.push_back(e->getSerialization(settings));
    return std::make_shared<SerializationRow>(std::move(field_serializations), names);
}

DataTypePtr DataTypeRow::getNormalizedType() const
{
    DataTypes ne;
    ne.reserve(elems.size());
    for (const auto & e : elems)
        ne.push_back(e->getNormalizedType());
    return std::make_shared<DataTypeRow>(ne, names);
}

void DataTypeRow::forEachChild(const ChildCallback & callback) const
{
    for (const auto & e : elems)
    {
        callback(*e);
        e->forEachChild(callback);
    }
}

void DataTypeRow::updateHashImpl(SipHash & hash) const
{
    hash.update(elems.size());
    for (size_t i = 0; i < elems.size(); ++i)
    {
        hash.update(names[i]);
        elems[i]->updateHash(hash);
    }
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Row data type requires at least one named field");

    DataTypes nested_types;
    Strings names;
    nested_types.reserve(arguments->children.size());
    names.reserve(arguments->children.size());

    for (const ASTPtr & child : arguments->children)
    {
        const auto * pair = child->as<ASTNameTypePair>();
        if (!pair)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Row data type requires all fields to be named (e.g. Row(a String, b Int))");
        nested_types.emplace_back(DataTypeFactory::instance().get(pair->type));
        names.emplace_back(pair->name);
    }

    return std::make_shared<DataTypeRow>(nested_types, names);
}


void registerDataTypeRow(DataTypeFactory & factory)
{
    factory.registerDataType("Row", create);
}

}
