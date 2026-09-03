#include <DataTypes/DataTypeRow.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
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

#include <algorithm>
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
    return SerializationRow::create(std::move(field_serializations), names);
}

bool DataTypeRow::isComparable() const
{
    return std::all_of(elems.begin(), elems.end(), [](const auto & elem) { return elem->isComparable(); });
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


DataTypePtr lowerRowTypesToTuples(const DataTypePtr & type)
{
    if (const auto * row_type = typeid_cast<const DataTypeRow *>(type.get()))
    {
        DataTypes elements = row_type->getElements();
        for (auto & element : elements)
            element = lowerRowTypesToTuples(element);
        return std::make_shared<DataTypeTuple>(elements, row_type->getElementNames());
    }
    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        DataTypes elements = tuple_type->getElements();
        bool changed = false;
        for (auto & element : elements)
        {
            auto lowered = lowerRowTypesToTuples(element);
            changed |= lowered.get() != element.get();
            element = std::move(lowered);
        }
        if (!changed)
            return type;
        return tuple_type->hasExplicitNames()
            ? std::make_shared<DataTypeTuple>(elements, tuple_type->getElementNames())
            : std::make_shared<DataTypeTuple>(elements);
    }
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        auto lowered = lowerRowTypesToTuples(array_type->getNestedType());
        if (lowered.get() == array_type->getNestedType().get())
            return type;
        return std::make_shared<DataTypeArray>(lowered);
    }
    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        auto lowered_key = lowerRowTypesToTuples(map_type->getKeyType());
        auto lowered_value = lowerRowTypesToTuples(map_type->getValueType());
        if (lowered_key.get() == map_type->getKeyType().get() && lowered_value.get() == map_type->getValueType().get())
            return type;
        return std::make_shared<DataTypeMap>(lowered_key, lowered_value);
    }
    return type;
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
    factory.registerDataType("Row", create, DataTypeFactory::Case::Sensitive, Documentation{
            .description = R"DOCS_MD(
`Row(name1 T1, name2 T2, ...)` is a named, ordered bundle of typed fields stored as a SINGLE physical column on disk -
one length-prefixed binary record per row - in contrast to `Tuple`, which produces one physical column file per element.

The intent is to bundle columns that are frequently read together, so that the storage layer pays one `open`/`seek` per
granule instead of one per column. This helps low-latency, narrow `SELECT` workloads such as recent-logs or
single-trace lookups; wide full scans are better left columnar.

All fields must be named, and the names must be unique within the `Row`.

A `Row` column is typically declared with a `MATERIALIZED` expression mirroring its field list, so it is populated
automatically on insert while the source columns keep their own columnar storage:

```sql
CREATE TABLE logs
(
    ts        DateTime,
    level     LowCardinality(String),
    msg       String,
    host      String,
    combined  Row(level LowCardinality(String), msg String, host String)
        MATERIALIZED (level, msg, host)
)
ENGINE = MergeTree ORDER BY ts;
```

When a query requests at least two of the wrapped columns, the query plan reads them from the single `combined` stream
instead of one file per column. The setting `query_plan_use_row_wrappers` turns that rewrite off.
)DOCS_MD",
            .syntax = "Row(name1 T1, name2 T2, ...)",
            .examples = {},
            .related = {"Tuple"},
        });
}

}
