#include <DataTypes/Serializations/SerializationInfoTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int THERE_IS_NO_COLUMN;
}

SerializationInfoTuple::SerializationInfoTuple(
    MutableSerializationInfos elems_, const Settings & settings_)
    : SerializationInfo(ISerialization::Kind::DEFAULT, settings_)
    , elems(std::move(elems_))
{
}

bool SerializationInfoTuple::hasCustomSerialization() const
{
    return std::any_of(elems.begin(), elems.end(), [](const auto & elem) { return elem->hasCustomSerialization(); });
}

void SerializationInfoTuple::add(const IColumn & column)
{
    SerializationInfo::add(column);

    const auto & column_tuple = assert_cast<const ColumnTuple &>(column);
    const auto & right_elems = column_tuple.getColumns();
    assert(elems.size() == right_elems.size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->add(*right_elems[i]);
}

void SerializationInfoTuple::add(const SerializationInfo & other)
{
    SerializationInfo::add(other);

    const auto & info_tuple = assert_cast<const SerializationInfoTuple &>(other);
    assert(elems.size() == info_tuple.elems.size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->add(*info_tuple.elems[i]);
}

void SerializationInfoTuple::replaceData(const SerializationInfo & other)
{
    SerializationInfo::add(other);

    const auto & info_tuple = assert_cast<const SerializationInfoTuple &>(other);
    assert(elems.size() == info_tuple.elems.size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->replaceData(*info_tuple.elems[i]);
}

MutableSerializationInfoPtr SerializationInfoTuple::clone() const
{
    MutableSerializationInfos elems_cloned;
    elems_cloned.reserve(elems.size());
    for (const auto & elem : elems)
        elems_cloned.push_back(elem->clone());

    return std::make_shared<SerializationInfoTuple>(std::move(elems_cloned), settings);
}

void SerializationInfoTuple::serialializeKindBinary(WriteBuffer & out) const
{
    SerializationInfo::serialializeKindBinary(out);
    for (const auto & elem : elems)
        elem->serialializeKindBinary(out);
}

void SerializationInfoTuple::deserializeFromKindsBinary(ReadBuffer & in)
{
    SerializationInfo::deserializeFromKindsBinary(in);
    for (const auto & elem : elems)
        elem->deserializeFromKindsBinary(in);
}

Poco::JSON::Object SerializationInfoTuple::toJSON() const
{
    auto object = SerializationInfo::toJSON();
    Poco::JSON::Array subcolumns;
    for (const auto & elem : elems)
        subcolumns.add(elem->toJSON());

    object.set("subcolumns", subcolumns);
    return object;
}

void SerializationInfoTuple::fromJSON(const Poco::JSON::Object & object)
{
    SerializationInfo::fromJSON(object);

    if (!object.has("subcolumns"))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Missed field '{}' in SerializationInfo of columns SerializationInfoTuple");

    auto subcolumns = object.getArray("subcolumns");
    if (elems.size() != subcolumns->size())
        throw Exception(ErrorCodes::THERE_IS_NO_COLUMN,
            "Mismatched number of subcolumns between JSON and SerializationInfoTuple."
            "Expected: {}, got: {}", elems.size(), subcolumns->size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->fromJSON(*subcolumns->getObject(i));
}

}
