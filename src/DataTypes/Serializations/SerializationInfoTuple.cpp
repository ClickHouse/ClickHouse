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
    extern const int NOT_IMPLEMENTED;
}

SerializationInfoTuple::SerializationInfoTuple(
    MutableSerializationInfos elems_, Names names_, const Settings & settings_)
    : SerializationInfo(ISerialization::Kind::DEFAULT, settings_)
    , elems(std::move(elems_))
    , names(std::move(names_))
{
    assert(names.size() == elems.size());
    for (size_t i = 0; i < names.size(); ++i)
        name_to_elem[names[i]] = elems[i];
}

bool SerializationInfoTuple::hasCustomSerialization() const
{
    return std::any_of(elems.begin(), elems.end(), [](const auto & elem) { return elem->hasCustomSerialization(); });
}

bool SerializationInfoTuple::structureEquals(const SerializationInfo & rhs) const
{
    const auto * rhs_tuple = typeid_cast<const SerializationInfoTuple *>(&rhs);
    if (!rhs_tuple || elems.size() != rhs_tuple->elems.size())
        return false;

    for (size_t i = 0; i < elems.size(); ++i)
        if (!elems[i]->structureEquals(*rhs_tuple->elems[i]))
            return false;

    return true;
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

    const auto & other_info = assert_cast<const SerializationInfoTuple &>(other);
    for (const auto & [name, elem] : name_to_elem)
    {
        auto it = other_info.name_to_elem.find(name);
        if (it != other_info.name_to_elem.end())
            elem->add(*it->second);
        else
            elem->addDefaults(other_info.getData().num_rows);
    }
}

void SerializationInfoTuple::remove(const SerializationInfo & other)
{
    if (!structureEquals(other))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot remove from serialization info different structure");

    SerializationInfo::remove(other);
    const auto & other_elems = assert_cast<const SerializationInfoTuple &>(other).elems;
    chassert(elems.size() == other_elems.size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->remove(*other_elems[i]);
}

void SerializationInfoTuple::addDefaults(size_t length)
{
    SerializationInfo::addDefaults(length);

    for (const auto & elem : elems)
        elem->addDefaults(length);
}

void SerializationInfoTuple::replaceData(const SerializationInfo & other)
{
    SerializationInfo::replaceData(other);

    const auto & other_info = assert_cast<const SerializationInfoTuple &>(other);
    for (const auto & [name, elem] : name_to_elem)
    {
        auto it = other_info.name_to_elem.find(name);
        if (it != other_info.name_to_elem.end())
            elem->replaceData(*it->second);
    }
}

MutableSerializationInfoPtr SerializationInfoTuple::clone() const
{
    MutableSerializationInfos elems_cloned;
    elems_cloned.reserve(elems.size());
    for (const auto & elem : elems)
        elems_cloned.push_back(elem->clone());

    auto ret = std::make_shared<SerializationInfoTuple>(std::move(elems_cloned), names, settings);
    ret->data = data;
    return ret;
}

MutableSerializationInfoPtr SerializationInfoTuple::createWithType(
    const IDataType & old_type,
    const IDataType & new_type,
    const Settings & new_settings) const
{
    const auto & old_tuple = assert_cast<const DataTypeTuple &>(old_type);
    const auto & new_tuple = assert_cast<const DataTypeTuple &>(new_type);

    const auto & old_elements = old_tuple.getElements();
    const auto & new_elements = new_tuple.getElements();

    assert(elems.size() == old_elements.size());
    assert(elems.size() == new_elements.size());

    MutableSerializationInfos infos;
    infos.reserve(elems.size());
    for (size_t i = 0; i < elems.size(); ++i)
        infos.push_back(elems[i]->createWithType(*old_elements[i], *new_elements[i], new_settings));

    return std::make_shared<SerializationInfoTuple>(std::move(infos), names, new_settings);
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
            "Missed field 'subcolumns' in SerializationInfo of columns SerializationInfoTuple");

    auto subcolumns = object.getArray("subcolumns");
    if (elems.size() != subcolumns->size())
        throw Exception(ErrorCodes::THERE_IS_NO_COLUMN,
            "Mismatched number of subcolumns between JSON and SerializationInfoTuple."
            "Expected: {}, got: {}", elems.size(), subcolumns->size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->fromJSON(*subcolumns->getObject(static_cast<unsigned>(i)));
}

}
