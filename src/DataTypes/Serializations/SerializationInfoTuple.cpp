#include <DataTypes/Serializations/SerializationInfoTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <Storages/Statistics/Estimate.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <IO/WriteHelpers.h>

#include <Poco/JSON/Object.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int THERE_IS_NO_COLUMN;
}

SerializationInfoTuple::SerializationInfoTuple(MutableSerializationInfos elems_, Names names_)
    /// Pass default settings because Tuple column cannot be sparse itself.
    : SerializationInfo({ISerialization::Kind::DEFAULT}, SerializationInfo::Settings{})
    , elems(std::move(elems_))
    , names(std::move(names_))
{
    chassert(names.size() == elems.size());
    for (size_t i = 0; i < names.size(); ++i)
        name_to_elem[names[i]] = elems[i];
}

bool SerializationInfoTuple::hasCustomSerialization() const
{
    return SerializationInfo::hasCustomSerialization() || std::any_of(elems.begin(), elems.end(), [](const auto & elem) { return elem->hasCustomSerialization(); });
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

MutableSerializationInfoPtr SerializationInfoTuple::clone() const
{
    MutableSerializationInfos elems_cloned;
    elems_cloned.reserve(elems.size());
    for (const auto & elem : elems)
        elems_cloned.push_back(elem ? elem->clone() : nullptr);

    return std::make_shared<SerializationInfoTuple>(std::move(elems_cloned), names);
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

    chassert(elems.size() == old_elements.size());
    chassert(elems.size() == new_elements.size());

    MutableSerializationInfos infos;
    infos.reserve(elems.size());
    for (size_t i = 0; i < elems.size(); ++i)
        infos.push_back(elems[i]->createWithType(*old_elements[i], *new_elements[i], new_settings));

    return std::make_shared<SerializationInfoTuple>(std::move(infos), names);
}

void SerializationInfoTuple::serialializeKindStackBinary(WriteBuffer & out) const
{
    SerializationInfo::serialializeKindStackBinary(out);
    for (const auto & elem : elems)
        elem->serialializeKindStackBinary(out);
}

void SerializationInfoTuple::deserializeFromKindsBinary(ReadBuffer & in)
{
    SerializationInfo::deserializeFromKindsBinary(in);
    for (const auto & elem : elems)
        elem->deserializeFromKindsBinary(in);
}

void SerializationInfoTuple::writeJSONFields(WriteBuffer & out, const String * name, const String & key, const Estimates & estimates) const
{
    SerializationInfo::writeJSONFields(out, name, key, estimates);
    writeString(R"(,"subcolumns":[)", out);

    bool first = true;
    for (size_t i = 0; i < elems.size(); ++i)
    {
        if (!first)
            writeChar(',', out);
        first = false;

        elems[i]->writeJSON(out, nullptr, subcolumnEstimateKey(key, names[i]), estimates);
    }

    writeChar(']', out);
}

void SerializationInfoTuple::fromJSON(const Poco::JSON::Object & object, const String & key, Estimates & estimates)
{
    SerializationInfo::fromJSON(object, key, estimates);

    if (!object.has("subcolumns"))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Missed field 'subcolumns' in SerializationInfo of columns SerializationInfoTuple");

    auto subcolumns = object.getArray("subcolumns");
    if (elems.size() != subcolumns->size())
        throw Exception(ErrorCodes::THERE_IS_NO_COLUMN,
            "Mismatched number of subcolumns between JSON and SerializationInfoTuple."
            "Expected: {}, got: {}", elems.size(), subcolumns->size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->fromJSON(*subcolumns->getObject(static_cast<unsigned>(i)), subcolumnEstimateKey(key, names[i]), estimates);
}

}
