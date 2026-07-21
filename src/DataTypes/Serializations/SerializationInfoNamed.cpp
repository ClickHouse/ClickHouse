#include <DataTypes/Serializations/SerializationInfoNamed.h>

#include <Common/Exception.h>
#include <IO/WriteHelpers.h>

#include <Poco/JSON/Object.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int THERE_IS_NO_COLUMN;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

SerializationInfoSettings withoutKindSelection(SerializationInfoSettings settings)
{
    settings.choose_kind = false;
    return settings;
}

}

SerializationInfoNamed::SerializationInfoNamed(MutableSerializationInfos elems_, Names names_, const Settings & settings_)
    : SerializationInfo({ISerialization::Kind::DEFAULT}, withoutKindSelection(settings_))
    , elems(std::move(elems_))
    , names(std::move(names_))
{
    chassert(names.size() == elems.size());
    for (size_t i = 0; i < names.size(); ++i)
        name_to_elem[names[i]] = elems[i];
}

bool SerializationInfoNamed::hasCustomSerialization() const
{
    return SerializationInfo::hasCustomSerialization()
        || std::any_of(elems.begin(), elems.end(), [](const auto & elem) { return elem->hasCustomSerialization(); });
}

bool SerializationInfoNamed::structureEquals(const SerializationInfo & rhs) const
{
    if (typeid(*this) != typeid(rhs))
        return false;

    const auto & rhs_named = static_cast<const SerializationInfoNamed &>(rhs);
    if (names != rhs_named.names || elems.size() != rhs_named.elems.size())
        return false;

    for (size_t i = 0; i < elems.size(); ++i)
        if (!elems[i]->structureEquals(*rhs_named.elems[i]))
            return false;

    return true;
}

void SerializationInfoNamed::add(const IColumn & column)
{
    SerializationInfo::add(column);
}

void SerializationInfoNamed::add(const SerializationInfo & other)
{
    SerializationInfo::add(other);

    if (typeid(*this) != typeid(other))
        return;

    const auto & other_named = static_cast<const SerializationInfoNamed &>(other);
    for (const auto & [name, elem] : name_to_elem)
    {
        auto it = other_named.name_to_elem.find(name);
        if (it != other_named.name_to_elem.end())
            elem->add(*it->second);
        else
            elem->addDefaults(other_named.getData().num_rows);
    }
}

void SerializationInfoNamed::remove(const SerializationInfo & other)
{
    if (!structureEquals(other))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot remove from serialization info with different named subcolumns");

    SerializationInfo::remove(other);
    const auto & other_elems = static_cast<const SerializationInfoNamed &>(other).elems;
    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->remove(*other_elems[i]);
}

void SerializationInfoNamed::addDefaults(size_t length)
{
    SerializationInfo::addDefaults(length);
    for (const auto & elem : elems)
        elem->addDefaults(length);
}

void SerializationInfoNamed::replaceData(const SerializationInfo & other)
{
    SerializationInfo::replaceData(other);

    if (typeid(*this) != typeid(other))
        return;

    const auto & other_named = static_cast<const SerializationInfoNamed &>(other);
    for (const auto & [name, elem] : name_to_elem)
    {
        auto it = other_named.name_to_elem.find(name);
        if (it != other_named.name_to_elem.end())
            elem->replaceData(*it->second);
    }
}

MutableSerializationInfos SerializationInfoNamed::cloneElements() const
{
    MutableSerializationInfos cloned;
    cloned.reserve(elems.size());
    for (const auto & elem : elems)
        cloned.push_back(elem ? elem->clone() : nullptr);
    return cloned;
}

void SerializationInfoNamed::serialializeKindStackBinary(WriteBuffer & out) const
{
    SerializationInfo::serialializeKindStackBinary(out);
    for (const auto & elem : elems)
        elem->serialializeKindStackBinary(out);
}

void SerializationInfoNamed::deserializeFromKindsBinary(ReadBuffer & in)
{
    SerializationInfo::deserializeFromKindsBinary(in);
    for (const auto & elem : elems)
        elem->deserializeFromKindsBinary(in);
}

void SerializationInfoNamed::writeJSONFields(WriteBuffer & out, const String * name) const
{
    SerializationInfo::writeJSONFields(out, name);
    writeString(R"(,"subcolumns":[)", out);

    for (size_t i = 0; i < elems.size(); ++i)
    {
        if (i)
            writeChar(',', out);
        elems[i]->writeJSON(out, nullptr);
    }

    writeChar(']', out);
}

void SerializationInfoNamed::toJSON(Poco::JSON::Object & object) const
{
    SerializationInfo::toJSON(object);
    Poco::JSON::Array subcolumns;
    for (const auto & elem : elems)
    {
        Poco::JSON::Object subcolumn;
        elem->toJSON(subcolumn);
        subcolumns.add(subcolumn);
    }
    object.set("subcolumns", subcolumns);
}

void SerializationInfoNamed::fromJSON(const Poco::JSON::Object & object)
{
    SerializationInfo::fromJSON(object);

    if (!object.has("subcolumns"))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Missing field 'subcolumns' in serialization info for named subcolumns");

    auto subcolumns = object.getArray("subcolumns");
    if (elems.size() != subcolumns->size())
        throw Exception(
            ErrorCodes::THERE_IS_NO_COLUMN,
            "Mismatched number of named subcolumns in serialization info. Expected: {}, got: {}",
            elems.size(),
            subcolumns->size());

    for (size_t i = 0; i < elems.size(); ++i)
        elems[i]->fromJSON(*subcolumns->getObject(static_cast<unsigned>(i)));
}

const MutableSerializationInfoPtr & SerializationInfoNamed::getElementInfo(const String & name) const
{
    auto it = name_to_elem.find(name);
    if (it == name_to_elem.end())
        throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Subcolumn '{}' is missing in serialization info", name);
    return it->second;
}

}
