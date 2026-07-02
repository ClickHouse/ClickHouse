#pragma once
#include <Core/Names.h>
#include <DataTypes/Serializations/SerializationInfo.h>

namespace DB
{

class SerializationInfoTuple : public SerializationInfo
{
public:
    SerializationInfoTuple(MutableSerializationInfos elems_, Names names_);

    bool hasCustomSerialization() const override;
    bool structureEquals(const SerializationInfo & rhs) const override;

    MutableSerializationInfoPtr clone() const override;

    MutableSerializationInfoPtr createWithType(
        const IDataType & old_type,
        const IDataType & new_type,
        const Settings & new_settings) const override;

    void serialializeKindStackBinary(WriteBuffer & out) const override;
    void deserializeFromKindsBinary(ReadBuffer & in) override;

    void fromJSON(const Poco::JSON::Object & object, const String & key, Estimates & estimates) override;

    const MutableSerializationInfoPtr & getElementInfo(size_t i) const { return elems[i]; }
    ISerialization::KindStack getElementKindStack(size_t i) const { return elems[i]->getKindStack(); }
    size_t getNumElements() const { return elems.size(); }

    /// Names of the tuple elements, parallel to the element infos. Used to build subcolumn paths when
    /// walking the info tree together with a flat, path-keyed set of estimates.
    const Names & getElementNames() const { return names; }

    /// Looks up an element's info by name; returns nullptr when there is no such element.
    const MutableSerializationInfoPtr * tryGetElementInfo(const String & name) const
    {
        auto it = name_to_elem.find(name);
        return it == name_to_elem.end() ? nullptr : &it->second;
    }

protected:
    void writeJSONFields(WriteBuffer & out, const String * name, const String & key, const Estimates & estimates) const override;

private:
    MutableSerializationInfos elems;
    Names names;

    using NameToElem = std::unordered_map<String, MutableSerializationInfoPtr>;
    NameToElem name_to_elem;
};

}
