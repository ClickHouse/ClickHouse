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

    void toJSON(Poco::JSON::Object & object) const override;
    void fromJSON(const Poco::JSON::Object & object) override;

    const MutableSerializationInfoPtr & getElementInfo(size_t i) const { return elems[i]; }
    ISerialization::KindStack getElementKindStack(size_t i) const { return elems[i]->getKindStack(); }
    size_t getNumElements() const { return elems.size(); }

    /// Looks up an element's info by name; returns nullptr when there is no such element. Used by
    /// `SerializationStatisticsBuilder` to combine source-part counts matching elements by name.
    const MutableSerializationInfoPtr * tryGetElementInfo(const String & name) const
    {
        auto it = name_to_elem.find(name);
        return it == name_to_elem.end() ? nullptr : &it->second;
    }

protected:
    void writeJSONFields(WriteBuffer & out, const String * name, bool has_internal_statistics) const override;

private:
    MutableSerializationInfos elems;
    Names names;

    using NameToElem = std::unordered_map<String, MutableSerializationInfoPtr>;
    NameToElem name_to_elem;
};

}
