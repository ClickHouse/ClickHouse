#pragma once
#include <Core/Names.h>
#include <DataTypes/Serializations/SerializationInfo.h>

namespace DB
{

class SerializationInfoTuple : public SerializationInfo
{
public:
    SerializationInfoTuple(MutableSerializationInfos elems_, Names names_, const Settings & settings_);

    bool hasCustomSerialization() const override;

    void add(const IColumn & column) override;
    void add(const SerializationInfo & other) override;
    void addDefaults(size_t length) override;
    void replaceData(const SerializationInfo & other) override;

    MutableSerializationInfoPtr clone() const override;

    void serialializeKindBinary(WriteBuffer & out) const override;
    void deserializeFromKindsBinary(ReadBuffer & in) override;

    Poco::JSON::Object toJSON() const override;
    void fromJSON(const Poco::JSON::Object & object) override;

    const MutableSerializationInfoPtr & getElementInfo(size_t i) const { return elems[i]; }
    ISerialization::Kind getElementKind(size_t i) const { return elems[i]->getKind(); }

private:
    MutableSerializationInfos elems;
    Names names;

    using NameToElem = std::unordered_map<String, MutableSerializationInfoPtr>;
    NameToElem name_to_elem;
};

}
