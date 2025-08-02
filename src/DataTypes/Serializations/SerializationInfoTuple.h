#pragma once
#include <Core/Names.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Poco/JSON/Object.h>

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

    void serialializeKindBinary(WriteBuffer & out) const override;
    void deserializeFromKindsBinary(ReadBuffer & in) override;

    void toJSON(Poco::JSON::Object & object) const override;
    void toJSONWithStats(Poco::JSON::Object & object, const StatisticsInfo & stats) const override;
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
