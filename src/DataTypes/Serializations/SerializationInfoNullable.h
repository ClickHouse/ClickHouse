#pragma once

#include <DataTypes/Serializations/SerializationInfo.h>

namespace DB
{

class SerializationInfoNullable final : public SerializationInfo
{
public:
    explicit SerializationInfoNullable(MutableSerializationInfoPtr nested_, const Settings & settings_);

    bool hasCustomSerialization() const override;
    bool structureEquals(const SerializationInfo & rhs) const override;

    void add(const IColumn & column) override;
    void add(const SerializationInfo & other) override;
    void remove(const SerializationInfo & other) override;
    void addDefaults(size_t length) override;
    void replaceData(const SerializationInfo & other) override;

    MutableSerializationInfoPtr clone() const override;
    MutableSerializationInfoPtr createWithType(
        const IDataType & old_type,
        const IDataType & new_type,
        const Settings & new_settings) const override;

    void serialializeKindStackBinary(WriteBuffer & out) const override;
    void deserializeFromKindsBinary(ReadBuffer & in) override;
    void writeJSON(WriteBuffer & out, const String * name) const override;
    void toJSON(Poco::JSON::Object & object) const override;
    void fromJSON(const Poco::JSON::Object & object) override;

    const MutableSerializationInfoPtr & getNestedInfo() const { return nested; }

private:
    void syncData();

    MutableSerializationInfoPtr nested;
};

bool canReuseSerializationInfoForTypeChange(const SerializationInfo & old_info, const SerializationInfo & new_info);

MutableSerializationInfoPtr tryReuseSerializationInfoThroughNullable(
    const SerializationInfo & old_info,
    const IDataType & old_type,
    const MutableSerializationInfoPtr & new_info,
    const IDataType & new_type,
    const SerializationInfoSettings & new_settings);

}
