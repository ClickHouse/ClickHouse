#pragma once
#include <DataTypes/Serializations/SerializationInfoNamed.h>

namespace DB
{

class SerializationInfoTuple final : public SerializationInfoNamed
{
public:
    SerializationInfoTuple(MutableSerializationInfos elems_, Names names_, const Settings & settings_ = {});

    [[nodiscard]] bool structureEquals(const SerializationInfo & rhs) const override;
    void add(const IColumn & column) override;

    [[nodiscard]] MutableSerializationInfoPtr clone() const override;

    [[nodiscard]] MutableSerializationInfoPtr createWithType(
        const IDataType & old_type,
        const IDataType & new_type,
        const Settings & new_settings) const override;

    [[nodiscard]] ISerialization::KindStack getElementKindStack(size_t i) const { return elems[i]->getKindStack(); }
};

}
