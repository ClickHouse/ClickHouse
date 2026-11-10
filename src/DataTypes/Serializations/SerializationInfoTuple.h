#pragma once
#include <DataTypes/Serializations/SerializationInfoNamed.h>

namespace DB
{

class SerializationInfoTuple final : public SerializationInfoNamed
{
public:
    SerializationInfoTuple(MutableSerializationInfos elems_, Names names_, const Settings & settings_ = {});

    bool structureEquals(const SerializationInfo & rhs) const override;
    void add(const IColumn & column) override;

    MutableSerializationInfoPtr clone() const override;

    MutableSerializationInfoPtr createWithType(
        const IDataType & old_type,
        const IDataType & new_type,
        const Settings & new_settings) const override;

    ISerialization::KindStack getElementKindStack(size_t i) const { return elems[i]->getKindStack(); }
};

}
