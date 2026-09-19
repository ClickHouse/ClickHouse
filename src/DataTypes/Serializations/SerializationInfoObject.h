#pragma once

#include <DataTypes/Serializations/SerializationInfoNamed.h>

namespace DB
{

class SerializationInfoObject final : public SerializationInfoNamed
{
public:
    using SerializationInfoNamed::SerializationInfoNamed;

    void add(const IColumn & column) override;
    MutableSerializationInfoPtr clone() const override;
    MutableSerializationInfoPtr createWithType(
        const IDataType & old_type,
        const IDataType & new_type,
        const Settings & new_settings) const override;

    const MutableSerializationInfoPtr & getTypedPathInfo(const String & path) const { return getElementInfo(path); }
};

}
