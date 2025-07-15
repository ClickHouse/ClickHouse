#pragma once

#include <Core/Types.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypeDecimalBase.h>

class DateLUTImpl;

namespace DB
{

/** DateTime64 is same as DateTime, but it stores values as Int64 and has configurable sub-second part.
 *
 * `scale` determines number of decimal places for sub-second part of the DateTime64.
  */
class DataTypeTime64 final : public DataTypeDecimalBase<Time64>, public TimezoneMixin
{
public:

    static constexpr int max_value = 63'539; // 999*60 + 59*60+ 59
    using Base = DataTypeDecimalBase<Time64>;
    static constexpr UInt8 default_scale = 3;

    static constexpr auto family_name = "Time64";
    static constexpr auto type_id = TypeIndex::Time64;

    explicit DataTypeTime64(UInt32 scale_, const std::string & time_zone_name = "");

    DataTypeTime64() : DataTypeTime64(default_scale, "") {}

    DataTypeTime64(UInt32 scale_, const TimezoneMixin & time_zone_info);

    const char * getFamilyName() const override { return family_name; }
    std::string doGetName() const override;
    void updateHashImpl(SipHash & hash) const override;
    TypeIndex getTypeId() const override { return type_id; }

    bool equals(const IDataType & rhs) const override;

    bool canBePromoted() const override { return false; }

    bool canBeUsedAsVersion() const override { return true; }

    bool isSummable() const override { return false; }

protected:
    SerializationPtr doGetDefaultSerialization() const override;
};

std::string getTimeTimezone(const IDataType & data_type);

}

