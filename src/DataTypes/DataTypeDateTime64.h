#pragma once

#include <Core/Types.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDecimalBase.h>

class DateLUTImpl;

namespace DB
{

/** DateTime64 is same as DateTime, but it stores values as Int64 and has configurable sub-second part.
 *
 * `scale` determines number of decimal places for sub-second part of the DateTime64.
  */
class DataTypeDateTime64 final : public DataTypeDecimalBase<DateTime64>, public TimezoneMixin
{
public:
    using Base = DataTypeDecimalBase<DateTime64>;
    static constexpr UInt8 default_scale = 3;

    static constexpr auto family_name = "DateTime64";
    static constexpr auto type_id = TypeIndex::DateTime64;

    explicit DataTypeDateTime64(UInt32 scale_, const std::string & time_zone_name = "");

    // reuse timezone from other DateTime/DateTime64
    DataTypeDateTime64(UInt32 scale_, const TimezoneMixin & time_zone_info);

    const char * getFamilyName() const override { return family_name; }
    std::string doGetName() const override;
    TypeIndex getTypeId() const override { return type_id; }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    bool equals(const IDataType & rhs) const override;

    bool canBePromoted() const override { return false; }
};

}

