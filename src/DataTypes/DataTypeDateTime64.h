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
    static constexpr UInt8 default_scale = 3;
    static constexpr auto family_name = "DateTime64";

    explicit DataTypeDateTime64(UInt32 scale_, const std::string & time_zone_name = "");

    // reuse timezone from other DateTime/DateTime64
    DataTypeDateTime64(UInt32 scale_, const TimezoneMixin & time_zone_info);

    const char * getFamilyName() const override { return family_name; }
    std::string doGetName() const override;
    TypeIndex getTypeId() const override { return TypeIndex::DateTime64; }

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
    void serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const override;
    void deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const override;

    bool equals(const IDataType & rhs) const override;
};


/** Basic wrapper for Tansform-types for DateTime64.
 *
 * Allows reusing existing Transform (that takes DateTime-values as UInt32) with DateTime64-values,
 * by discarding fractional part and producing SAME return type as original Transform.
 *
 * Such Transfotm-types are commonly used in Date/DateTime manipulation functions,
 * and implement static execute fucntion with following signature:
 *      R execute(UInt32, T, const DateLUTImpl &)
 *
 * Wehere R and T could be arbitrary types.
*/
template <typename Transform>
class DateTime64BasicTransformWrapper : public Transform
{
public:
    using Transform::execute;

    explicit DateTime64BasicTransformWrapper(UInt32 scale_)
        : scale_multiplier(DecimalUtils::scaleMultiplier<DateTime64::NativeType>(scale_))
    {}

    template <typename T>
    auto execute(DateTime64 t, T v, const DateLUTImpl & time_zone) const
    {
        const auto components = DecimalUtils::splitWithScaleMultiplier(t, scale_multiplier);
        return static_cast<const Transform *>(this)->execute(
            static_cast<UInt32>(components.whole), v, time_zone);
    }

private:
    UInt32 scale_multiplier = 1;
};


}

