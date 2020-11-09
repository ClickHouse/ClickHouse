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
    void serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const override;
    void deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const override;

    bool equals(const IDataType & rhs) const override;

    bool canBePromoted() const override { return false; }
};

/** Tansform-type wrapper for DateTime64, applies given Transform to DateTime64 value or only to a whole part of it.
 *
 * Depending on what overloads of Transform::execute() are available, when called with DateTime64 value,
 * invokes Transform::execute() with:
 * * whole part of DateTime64 value, discarding fractional part.
 * * DateTime64 value and scale factor.
 *
 * Suitable Transfotm-types are commonly used in Date/DateTime manipulation functions,
 * and should implement static (or const) fucntion with following signatures:
 *      R execute(UInt32 whole_value, ... , const TimeZoneImpl &)
 * OR
 *      R execute(DateTime64 value, Int64 scale_factor, ... , const TimeZoneImpl &)
 *
 * Where R and T could be arbitrary types.
*/
template <typename Transform>
class TransformDateTime64 : public Transform
{
private:
    // Detect if Transform::execute is const or static method
    // with signature defined by template args (ignoring result type).
    template<typename = void, typename... Args>
    struct TransformHasExecuteOverload : std::false_type {};

    template<typename... Args>
    struct TransformHasExecuteOverload<std::void_t<decltype(std::declval<Transform>().execute(std::declval<Args>()...))>, Args...>
        : std::true_type {};

    template<typename... Args>
    static constexpr bool TransformHasExecuteOverload_v = TransformHasExecuteOverload<void, Args...>::value;

public:
    static constexpr auto name = Transform::name;

    using Transform::execute;

    // non-explicit constructor to allow creating from scale value (or with no scale at all), indispensable in some contexts.
    TransformDateTime64(UInt32 scale_ = 0)
        : scale_multiplier(DecimalUtils::scaleMultiplier<DateTime64::NativeType>(scale_))
    {}

    template <typename ... Args>
    inline auto execute(const DateTime64 & t, Args && ... args) const
    {
        const auto transform = static_cast<const Transform *>(this);

        if constexpr (TransformHasExecuteOverload_v<DateTime64, decltype(scale_multiplier), Args...>)
        {
            return transform->execute(t, scale_multiplier, std::forward<Args>(args)...);
        }
        else
        {
            const auto components = DecimalUtils::splitWithScaleMultiplier(t, scale_multiplier);
            return transform->execute(static_cast<UInt32>(components.whole), std::forward<Args>(args)...);
        }
    }

private:
    DateTime64::NativeType scale_multiplier = 1;
};

}

