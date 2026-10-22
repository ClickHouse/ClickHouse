#include <Core/SettingsFieldsConversionHelpers.h>

#include <Core/AccurateComparison.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>
#include <Common/NaNUtils.h>
#include <base/demangle.h>

#include <boost/algorithm/string/predicate.hpp>

#include <cmath>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_BOOL;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_CONVERT_TYPE;
}

bool stringToBoolSettingValue(const String & str)
{
    if (str == "0")
        return false;
    if (str == "1")
        return true;
    if (boost::iequals(str, "false"))
        return false;
    if (boost::iequals(str, "true"))
        return true;
    throw Exception(ErrorCodes::CANNOT_PARSE_BOOL, "Cannot parse bool from string '{}'", str);
}

template <typename T>
void validateFloatingPointSettingValue(T value)
{
    if constexpr (std::is_floating_point_v<T>)
    {
        if (!std::isfinite(value))
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER,
                "Float setting value must be finite, got {}", value);
    }
}

template <typename T>
T stringToNumberSettingValue(const String & str)
{
    if constexpr (std::is_same_v<T, bool>)
    {
        return stringToBoolSettingValue(str);
    }
    else
    {
        T value = parseWithSizeSuffix<T>(str);
        validateFloatingPointSettingValue(value);
        return value;
    }
}

template <typename T>
T fieldToNumberSettingValue(const Field & f)
{
    if (f.getType() == Field::Types::String)
    {
        return stringToNumberSettingValue<T>(f.safeGet<String>());
    }
    if (f.getType() == Field::Types::UInt64)
    {
        T result;
        if (!accurate::convertNumeric(f.safeGet<UInt64>(), result))
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                            "Field value {} is out of range of {} type", f, demangle(typeid(T).name()));
        validateFloatingPointSettingValue(result);
        return result;
    }
    if (f.getType() == Field::Types::Int64)
    {
        T result;
        if (!accurate::convertNumeric(f.safeGet<Int64>(), result))
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                            "Field value {} is out of range of {} type", f, demangle(typeid(T).name()));
        validateFloatingPointSettingValue(result);
        return result;
    }
    if (f.getType() == Field::Types::Bool)
    {
        return T(f.safeGet<bool>());
    }
    if (f.getType() == Field::Types::Float64)
    {
        Float64 x = f.safeGet<Float64>();
        validateFloatingPointSettingValue(x);
        if constexpr (std::is_floating_point_v<T>)
        {
            return T(x);
        }
        else
        {
            if (!isFinite(x))
            {
                /// Conversion of infinite values to integer is undefined.
                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert infinite value to integer type");
            }
            /// Use precision-correct float-vs-integer comparison via `accurate::greaterOp` / `accurate::lessOp`.
            /// A naive `x > Float64(numeric_limits<T>::max())` is wrong for wide integer types like `UInt64`:
            /// `Float64(numeric_limits<UInt64>::max())` rounds UP to `2^64`, so a `Float64` value equal to
            /// that rounded-up boundary slips through the check and produces undefined behavior in the
            /// subsequent `static_cast<T>(x)`. See issue #103817.
            ///
            /// Bool is special-cased: `numeric_limits<bool>` is exactly representable in `Float64`, and
            /// `accurate::lessOp` would fail to instantiate for `bool` (`make_unsigned_t<bool>` is ill-formed).
            if constexpr (std::is_same_v<T, bool>)
            {
                if (x > Float64(std::numeric_limits<T>::max()) || x < Float64(std::numeric_limits<T>::lowest()))
                    throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert out of range floating point value to integer type");
            }
            else if (accurate::greaterOp(x, std::numeric_limits<T>::max())
                     || accurate::lessOp(x, std::numeric_limits<T>::lowest()))
            {
                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert out of range floating point value to integer type");
            }
            return T(x);
        }
    }
    else
        throw Exception(
            ErrorCodes::CANNOT_CONVERT_TYPE, "Invalid value {} of the setting, which needs {}", f, demangle(typeid(T).name()));
}

template void validateFloatingPointSettingValue<UInt64>(UInt64);
template void validateFloatingPointSettingValue<Int64>(Int64);
template void validateFloatingPointSettingValue<Int32>(Int32);
template void validateFloatingPointSettingValue<UInt32>(UInt32);
template void validateFloatingPointSettingValue<float>(float);
template void validateFloatingPointSettingValue<double>(double);
template void validateFloatingPointSettingValue<bool>(bool);

template UInt64 stringToNumberSettingValue<UInt64>(const String &);
template Int64 stringToNumberSettingValue<Int64>(const String &);
template Int32 stringToNumberSettingValue<Int32>(const String &);
template UInt32 stringToNumberSettingValue<UInt32>(const String &);
template float stringToNumberSettingValue<float>(const String &);
template double stringToNumberSettingValue<double>(const String &);
template bool stringToNumberSettingValue<bool>(const String &);

template UInt64 fieldToNumberSettingValue<UInt64>(const Field &);
template Int64 fieldToNumberSettingValue<Int64>(const Field &);
template Int32 fieldToNumberSettingValue<Int32>(const Field &);
template UInt32 fieldToNumberSettingValue<UInt32>(const Field &);
template float fieldToNumberSettingValue<float>(const Field &);
template double fieldToNumberSettingValue<double>(const Field &);
template bool fieldToNumberSettingValue<bool>(const Field &);

}
