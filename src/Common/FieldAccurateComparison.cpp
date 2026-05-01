#include <Common/FieldAccurateComparison.h>

#include <Core/Field.h>
#include <Core/AccurateComparison.h>
#include <Core/DecimalFunctions.h>
#include <Core/CompareHelper.h>
#include <base/demangle.h>
#include <Common/FieldVisitors.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
}

template <typename T>
static Float64 decimalFieldToFloat64(const DecimalField<T> & decimal)
{
    return DecimalUtils::convertTo<Float64>(decimal.getValue(), decimal.getScale());
}

/** More precise comparison, used for index.
  * Differs from Field::operator< and Field::operator== in that it also compares values of different types.
  * Comparison rules are same as in FunctionsComparison (to be consistent with expression evaluation in query).
  *
  * NaN policy: NaN != any value including NaN (same-type and cross-type).
  * This is consistent with ClickHouse sort order where NaN has a defined position.
  */
class FieldVisitorAccurateEquals : public StaticVisitor<bool>
{
public:
    template <typename T, typename U>
    bool operator() (const T & l, const U & r) const
    {
        if constexpr (std::is_same_v<T, Null> || std::is_same_v<U, Null>)
        {
            if constexpr (std::is_same_v<T, Null> && std::is_same_v<U, Null>)
                return l == r;
            return false;
        }
        else if constexpr (std::is_same_v<T, bool>)
        {
            return operator()(UInt8(l), r);
        }
        else if constexpr (std::is_same_v<U, bool>)
        {
            return operator()(l, UInt8(r));
        }
        else
        {
            if constexpr (std::is_same_v<T, U>)
            {
                if constexpr (std::is_floating_point_v<T>)
                    return accurate::equalsOp(l, r);
                else
                    return l == r;
            }

            if constexpr (is_arithmetic_v<T> && is_arithmetic_v<U>)
            {
                if constexpr (std::is_floating_point_v<T>)
                    if (isNaN(l)) return false;
                if constexpr (std::is_floating_point_v<U>)
                    if (isNaN(r)) return false;
                return accurate::equalsOp(l, r);
            }

            if constexpr (is_decimal_field<T> && is_decimal_field<U>)
                return l == r;

            /// Decimal vs Float: convert both to Float64 (same approach as FunctionsComparison).
            if constexpr (is_decimal_field<T> && is_floating_point<U>)
            {
                if (isNaN(r)) return false;
                return accurate::equalsOp(decimalFieldToFloat64(l), static_cast<Float64>(r));
            }

            if constexpr (is_floating_point<T> && is_decimal_field<U>)
            {
                if (isNaN(l)) return false;
                return accurate::equalsOp(static_cast<Float64>(l), decimalFieldToFloat64(r));
            }

            /// Decimal vs Integer: convert integer to Decimal256 for precise comparison.
            if constexpr (is_decimal_field<T> && is_integer<U>)
                return l == DecimalField<Decimal256>(Decimal256(r), 0);

            if constexpr (is_integer<T> && is_decimal_field<U>)
                return DecimalField<Decimal256>(Decimal256(l), 0) == r;

            if constexpr (std::is_same_v<T, String> && is_arithmetic_v<U>)
            {
                ReadBufferFromString in(l);
                U parsed;
                readText(parsed, in);
                return operator()(parsed, r);
            }

            if constexpr (std::is_same_v<U, String> && is_arithmetic_v<T>)
            {
                ReadBufferFromString in(r);
                T parsed;
                readText(parsed, in);
                return operator()(l, parsed);
            }
        }

        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Cannot compare {} with {}",
            demangle(typeid(T).name()), demangle(typeid(U).name()));
    }
};


/** Less-than comparison with NaN policy: NaN is greater than all values (nan_direction_hint = 1).
  * This is consistent with ClickHouse sort order and prevents Range::intersectsRange breakage.
  */
class FieldVisitorAccurateLess : public StaticVisitor<bool>
{
public:
    template <typename T, typename U>
    bool operator() (const T & l, const U & r) const
    {
        if constexpr (std::is_same_v<T, Null> && std::is_same_v<U, Null>)
        {
            return l.isNegativeInfinity() && r.isPositiveInfinity();
        }
        else if constexpr (std::is_same_v<T, Null>)
        {
            return l.isNegativeInfinity();
        }
        else if constexpr (std::is_same_v<U, Null>)
        {
            return r.isPositiveInfinity();
        }
        else if constexpr (std::is_same_v<T, bool>)
        {
            return operator()(UInt8(l), r);
        }
        else if constexpr (std::is_same_v<U, bool>)
        {
            return operator()(l, UInt8(r));
        }
        else
        {
            if constexpr (std::is_same_v<T, U>)
            {
                if constexpr (std::is_floating_point_v<T>)
                {
                    static constexpr int nan_direction_hint = 1;
                    return FloatCompareHelper<T>::less(l, r, nan_direction_hint);
                }
                else
                    return l < r;
            }

            if constexpr (is_arithmetic_v<T> && is_arithmetic_v<U>)
            {
                if constexpr (std::is_floating_point_v<T>)
                    if (isNaN(l)) return false; /// NaN is not less than anything
                if constexpr (std::is_floating_point_v<U>)
                    if (isNaN(r)) return true; /// everything is less than NaN
                return accurate::lessOp(l, r);
            }

            if constexpr (is_decimal_field<T> && is_decimal_field<U>)
                return l < r;

            /// Decimal vs Float: convert both to Float64 (same approach as FunctionsComparison).
            if constexpr (is_decimal_field<T> && is_floating_point<U>)
            {
                if (isNaN(r)) return true;  /// decimal is less than NaN
                return accurate::lessOp(decimalFieldToFloat64(l), static_cast<Float64>(r));
            }

            if constexpr (is_floating_point<T> && is_decimal_field<U>)
            {
                if (isNaN(l)) return false; /// NaN is not less than anything
                return accurate::lessOp(static_cast<Float64>(l), decimalFieldToFloat64(r));
            }

            /// Decimal vs Integer: convert integer to Decimal256 for precise comparison.
            if constexpr (is_decimal_field<T> && is_integer<U>)
                return l < DecimalField<Decimal256>(Decimal256(r), 0);

            if constexpr (is_integer<T> && is_decimal_field<U>)
                return DecimalField<Decimal256>(Decimal256(l), 0) < r;

            if constexpr (std::is_same_v<T, String> && is_arithmetic_v<U>)
            {
                ReadBufferFromString in(l);
                U parsed;
                readText(parsed, in);
                return operator()(parsed, r);
            }

            if constexpr (std::is_same_v<U, String> && is_arithmetic_v<T>)
            {
                ReadBufferFromString in(r);
                T parsed;
                readText(parsed, in);
                return operator()(l, parsed);
            }
        }

        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Cannot compare {} with {}",
            demangle(typeid(T).name()), demangle(typeid(U).name()));
    }
};


class FieldVisitorAccurateLessOrEqual : public StaticVisitor<bool>
{
public:
    template <typename T, typename U>
    bool operator()(const T & l, const U & r) const
    {
        return !FieldVisitorAccurateLess()(r, l);
    }
};

bool accurateEquals(const Field & left, const Field & right)
{
    return applyVisitor(FieldVisitorAccurateEquals(), left, right);
}

bool accurateLess(const Field & left, const Field & right)
{
    return applyVisitor(FieldVisitorAccurateLess(), left, right);
}

bool accurateLessOrEqual(const Field & left, const Field & right)
{
    return applyVisitor(FieldVisitorAccurateLessOrEqual(), left, right);
}

}
