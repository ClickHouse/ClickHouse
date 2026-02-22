#include <Common/FieldAccurateComparison.h>

#include <Core/Field.h>
#include <Core/AccurateComparison.h>
#include <base/demangle.h>
#include <Common/FieldVisitors.h>
#include <Common/NaNUtils.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
}

/** More precise comparison, used for index.
  * Differs from Field::operator< and Field::operator== in that it also compares values of different types.
  * Comparison rules are same as in FunctionsComparison (to be consistent with expression evaluation in query).
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
                /// Match IColumn::compareAt behavior: NaN == NaN is true.
                if constexpr (is_floating_point<T>)
                {
                    if (isNaN(l) && isNaN(r))
                        return true;
                }
                return l == r;
            }

            if constexpr (is_arithmetic_v<T> && is_arithmetic_v<U>)
            {
                /// Match IColumn::compareAt behavior: NaN == NaN is true.
                if (isNaN(l) && isNaN(r))
                    return true;
                return accurate::equalsOp(l, r);
            }

            /// TODO This is wrong (does not respect scale).
            if constexpr (is_decimal_field<T> && is_decimal_field<U>)
                return l == r;

            if constexpr (is_decimal_field<T> && is_arithmetic_v<U>)
                return l == DecimalField<Decimal256>(Decimal256(r), 0);

            if constexpr (is_arithmetic_v<T> && is_decimal_field<U>)
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
            /// Match IColumn::compareAt behavior with nan_direction_hint=-1:
            /// NULL sorts before non-NULL. Plain Null and negative-infinity Null
            /// are both "less than" any non-Null value; only positive-infinity is not.
            return !l.isPositiveInfinity();
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
                /// Match IColumn::compareAt behavior with nan_direction_hint=-1:
                /// NaN sorts before everything.
                if constexpr (is_floating_point<T>)
                {
                    bool l_nan = isNaN(l);
                    bool r_nan = isNaN(r);
                    if (l_nan || r_nan)
                        return l_nan && !r_nan;
                }
                return l < r;
            }

            if constexpr (is_arithmetic_v<T> && is_arithmetic_v<U>)
            {
                /// Match IColumn::compareAt behavior with nan_direction_hint=-1:
                /// NaN sorts before everything.
                bool l_nan = isNaN(l);
                bool r_nan = isNaN(r);
                if (l_nan || r_nan)
                    return l_nan && !r_nan;
                return accurate::lessOp(l, r);
            }

            /// TODO This is wrong (does not respect scale).
            if constexpr (is_decimal_field<T> && is_decimal_field<U>)
                return l < r;

            if constexpr (is_decimal_field<T> && is_arithmetic_v<U>)
                return l < DecimalField<Decimal256>(Decimal256(r), 0);

            if constexpr (is_arithmetic_v<T> && is_decimal_field<U>)
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
        auto less_cmp = FieldVisitorAccurateLess();
        return !less_cmp(r, l);
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
