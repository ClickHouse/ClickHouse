#pragma once

#include <Core/Field.h>
#include <Core/AccurateComparison.h>
#include <common/demangle.h>
#include <Common/FieldVisitors.h>
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
            return std::is_same_v<T, U>;
        else
        {
            if constexpr (std::is_same_v<T, U>)
                return l == r;

            if constexpr (std::is_arithmetic_v<T> && std::is_arithmetic_v<U>)
                return accurate::equalsOp(l, r);

            if constexpr (isDecimalField<T>() && isDecimalField<U>())
                return l == r;

            if constexpr (isDecimalField<T>() && std::is_arithmetic_v<U>)
                return l == DecimalField<Decimal128>(r, 0);

            if constexpr (std::is_arithmetic_v<T> && isDecimalField<U>())
                return DecimalField<Decimal128>(l, 0) == r;

            if constexpr (std::is_same_v<T, String>)
            {
                if constexpr (std::is_same_v<U, UInt128>)
                    return stringToUUID(l) == r;

                if constexpr (std::is_arithmetic_v<U>)
                {
                    ReadBufferFromString in(l);
                    T parsed;
                    readText(parsed, in);
                    return operator()(parsed, r);
                }
            }

            if constexpr (std::is_same_v<U, String>)
            {
                if constexpr (std::is_same_v<T, UInt128>)
                    return l == stringToUUID(r);

                if constexpr (std::is_arithmetic_v<T>)
                {
                    ReadBufferFromString in(r);
                    T parsed;
                    readText(parsed, in);
                    return operator()(l, parsed);
                }
            }
        }

        throw Exception("Cannot compare " + demangle(typeid(T).name()) + " with " + demangle(typeid(U).name()),
            ErrorCodes::BAD_TYPE_OF_FIELD);
    }
};


class FieldVisitorAccurateLess : public StaticVisitor<bool>
{
public:
    template <typename T, typename U>
    bool operator() (const T & l, const U & r) const
    {
        if constexpr (std::is_same_v<T, Null> || std::is_same_v<U, Null>)
            return false;
        else
        {
            if constexpr (std::is_same_v<T, U>)
                return l < r;

            if constexpr (std::is_arithmetic_v<T> && std::is_arithmetic_v<U>)
                return accurate::lessOp(l, r);

            if constexpr (isDecimalField<T>() && isDecimalField<U>())
                return l < r;

            if constexpr (isDecimalField<T>() && std::is_arithmetic_v<U>)
                return l < DecimalField<Decimal128>(r, 0);

            if constexpr (std::is_arithmetic_v<T> && isDecimalField<U>())
                return DecimalField<Decimal128>(l, 0) < r;

            if constexpr (std::is_same_v<T, String>)
            {
                if constexpr (std::is_same_v<U, UInt128>)
                    return stringToUUID(l) < r;

                if constexpr (std::is_arithmetic_v<U>)
                {
                    ReadBufferFromString in(l);
                    T parsed;
                    readText(parsed, in);
                    return operator()(parsed, r);
                }
            }

            if constexpr (std::is_same_v<U, String>)
            {
                if constexpr (std::is_same_v<T, UInt128>)
                    return l < stringToUUID(r);

                if constexpr (std::is_arithmetic_v<T>)
                {
                    ReadBufferFromString in(r);
                    T parsed;
                    readText(parsed, in);
                    return operator()(l, parsed);
                }
            }
        }

        throw Exception("Cannot compare " + demangle(typeid(T).name()) + " with " + demangle(typeid(U).name()),
            ErrorCodes::BAD_TYPE_OF_FIELD);
    }
};

}
