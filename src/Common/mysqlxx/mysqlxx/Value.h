#pragma once

#include <Common/LocalDateTime.h>
#include <mysqlxx/Types.h>

#include <ostream>
#include <string>


namespace mysqlxx
{


class ResultBase;


/** Represents a single value read from MySQL.
  * It doesn't owns the value. It's just a wrapper of a pair (const char *, size_t).
  * If the UseQueryResult or Connection is destroyed,
  *  or you have read the next Row while using UseQueryResult, then the object is invalidated.
  * Allows to transform (parse) the value to various data types:
  * - with getUInt(), getString(), ... (recommended);
  * - with template function get<Type>() that is specialized for multiple data types;
  * - the template function get<Type> also works for all types that can be constructed from Value
  *   (it is an extension point);
  * - with operator Type() - this is done for compatibility and not recommended because ambiguities possible.
  *
  * On parsing error, exception is thrown.
  * When trying to extract a value that is nullptr, exception is thrown
  * - use isNull() method to check.
  *
  * As time_t is just an alias for integer data type
  * to allow to write row[0].get<time_t>(), and expect that the values like '2011-01-01 00:00:00'
  *  will be successfully parsed according to the current time zone,
  *  the getUInt method and the corresponding get<>() methods
  *  are capable of parsing Date and DateTime.
  */
class Value
{
public:
    /** Parameter res_ is used only for generating detailed information in exceptions.
      * You can pass NULL - then there will be no detailed information in exceptions.
      */
    Value(const char * data_, size_t length_, const ResultBase * res_) : m_data(data_), m_length(length_), res(res_)
    {
    }

    /// Get bool value.
    bool getBool() const
    {
        if (unlikely(isNull()))
            throwException("Value is NULL");

        return m_length > 0 && m_data[0] != '0';
    }

    /// Get unsigned integer.
    UInt64 getUInt() const
    {
        if (unlikely(isNull()))
            throwException("Value is NULL");

        return readUIntText(m_data, m_length);
    }

    /// Get signed integer or date or date-time (as unix timestamp according to current time zone).
    Int64 getInt() const
    {
        return getIntOrDateTime();
    }

    /// Get floating point number.
    double getDouble() const
    {
        if (unlikely(isNull()))
            throwException("Value is NULL");

        return readFloatText(m_data, m_length);
    }

    /// Get date-time (from value like '2011-01-01 00:00:00').
    LocalDateTime getDateTime() const
    {
        return LocalDateTime(data(), size());
    }

    /// Get date (from value like '2011-01-01' or '2011-01-01 00:00:00').
    LocalDate getDate() const
    {
        return LocalDate(data(), size());
    }

    /// Get string.
    std::string getString() const
    {
        if (unlikely(isNull()))
            throwException("Value is NULL");

        return std::string(m_data, m_length);
    }

    /// Is NULL.
    bool isNull() const
    {
        return m_data == nullptr;
    }

    /// For compatibility (use isNull() method instead)
    bool is_null() const { return isNull(); } /// NOLINT

    /** Get any supported type (for template code).
      * Basic types are supported, as well as any types with constructor from Value (for convenient extension).
      */
    template <typename T> T get() const;

    /// For compatibility. Not recommended for use as it's inconvenient (ambiguities often arise).
    template <typename T> operator T() const { return get<T>(); } /// NOLINT

    const char * data() const     { return m_data; }
    size_t length() const         { return m_length; }
    size_t size() const         { return m_length; }
    bool empty() const             { return 0 == m_length; }

private:
    const char * m_data;
    size_t m_length;
    const ResultBase * res;


    bool checkDateTime() const
    {
        return (m_length == 10 || m_length == 19) && m_data[4] == '-' && m_data[7] == '-';
    }


    time_t getDateTimeImpl() const;

    time_t getDateImpl() const;


    Int64 getIntImpl() const
    {
        return readIntText(m_data, m_length);
    }


    Int64 getIntOrDateTime() const
    {
        if (unlikely(isNull()))
            throwException("Value is NULL");

        if (checkDateTime())
            return getDateTimeImpl();
        return getIntImpl();
    }


    Int64 getIntOrDate() const;


    /// Read unsigned integer in simple format from non-0-terminated string.
    UInt64 readUIntText(const char * buf, size_t length) const;

    /// Read signed integer in simple format from non-0-terminated string.
    Int64 readIntText(const char * buf, size_t length) const;

    /// Read floating point number in simple format, with rough rounding, from non-0-terminated string.
    double readFloatText(const char * buf, size_t length) const;

    /// Throw exception with detailed information
    [[noreturn]] void throwException(const char * text) const;
};


template <> inline bool                 Value::get<bool                 >() const { return getBool(); }
template <> inline char                 Value::get<char                 >() const { return getInt(); }
template <> inline signed char          Value::get<signed char          >() const { return getInt(); }
template <> inline unsigned char        Value::get<unsigned char        >() const { return getUInt(); }
template <> inline char8_t              Value::get<char8_t              >() const { return getUInt(); }
template <> inline short                Value::get<short                >() const { return getInt(); } /// NOLINT
template <> inline unsigned short       Value::get<unsigned short       >() const { return getUInt(); } /// NOLINT
template <> inline int                  Value::get<int                  >() const { return static_cast<int>(getInt()); }
template <> inline unsigned int         Value::get<unsigned int         >() const { return static_cast<unsigned int>(getUInt()); }
template <> inline long                 Value::get<long                 >() const { return getInt(); } /// NOLINT
template <> inline unsigned long        Value::get<unsigned long        >() const { return getUInt(); } /// NOLINT
template <> inline long long            Value::get<long long            >() const { return getInt(); } /// NOLINT
template <> inline unsigned long long   Value::get<unsigned long long   >() const { return getUInt(); } /// NOLINT
template <> inline double               Value::get<double               >() const { return getDouble(); }
template <> inline std::string          Value::get<std::string          >() const { return getString(); }
template <> inline LocalDate            Value::get<LocalDate            >() const { return getDate(); }
template <> inline LocalDateTime        Value::get<LocalDateTime        >() const { return getDateTime(); }


namespace details
{
// To avoid stack overflow when converting to type with no appropriate c-tor,
// resulting in endless recursive calls from `Value::get<T>()` to `Value::operator T()` to `Value::get<T>()` to ...
template <typename T>
requires std::is_constructible_v<T, Value>
inline T contructFromValue(const Value & val)
{
    return T(val);
}
}

template <typename T>
inline T Value::get() const
{
    return details::contructFromValue<T>(*this);
}


inline std::ostream & operator<< (std::ostream & ostr, const Value & x)
{
    return ostr.write(x.data(), x.size());
}


}
