#pragma once

#include <string.h>
#include <stdio.h>
#include <time.h>
#include <math.h>

#include <string>
#include <limits>

#include <base/preciseExp10.h>
#include <base/types.h>
#include <Common/DateLUT.h>

#include <mysqlxx/Types.h>
#include <Common/LocalDateTime.h>


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
    /** Параметр res_ используется только для генерации подробной информации в исключениях.
      * Можно передать NULL - тогда подробной информации в исключениях не будет.
      */
    Value(const char * data_, size_t length_, const ResultBase * res_) : m_data(data_), m_length(length_), res(res_)
    {
    }

    /// Получить значение bool.
    bool getBool() const
    {
        if (unlikely(isNull()))
            throwException("Value is NULL");

        return m_length > 0 && m_data[0] != '0';
    }

    /// Получить беззнаковое целое.
    UInt64 getUInt() const
    {
        if (unlikely(isNull()))
            throwException("Value is NULL");

        return readUIntText(m_data, m_length);
    }

    /// Получить целое со знаком или дату или дату-время (в unix timestamp согласно текущей тайм-зоне).
    Int64 getInt() const
    {
        return getIntOrDateTime();
    }

    /// Получить число с плавающей запятой.
    double getDouble() const
    {
        if (unlikely(isNull()))
            throwException("Value is NULL");

        return readFloatText(m_data, m_length);
    }

    /// Получить дату-время (из значения вида '2011-01-01 00:00:00').
    LocalDateTime getDateTime() const
    {
        return LocalDateTime(data(), size());
    }

    /// Получить дату (из значения вида '2011-01-01' или '2011-01-01 00:00:00').
    LocalDate getDate() const
    {
        return LocalDate(data(), size());
    }

    /// Получить строку.
    std::string getString() const
    {
        if (unlikely(isNull()))
            throwException("Value is NULL");

        return std::string(m_data, m_length);
    }

    /// Является ли NULL.
    bool isNull() const
    {
        return m_data == nullptr;
    }

    /// Для совместимости (используйте вместо этого метод isNull())
    bool is_null() const { return isNull(); } /// NOLINT

    /** Получить любой поддерживаемый тип (для шаблонного кода).
      * Поддерживаются основные типы, а также любые типы с конструктором от Value (для удобства расширения).
      */
    template <typename T> T get() const;

    /// Для совместимости. Не рекомендуется к использованию, так как неудобен (часто возникают неоднозначности).
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


    time_t getDateTimeImpl() const
    {
        const auto & date_lut = DateLUT::instance();

        if (m_length == 10)
        {
            return date_lut.makeDate(
                (m_data[0] - '0') * 1000 + (m_data[1] - '0') * 100 + (m_data[2] - '0') * 10 + (m_data[3] - '0'),
                (m_data[5] - '0') * 10 + (m_data[6] - '0'),
                (m_data[8] - '0') * 10 + (m_data[9] - '0'));
        }
        if (m_length == 19)
        {
            return date_lut.makeDateTime(
                (m_data[0] - '0') * 1000 + (m_data[1] - '0') * 100 + (m_data[2] - '0') * 10 + (m_data[3] - '0'),
                (m_data[5] - '0') * 10 + (m_data[6] - '0'),
                (m_data[8] - '0') * 10 + (m_data[9] - '0'),
                (m_data[11] - '0') * 10 + (m_data[12] - '0'),
                (m_data[14] - '0') * 10 + (m_data[15] - '0'),
                (m_data[17] - '0') * 10 + (m_data[18] - '0'));
        }
        throwException("Cannot parse DateTime");
    }


    time_t getDateImpl() const
    {
        const auto & date_lut = DateLUT::instance();

        if (m_length == 10 || m_length == 19)
        {
            return date_lut.makeDate(
                (m_data[0] - '0') * 1000 + (m_data[1] - '0') * 100 + (m_data[2] - '0') * 10 + (m_data[3] - '0'),
                (m_data[5] - '0') * 10 + (m_data[6] - '0'),
                (m_data[8] - '0') * 10 + (m_data[9] - '0'));
        }
        throwException("Cannot parse Date");

        return 0;    /// avoid warning. /// NOLINT
    }


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


    Int64 getIntOrDate() const
    {
        if (unlikely(isNull()))
            throwException("Value is NULL");

        if (checkDateTime())
            return getDateImpl();

        const auto & date_lut = DateLUT::instance();
        return date_lut.toDate(getIntImpl());
    }


    /// Прочитать беззнаковое целое в простом формате из не-0-terminated строки.
    UInt64 readUIntText(const char * buf, size_t length) const;

    /// Прочитать знаковое целое в простом формате из не-0-terminated строки.
    Int64 readIntText(const char * buf, size_t length) const;

    /// Прочитать число с плавающей запятой в простом формате, с грубым округлением, из не-0-terminated строки.
    double readFloatText(const char * buf, size_t length) const;

    /// Выкинуть исключение с подробной информацией
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
