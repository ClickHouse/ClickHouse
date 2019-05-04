#pragma once

#include <DataTypes/DataTypeNumberBase.h>


class DateLUTImpl;


template <class, template <class, class...> class>
struct is_instance : public std::false_type {};

template <class...Ts, template <class, class...> class U>
struct is_instance<U<Ts...>, U> : public std::true_type {};

namespace DB
{

/** DateTime stores time as unix timestamp.
  * The value itself is independent of time zone.
  *
  * In binary format it is represented as unix timestamp.
  * In text format it is serialized to and parsed from YYYY-MM-DD hh:mm:ss format.
  * The text format is dependent of time zone.
  *
  * To convert from/to text format, time zone may be specified explicitly or implicit time zone may be used.
  *
  * Time zone may be specified explicitly as type parameter, example: DateTime('Europe/Moscow').
  * As it does not affect the internal representation of values,
  *  all types with different time zones are equivalent and may be used interchangingly.
  * Time zone only affects parsing and displaying in text formats.
  *
  * If time zone is not specified (example: DateTime without parameter), then default time zone is used.
  * Default time zone is server time zone, if server is doing transformations
  *  and if client is doing transformations, unless 'use_client_time_zone' setting is passed to client;
  * Server time zone is the time zone specified in 'timezone' parameter in configuration file,
  *  or system time zone at the moment of server startup.
  */
template<typename NumberBase>
class DataTypeDateTimeBase : public DataTypeNumberBase<NumberBase>
{
public:
    DataTypeDateTimeBase(const std::string & time_zone_name = "");

    const char * getFamilyName() const override;
    std::string doGetName() const override;
    TypeIndex getTypeId() const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
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

    bool canBeUsedAsVersion() const override { return true; }
    bool canBeInsideNullable() const override { return true; }

    bool equals(const IDataType & rhs) const override;

    const DateLUTImpl & getTimeZone() const { return time_zone; }

protected:
    bool has_explicit_time_zone;
    const DateLUTImpl & time_zone;
    const DateLUTImpl & utc_time_zone;
};

struct DataTypeDateTime : DataTypeDateTimeBase<UInt32> {
    using DataTypeDateTimeBase::DataTypeDateTimeBase;
};

// this is a separate class to avoid accidental conversions that
// might occur between time_t and the type storing the datetime64
// time_t might have a different definition on different libcs
struct DateTime64 {
    using Type = Int64;
    struct Components {
        time_t datetime = 0;
        UInt32 nanos = 0;
    };

    Components split() const;
    explicit DateTime64(Components c);
    explicit DateTime64(Type tt) : t{tt} {}
    explicit operator bool() const {
        return t != 0;
    }
    Type get() const { return t; }
private:
    Type t;
};

struct DataTypeDateTime64 : DataTypeDateTimeBase<DateTime64::Type> {
    using DataTypeDateTimeBase::DataTypeDateTimeBase;
};

}

