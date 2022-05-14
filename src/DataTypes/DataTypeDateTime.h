#pragma once

#include <Core/Types.h>
#include <DataTypes/DataTypeNumberBase.h>
#include <DataTypes/TimezoneMixin.h>

namespace DB
{

/** DateTime stores time as unix timestamp.
  * The value itself is independent of time zone.
  *
  * In binary format it is represented as unix timestamp.
  * In text format it is serialized to and parsed from YYYY-MM-DD hh:mm:ss format.
  * The text format is dependent of time zone.
  *
  * To cast from/to text format, time zone may be specified explicitly or implicit time zone may be used.
  *
  * Time zone may be specified explicitly as type parameter, example: DateTime('Pacific/Pitcairn').
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
class DataTypeDateTime final : public DataTypeNumberBase<UInt32>, public TimezoneMixin
{
public:
    explicit DataTypeDateTime(const String & time_zone_name = "");
    explicit DataTypeDateTime(const TimezoneMixin & time_zone);

    static constexpr auto family_name = "DateTime";

    const char * getFamilyName() const override { return family_name; }
    String doGetName() const override;
    TypeIndex getTypeId() const override { return TypeIndex::DateTime; }

    bool canBeUsedAsVersion() const override { return true; }
    bool canBeInsideNullable() const override { return true; }

    bool equals(const IDataType & rhs) const override;

    SerializationPtr doGetDefaultSerialization() const override;
};

}
