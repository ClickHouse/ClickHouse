# DateTime {#data_type-datetime}

Date with time. Stored in four bytes as a Unix timestamp (unsigned).
Allows storing values in the same range as for the [Date type](https://clickhouse.yandex/docs/en/data_types/date/). 
The value itself is independent of time zone.
The time is stored with accuracy up to one second (without leap seconds).
In text format it is serialized to and parsed from YYYY-MM-DD hh:mm:ss format.
The text format is dependent of time zone.

## Time Zones

To convert from/to text format, time zone may be specified explicitly or implicit time zone may be used.
Time zone may be specified explicitly as type parameter, example: `DateTime('Europe/Moscow')`.
As it does not affect the internal representation of values, all types with different time zones are equivalent and may be used interchangingly.
Time zone only affects parsing and displaying in text formats. List of the available time zones describes in the [article](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones).

If time zone is not specified (example: DateTime without parameter), then default time zone is used.
By default, the client switches to the time zone of the server when it connects. 
You can use client time zone by enabling the client command-line option `--use_client_time_zone`.
Server time zone is the time zone specified in 'timezone' parameter in configuration file,
or system time zone at the moment of server startup.

[Original article](https://clickhouse.yandex/docs/en/data_types/datetime/) <!--hide-->
