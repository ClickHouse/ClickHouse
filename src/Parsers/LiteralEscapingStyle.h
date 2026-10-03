#pragma once


namespace DB
{

/// Method to escape single quotes.
enum class LiteralEscapingStyle : uint8_t
{
    Regular,         /// Escape backslashes with backslash (\\) and quotes with backslash (\')
    MySQL,           /// Use ordinary quoted strings for printable text and hexadecimal literals for bytes that MySQL can reinterpret
    PostgreSQL,      /// Escape quotes with quote (''); strings with backslashes or control characters use the escape string constant form E'...' so that every byte round-trips
    SQLite,          /// Only escape quotes as (''); all other bytes (including \n, \r, \t) are embedded literally
};

}
