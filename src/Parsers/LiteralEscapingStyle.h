#pragma once


namespace DB
{

/// Method to escape single quotes.
enum class LiteralEscapingStyle : uint8_t
{
    Regular,         /// Escape backslashes with backslash (\\) and quotes with backslash (\')
    PostgreSQL,      /// Do not escape backslashes (\), escape quotes with quote ('')
    SQLite,          /// Only escape quotes as (''); all other bytes (including \n, \r, \t) are embedded literally
};

}
