#pragma once


namespace DB
{

/// Method to escape single quotes.
enum class LiteralEscapingStyle : uint8_t
{
    Regular,         /// Escape backslashes with backslash (\\) and quotes with backslash (\')
    PostgreSQL,      /// Emit a PostgreSQL `E'...'` escape-string constant, doubling both the quote ('') and
                     /// the backslash (\\); safe regardless of the server's `standard_conforming_strings`.
};

}
