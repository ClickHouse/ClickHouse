#pragma once


namespace DB
{

/// Method to escape single quotes.
enum class LiteralEscapingStyle : uint8_t
{
    Regular,         /// Escape backslashes with backslash (\\) and quotes with backslash (\')
    PostgreSQL,      /// Do not escape backslashes (\), escape quotes with quote ('')
    StandardSQL,     /// Standard SQL string literal: only the quote is doubled (''); every other byte, including
                     /// backslashes and control characters, stays literal (SQLite)
};

}
