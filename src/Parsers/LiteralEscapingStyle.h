#pragma once


namespace DB
{

/// Method to escape single quotes.
enum class LiteralEscapingStyle
{
    Regular,         /// Escape backslashes with backslash (\\) and quotes with backslash (\')
    PostgreSQL,      /// Do not escape backslashes (\), escape quotes with quote ('')
};

}
