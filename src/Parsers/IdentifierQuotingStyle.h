#pragma once


namespace DB
{

/// Method to quote identifiers.
/// NOTE There could be differences in escaping rules inside quotes. Escaping rules may not match that required by specific external DBMS.
enum class IdentifierQuotingStyle : uint8_t
{
    None,            /// Write as-is, without quotes.
    Backticks,       /// `clickhouse` style
    DoubleQuotes,    /// "postgres" style
    BackticksMySQL,  /// `mysql` style, most same as Backticks, but it uses '``' to escape '`'
};

enum class IdentifierQuotingRule : uint8_t
{
    UserDisplay,    /// When the identifiers is one of the certain keywords defined in `writeProbablyQuotedStringImpl`
    WhenNecessary,  /// When the identifiers is one of the certain keywords defined in `writeProbablyQuotedStringImpl`, and ambiguous identifiers passed to `writeIdentifier`
    Always,         /// Always quote identifiers
};
}
