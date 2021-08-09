#pragma once


namespace DB
{

/// Method to quote identifiers.
/// NOTE There could be differences in escaping rules inside quotes. Escaping rules may not match that required by specific external DBMS.
enum class IdentifierQuotingStyle
{
    None,            /// Write as-is, without quotes.
    Backticks,       /// `clickhouse` style
    DoubleQuotes,    /// "postgres" style
    BackticksMySQL,  /// `mysql` style, most same as Backticks, but it uses '``' to escape '`'
};

}
