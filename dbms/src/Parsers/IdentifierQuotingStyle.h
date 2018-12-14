#pragma once


namespace DB
{

/// Method to quote identifiers.
/// NOTE There could be differences in escaping rules inside quotes. Escaping rules may not match that required by specific external DBMS.
enum class IdentifierQuotingStyle
{
    None,           /// Write as-is, without quotes.
    Backticks,      /// `mysql` style
    DoubleQuotes    /// "postgres" style
};

}
