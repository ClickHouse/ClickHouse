#pragma once


namespace DB
{

/// Method to quote identifiers.
/// NOTE There could be differences in escaping rules inside quotes. Escaping rules may not match that required by specific external DBMS.
enum class IdentifierQuotingStyle : uint8_t
{
    Backticks, /// `clickhouse` style
    DoubleQuotes, /// "postgres" style
    BackticksMySQL, /// `mysql` style, most same as Backticks, but it uses '``' to escape '`'
};

enum class IdentifierQuotingRule : uint8_t
{
    /// When the identifiers is one of {"distinct", "all", "table"} (defined in `DB::writeProbablyQuotedStringImpl`),
    /// or it can cause ambiguity: column names, dictionary attribute names (passed to `DB::FormatSettings::writeIdentifier` with `ambiguous=true`)
    WhenNecessary,
    /// Always quote identifiers
    Always,
    /// When the identifiers is a keyword (defined in `DB::Keyword`)
    UserDisplay,
};
}
