#pragma once


namespace DB
{

/// Method to quote identifiers.
/// NOTE There could be differences in escaping rules inside quotes. Escaping rules may not match that required by specific external DBMS.
enum class IdentifierQuotingStyle : uint8_t
{
    Backticks, /// `clickhouse` style
    DoubleQuotes, /// "postgres" style, but with ClickHouse escaping: '\"' for an embedded quote, '\\' for a backslash
    BackticksMySQL, /// `mysql` style, most same as Backticks, but it uses '``' to escape '`'
    DoubleQuotesStandard, /// standard SQL style: an embedded double quote is doubled ('""'), a backslash stays literal (SQLite, PostgreSQL)
    BackticksSQLite, /// SQLite strict identifier style: an embedded backtick is doubled, every other byte stays literal
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
    /// Unless the identifier contains upper-case characters.
    /// This is the rule for re-serializing a query that is sent to an external database which folds an
    /// unquoted identifier to lower case and matches a quoted one case-sensitively (PostgreSQL). Quoting a
    /// name without upper-case characters resolves to exactly the same column as leaving it unquoted, so it
    /// is always safe and it makes reserved words such as `where` or `group` survive the re-serialization,
    /// while a name that contains upper-case characters is left unquoted and keeps being folded as before -
    /// `(SELECT Foo FROM t)` has to keep resolving to the column `foo`, not to a case-sensitive `Foo`.
    AlwaysUnlessUpperCase,
};
}
