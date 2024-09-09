#pragma once


namespace DB
{

enum class IdentifierQuotingRule : uint8_t
{
    WhenNecessary, /// Only quote identifiers if they are one of certain keywords defined in `writeProbablyQuotedStringImpl`
    WhenNecessaryAndAvoidAmbiguity, /// Same as `WhenNecessary`, plus ambiguous identifiers: column names, dictionary attribute names
    AlwaysQuote, // Always quote identifiers
};

}
