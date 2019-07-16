#include "ASTColumnsClause.h"

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>

#include <re2/re2.h>



namespace DB
{

ASTPtr ASTColumnsClause::clone() const
{
    auto clone = std::make_shared<ASTColumnsClause>(*this);
    clone->cloneChildren();
    return clone;
}

void ASTColumnsClause::appendColumnName(WriteBuffer & ostr) const { writeString(original_pattern, ostr); }

void ASTColumnsClause::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    WriteBufferFromOwnString pattern_quoted;
    writeQuotedString(original_pattern, pattern_quoted);

    settings.ostr << (settings.hilite ? hilite_keyword : "") << "COLUMNS" << (settings.hilite ? hilite_none : "") << "(" << pattern_quoted.str() << ")";
}

void ASTColumnsClause::setPattern(String pattern)
{
    original_pattern = std::move(pattern);
    column_matcher = std::make_shared<RE2>(pattern, RE2::Quiet);
    if (!column_matcher->ok())
        throw DB::Exception("COLUMNS pattern " + original_pattern + " cannot be compiled: " + column_matcher->error(), DB::ErrorCodes::CANNOT_COMPILE_REGEXP);
}

bool ASTColumnsClause::isColumnMatching(const String & column_name) const
{
    return RE2::FullMatch(column_name, *column_matcher);
}


}
