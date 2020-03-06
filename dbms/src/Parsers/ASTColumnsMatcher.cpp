#include "ASTColumnsMatcher.h"
#include <IO/WriteHelpers.h>
#include <Common/quoteString.h>
#include <re2/re2.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
}

ASTPtr ASTColumnsMatcher::clone() const
{
    auto clone = std::make_shared<ASTColumnsMatcher>(*this);
    clone->cloneChildren();
    return clone;
}

void ASTColumnsMatcher::appendColumnName(WriteBuffer & ostr) const { writeString(original_pattern, ostr); }

void ASTColumnsMatcher::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "COLUMNS" << (settings.hilite ? hilite_none : "") << "("
                  << quoteString(original_pattern) << ")";
}

void ASTColumnsMatcher::setPattern(String pattern)
{
    original_pattern = std::move(pattern);
    column_matcher = std::make_shared<RE2>(original_pattern, RE2::Quiet);
    if (!column_matcher->ok())
        throw DB::Exception("COLUMNS pattern " + original_pattern + " cannot be compiled: " + column_matcher->error(), DB::ErrorCodes::CANNOT_COMPILE_REGEXP);
}

bool ASTColumnsMatcher::isColumnMatching(const String & column_name) const
{
    return RE2::PartialMatch(column_name, *column_matcher);
}


}
