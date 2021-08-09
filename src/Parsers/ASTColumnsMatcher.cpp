#include "ASTColumnsMatcher.h"
#include <IO/WriteHelpers.h>
#include <Common/quoteString.h>
#include <re2/re2.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


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

void ASTColumnsMatcher::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(original_pattern.size());
    hash_state.update(original_pattern);
    IAST::updateTreeHashImpl(hash_state);
}

void ASTColumnsMatcher::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "COLUMNS" << (settings.hilite ? hilite_none : "") << "(";
    if (column_list)
    {
        frame.expression_list_prepend_whitespace = false;
        column_list->formatImpl(settings, state, frame);
    }
    else
        settings.ostr << quoteString(original_pattern);
    settings.ostr << ")";
    for (ASTs::const_iterator it = children.begin() + 1; it != children.end(); ++it)
    {
        settings.ostr << ' ';
        (*it)->formatImpl(settings, state, frame);
    }
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
