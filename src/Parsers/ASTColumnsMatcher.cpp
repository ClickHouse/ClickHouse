#include <Parsers/ASTColumnsMatcher.h>

#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <re2/re2.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
}

ASTPtr ASTColumnsRegexpMatcher::clone() const
{
    auto clone = std::make_shared<ASTColumnsRegexpMatcher>(*this);
    clone->cloneChildren();
    return clone;
}

void ASTColumnsRegexpMatcher::appendColumnName(WriteBuffer & ostr) const
{
    writeCString("COLUMNS(", ostr);
    writeQuotedString(original_pattern, ostr);
    writeChar(')', ostr);
}

void ASTColumnsRegexpMatcher::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(original_pattern.size());
    hash_state.update(original_pattern);
    IAST::updateTreeHashImpl(hash_state);
}

void ASTColumnsRegexpMatcher::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "COLUMNS" << (settings.hilite ? hilite_none : "") << "(";
    settings.ostr << quoteString(original_pattern);
    settings.ostr << ")";

    /// Format column transformers
    for (const auto & child : children)
    {
        settings.ostr << ' ';
        child->formatImpl(settings, state, frame);
    }
}

void ASTColumnsRegexpMatcher::setPattern(String pattern)
{
    original_pattern = std::move(pattern);
    column_matcher = std::make_shared<RE2>(original_pattern, RE2::Quiet);
    if (!column_matcher->ok())
        throw DB::Exception(
            "COLUMNS pattern " + original_pattern + " cannot be compiled: " + column_matcher->error(),
            DB::ErrorCodes::CANNOT_COMPILE_REGEXP);
}

bool ASTColumnsRegexpMatcher::isColumnMatching(const String & column_name) const
{
    return RE2::PartialMatch(column_name, *column_matcher);
}

ASTPtr ASTColumnsListMatcher::clone() const
{
    auto clone = std::make_shared<ASTColumnsListMatcher>(*this);
    clone->column_list = column_list->clone();
    clone->cloneChildren();
    return clone;
}

void ASTColumnsListMatcher::updateTreeHashImpl(SipHash & hash_state) const
{
    column_list->updateTreeHash(hash_state);
    IAST::updateTreeHashImpl(hash_state);
}

void ASTColumnsListMatcher::appendColumnName(WriteBuffer & ostr) const
{
    writeCString("COLUMNS(", ostr);
    for (auto it = column_list->children.begin(); it != column_list->children.end(); ++it)
    {
        if (it != column_list->children.begin())
            writeCString(", ", ostr);

        (*it)->appendColumnName(ostr);
    }
    writeChar(')', ostr);
}

void ASTColumnsListMatcher::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "COLUMNS" << (settings.hilite ? hilite_none : "") << "(";

    for (ASTs::const_iterator it = column_list->children.begin(); it != column_list->children.end(); ++it)
    {
        if (it != column_list->children.begin())
        {
            settings.ostr << ", ";
        }
        (*it)->formatImpl(settings, state, frame);
    }
    settings.ostr << ")";

    /// Format column transformers
    for (const auto & child : children)
    {
        settings.ostr << ' ';
        child->formatImpl(settings, state, frame);
    }
}

}
