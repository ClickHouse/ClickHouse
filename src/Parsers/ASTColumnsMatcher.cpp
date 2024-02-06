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
    clone->children.clear();

    if (expression) { clone->expression = expression->clone(); clone->children.push_back(clone->expression); }
    if (transformers) { clone->transformers = transformers->clone(); clone->children.push_back(clone->transformers); }

    return clone;
}

void ASTColumnsRegexpMatcher::appendColumnName(WriteBuffer & ostr) const
{
    if (expression)
    {
        expression->appendColumnName(ostr);
        writeCString(".", ostr);
    }
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
    settings.ostr << (settings.hilite ? hilite_keyword : "");

    if (expression)
    {
        expression->formatImpl(settings, state, frame);
        settings.ostr << ".";
    }

    settings.ostr << "COLUMNS" << (settings.hilite ? hilite_none : "") << "(";
    settings.ostr << quoteString(original_pattern);
    settings.ostr << ")";

    if (transformers)
    {
        transformers->formatImpl(settings, state, frame);
    }
}

void ASTColumnsRegexpMatcher::setPattern(String pattern)
{
    original_pattern = std::move(pattern);
    column_matcher = std::make_shared<RE2>(original_pattern, RE2::Quiet);
    if (!column_matcher->ok())
        throw DB::Exception(DB::ErrorCodes::CANNOT_COMPILE_REGEXP,
            "COLUMNS pattern {} cannot be compiled: {}", original_pattern, column_matcher->error());
}

const String & ASTColumnsRegexpMatcher::getPattern() const
{
    return original_pattern;
}

const std::shared_ptr<re2::RE2> & ASTColumnsRegexpMatcher::getMatcher() const
{
    return column_matcher;
}

bool ASTColumnsRegexpMatcher::isColumnMatching(const String & column_name) const
{
    return RE2::PartialMatch(column_name, *column_matcher);
}

ASTPtr ASTColumnsListMatcher::clone() const
{
    auto clone = std::make_shared<ASTColumnsListMatcher>(*this);
    clone->children.clear();

    if (expression) { clone->expression = expression->clone(); clone->children.push_back(clone->expression); }
    if (transformers) { clone->transformers = transformers->clone(); clone->children.push_back(clone->transformers); }

    clone->column_list = column_list->clone();
    clone->children.push_back(clone->column_list);

    return clone;
}

void ASTColumnsListMatcher::appendColumnName(WriteBuffer & ostr) const
{
    if (expression)
    {
        expression->appendColumnName(ostr);
        writeCString(".", ostr);
    }
    writeCString("COLUMNS(", ostr);
    for (auto * it = column_list->children.begin(); it != column_list->children.end(); ++it)
    {
        if (it != column_list->children.begin())
            writeCString(", ", ostr);

        (*it)->appendColumnName(ostr);
    }
    writeChar(')', ostr);
}

void ASTColumnsListMatcher::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "");

    if (expression)
    {
        expression->formatImpl(settings, state, frame);
        settings.ostr << ".";
    }

    settings.ostr << "COLUMNS" << (settings.hilite ? hilite_none : "") << "(";

    for (ASTs::const_iterator it = column_list->children.begin(); it != column_list->children.end(); ++it)
    {
        if (it != column_list->children.begin())
        {
            settings.ostr << ", ";
        }
        (*it)->formatImpl(settings, state, frame);
    }
    settings.ostr << ")";

    if (transformers)
    {
        transformers->formatImpl(settings, state, frame);
    }
}

ASTPtr ASTQualifiedColumnsRegexpMatcher::clone() const
{
    auto clone = std::make_shared<ASTQualifiedColumnsRegexpMatcher>(*this);
    clone->children.clear();

    if (transformers) { clone->transformers = transformers->clone(); clone->children.push_back(clone->transformers); }

    clone->qualifier = qualifier->clone();
    clone->children.push_back(clone->qualifier);

    return clone;
}

void ASTQualifiedColumnsRegexpMatcher::appendColumnName(WriteBuffer & ostr) const
{
    qualifier->appendColumnName(ostr);
    writeCString(".COLUMNS(", ostr);
    writeQuotedString(original_pattern, ostr);
    writeChar(')', ostr);
}

void ASTQualifiedColumnsRegexpMatcher::setPattern(String pattern, bool set_matcher)
{
    original_pattern = std::move(pattern);

    if (!set_matcher)
        return;

    column_matcher = std::make_shared<RE2>(original_pattern, RE2::Quiet);
    if (!column_matcher->ok())
        throw DB::Exception(DB::ErrorCodes::CANNOT_COMPILE_REGEXP,
            "COLUMNS pattern {} cannot be compiled: {}", original_pattern, column_matcher->error());
}

void ASTQualifiedColumnsRegexpMatcher::setMatcher(std::shared_ptr<re2::RE2> matcher)
{
    column_matcher = std::move(matcher);
}

const std::shared_ptr<re2::RE2> & ASTQualifiedColumnsRegexpMatcher::getMatcher() const
{
    return column_matcher;
}

void ASTQualifiedColumnsRegexpMatcher::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(original_pattern.size());
    hash_state.update(original_pattern);
    IAST::updateTreeHashImpl(hash_state);
}

void ASTQualifiedColumnsRegexpMatcher::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "");

    qualifier->formatImpl(settings, state, frame);

    settings.ostr << ".COLUMNS" << (settings.hilite ? hilite_none : "") << "(";
    settings.ostr << quoteString(original_pattern);
    settings.ostr << ")";

    if (transformers)
    {
        transformers->formatImpl(settings, state, frame);
    }
}

ASTPtr ASTQualifiedColumnsListMatcher::clone() const
{
    auto clone = std::make_shared<ASTQualifiedColumnsListMatcher>(*this);
    clone->children.clear();

    if (transformers) { clone->transformers = transformers->clone(); clone->children.push_back(clone->transformers); }

    clone->qualifier = qualifier->clone();
    clone->column_list = column_list->clone();

    clone->children.push_back(clone->qualifier);
    clone->children.push_back(clone->column_list);

    return clone;
}

void ASTQualifiedColumnsListMatcher::appendColumnName(WriteBuffer & ostr) const
{
    qualifier->appendColumnName(ostr);
    writeCString(".COLUMNS(", ostr);

    for (auto * it = column_list->children.begin(); it != column_list->children.end(); ++it)
    {
        if (it != column_list->children.begin())
            writeCString(", ", ostr);

        (*it)->appendColumnName(ostr);
    }
    writeChar(')', ostr);
}

void ASTQualifiedColumnsListMatcher::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "");
    qualifier->formatImpl(settings, state, frame);
    settings.ostr << ".COLUMNS" << (settings.hilite ? hilite_none : "") << "(";

    for (ASTs::const_iterator it = column_list->children.begin(); it != column_list->children.end(); ++it)
    {
        if (it != column_list->children.begin())
            settings.ostr << ", ";

        (*it)->formatImpl(settings, state, frame);
    }
    settings.ostr << ")";

    if (transformers)
    {
        transformers->formatImpl(settings, state, frame);
    }
}

}
