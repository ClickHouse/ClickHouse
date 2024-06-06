#include <Parsers/ASTColumnsMatcher.h>

#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>


namespace DB
{

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
    writeQuotedString(pattern, ostr);
    writeChar(')', ostr);
}

void ASTColumnsRegexpMatcher::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(pattern.size());
    hash_state.update(pattern);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
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
    settings.ostr << quoteString(pattern);
    settings.ostr << ")";

    if (transformers)
    {
        transformers->formatImpl(settings, state, frame);
    }
}

void ASTColumnsRegexpMatcher::setPattern(String pattern_)
{
    pattern = std::move(pattern_);
}

const String & ASTColumnsRegexpMatcher::getPattern() const
{
    return pattern;
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
    writeQuotedString(pattern, ostr);
    writeChar(')', ostr);
}

void ASTQualifiedColumnsRegexpMatcher::setPattern(String pattern_)
{
    pattern = std::move(pattern_);
}

const String & ASTQualifiedColumnsRegexpMatcher::getPattern() const
{
    return pattern;
}

void ASTQualifiedColumnsRegexpMatcher::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(pattern.size());
    hash_state.update(pattern);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTQualifiedColumnsRegexpMatcher::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "");

    qualifier->formatImpl(settings, state, frame);

    settings.ostr << ".COLUMNS" << (settings.hilite ? hilite_none : "") << "(";
    settings.ostr << quoteString(pattern);
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
