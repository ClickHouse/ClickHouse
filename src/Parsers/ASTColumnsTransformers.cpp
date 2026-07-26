#include <Parsers/ASTColumnsTransformers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

void ASTColumnsTransformerList::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    for (const auto & child : children)
    {
        ostr << ' ';
        child->format(ostr, settings, state, frame);
    }
}

void ASTColumnsApplyTransformer::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "APPLY" << " ";

    if (!column_name_prefix.empty())
        ostr << "(";

    if (lambda)
    {
        lambda->format(ostr, settings, state, frame);
    }
    else
    {
        ostr << func_name;

        if (parameters)
        {
            auto nested_frame = frame;
            nested_frame.expression_list_prepend_whitespace = false;
            ostr << "(";
            parameters->format(ostr, settings, state, nested_frame);
            ostr << ")";
        }
    }

    if (!column_name_prefix.empty())
        ostr << ", '" << column_name_prefix << "')";
}

void ASTColumnsApplyTransformer::appendColumnName(WriteBuffer & ostr) const
{
    writeCString("APPLY ", ostr);
    if (!column_name_prefix.empty())
        writeChar('(', ostr);

    if (lambda)
        lambda->appendColumnName(ostr);
    else
    {
        writeString(func_name, ostr);

        if (parameters)
            parameters->appendColumnName(ostr);
    }

    if (!column_name_prefix.empty())
    {
        writeCString(", '", ostr);
        writeString(column_name_prefix, ostr);
        writeCString("')", ostr);
    }
}

void ASTColumnsApplyTransformer::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(func_name.size());
    hash_state.update(func_name);
    if (parameters)
        parameters->updateTreeHashImpl(hash_state, ignore_aliases);

    if (lambda)
        lambda->updateTreeHashImpl(hash_state, ignore_aliases);

    hash_state.update(lambda_arg.size());
    hash_state.update(lambda_arg);

    hash_state.update(column_name_prefix.size());
    hash_state.update(column_name_prefix);

    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTColumnsExceptTransformer::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "EXCEPT" << (is_strict ? " STRICT " : " ");

    if (children.size() > 1)
        ostr << "(";

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
        {
            ostr << ", ";
        }
        (*it)->format(ostr, settings, state, frame);
    }

    if (pattern)
        ostr << quoteString(*pattern);

    if (children.size() > 1)
        ostr << ")";
}

void ASTColumnsExceptTransformer::appendColumnName(WriteBuffer & ostr) const
{
    writeCString("EXCEPT ", ostr);
    if (is_strict)
        writeCString("STRICT ", ostr);

    if (children.size() > 1)
        writeChar('(', ostr);

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
            writeCString(", ", ostr);
        (*it)->appendColumnName(ostr);
    }

    if (pattern)
        writeQuotedString(*pattern, ostr);

    if (children.size() > 1)
        writeChar(')', ostr);
}

void ASTColumnsExceptTransformer::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(is_strict);
    if (pattern)
    {
        hash_state.update(pattern->size());
        hash_state.update(*pattern);
    }

    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTColumnsExceptTransformer::setPattern(String pattern_)
{
    pattern = std::move(pattern_);
}

void ASTColumnsReplaceTransformer::Replacement::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    chassert(children.size() == 1);

    children[0]->format(ostr, settings, state, frame);
    ostr << " AS " << backQuoteIfNeed(name);
}

void ASTColumnsReplaceTransformer::Replacement::appendColumnName(WriteBuffer & ostr) const
{
    chassert(children.size() == 1);

    children[0]->appendColumnName(ostr);
    writeCString(" AS ", ostr);
    writeProbablyBackQuotedString(name, ostr);
}

void ASTColumnsReplaceTransformer::Replacement::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    chassert(children.size() == 1);

    hash_state.update(name.size());
    hash_state.update(name);
    children[0]->updateTreeHashImpl(hash_state, ignore_aliases);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTColumnsReplaceTransformer::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "REPLACE" << (is_strict ? " STRICT " : " ");

    ostr << "(";
    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
            ostr << ", ";

        (*it)->format(ostr, settings, state, frame);
    }
    ostr << ")";
}

void ASTColumnsReplaceTransformer::appendColumnName(WriteBuffer & ostr) const
{
    writeCString("REPLACE ", ostr);
    if (is_strict)
        writeCString("STRICT ", ostr);

    if (children.size() > 1)
        writeChar('(', ostr);

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
            writeCString(", ", ostr);
        (*it)->appendColumnName(ostr);
    }

    if (children.size() > 1)
        writeChar(')', ostr);
}

void ASTColumnsReplaceTransformer::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(is_strict);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

}
