#include <map>
#include "ASTColumnsTransformers.h"
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <Common/re2.h>
#include <IO/Operators.h>
#include <stack>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int CANNOT_COMPILE_REGEXP;
}

void ASTColumnsTransformerList::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    for (const auto & child : children)
    {
        ostr << ' ';
        child->formatImpl(ostr, settings, state, frame);
    }
}

void IASTColumnsTransformer::transform(const ASTPtr & transformer, ASTs & nodes)
{
    if (const auto * apply = transformer->as<ASTColumnsApplyTransformer>())
    {
        apply->transform(nodes);
    }
    else if (const auto * except = transformer->as<ASTColumnsExceptTransformer>())
    {
        except->transform(nodes);
    }
    else if (const auto * replace = transformer->as<ASTColumnsReplaceTransformer>())
    {
        replace->transform(nodes);
    }
}

void ASTColumnsApplyTransformer::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << (settings.hilite ? hilite_keyword : "") << "APPLY" << (settings.hilite ? hilite_none : "") << " ";

    if (!column_name_prefix.empty())
        ostr << "(";

    if (lambda)
    {
        lambda->formatImpl(ostr, settings, state, frame);
    }
    else
    {
        ostr << func_name;

        if (parameters)
        {
            auto nested_frame = frame;
            nested_frame.expression_list_prepend_whitespace = false;
            ostr << "(";
            parameters->formatImpl(ostr, settings, state, nested_frame);
            ostr << ")";
        }
    }

    if (!column_name_prefix.empty())
        ostr << ", '" << column_name_prefix << "')";
}

void ASTColumnsApplyTransformer::transform(ASTs & nodes) const
{
    for (auto & column : nodes)
    {
        String name;
        auto alias = column->tryGetAlias();
        if (!alias.empty())
            name = alias;
        else
        {
            if (const auto * id = column->as<ASTIdentifier>())
                name = id->shortName();
            else
                name = column->getColumnName();
        }
        if (lambda)
        {
            auto body = lambda->as<const ASTFunction &>().arguments->children.at(1)->clone();
            std::stack<ASTPtr> stack;
            stack.push(body);
            while (!stack.empty())
            {
                auto ast = stack.top();
                stack.pop();
                for (auto & child : ast->children)
                {
                    if (auto arg_name = tryGetIdentifierName(child); arg_name && arg_name == lambda_arg)
                    {
                        child = column->clone();
                        continue;
                    }
                    stack.push(child);
                }
            }
            column = body;
        }
        else
        {
            auto function = makeASTFunction(func_name, column);
            function->parameters = parameters;
            column = function;
        }
        if (!column_name_prefix.empty())
            column->setAlias(column_name_prefix + name);
    }
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
    ostr << (settings.hilite ? hilite_keyword : "") << "EXCEPT" << (is_strict ? " STRICT " : " ") << (settings.hilite ? hilite_none : "");

    if (children.size() > 1)
        ostr << "(";

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
        {
            ostr << ", ";
        }
        (*it)->formatImpl(ostr, settings, state, frame);
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

void ASTColumnsExceptTransformer::transform(ASTs & nodes) const
{
    std::set<String> expected_columns;
    if (!pattern)
    {
        for (const auto & child : children)
            expected_columns.insert(child->as<const ASTIdentifier &>().name());

        for (auto * it = nodes.begin(); it != nodes.end();)
        {
            if (const auto * id = it->get()->as<ASTIdentifier>())
            {
                auto expected_column = expected_columns.find(id->shortName());
                if (expected_column != expected_columns.end())
                {
                    expected_columns.erase(expected_column);
                    it = nodes.erase(it);
                    continue;
                }
            }
            ++it;
        }
    }
    else
    {
        auto regexp = getMatcher();

        for (auto * it = nodes.begin(); it != nodes.end();)
        {
            if (const auto * id = it->get()->as<ASTIdentifier>())
            {
                if (RE2::PartialMatch(id->shortName(), *regexp))
                {
                    it = nodes.erase(it);
                    continue;
                }
            }
            ++it;
        }
    }

    if (is_strict && !expected_columns.empty())
    {
        String expected_columns_str;
        std::for_each(expected_columns.begin(), expected_columns.end(),
            [&](String x) { expected_columns_str += (" " + x) ; });

        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Columns transformer EXCEPT expects following column(s) :{}",
            expected_columns_str);
    }
}

void ASTColumnsExceptTransformer::setPattern(String pattern_)
{
    pattern = std::move(pattern_);
}

std::shared_ptr<re2::RE2> ASTColumnsExceptTransformer::getMatcher() const
{
    if (!pattern)
        return {};

    auto regexp = std::make_shared<re2::RE2>(*pattern, re2::RE2::Quiet);
    if (!regexp->ok())
        throw Exception(ErrorCodes::CANNOT_COMPILE_REGEXP,
            "COLUMNS pattern {} cannot be compiled: {}", *pattern, regexp->error());
    return regexp;
}

void ASTColumnsReplaceTransformer::Replacement::formatImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    assert(children.size() == 1);

    children[0]->formatImpl(ostr, settings, state, frame);
    ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "") << backQuoteIfNeed(name);
}

void ASTColumnsReplaceTransformer::Replacement::appendColumnName(WriteBuffer & ostr) const
{
    assert(children.size() == 1);

    children[0]->appendColumnName(ostr);
    writeCString(" AS ", ostr);
    writeProbablyBackQuotedString(name, ostr);
}

void ASTColumnsReplaceTransformer::Replacement::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    assert(children.size() == 1);

    hash_state.update(name.size());
    hash_state.update(name);
    children[0]->updateTreeHashImpl(hash_state, ignore_aliases);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTColumnsReplaceTransformer::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << (settings.hilite ? hilite_keyword : "") << "REPLACE" << (is_strict ? " STRICT " : " ") << (settings.hilite ? hilite_none : "");

    ostr << "(";
    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
            ostr << ", ";

        (*it)->formatImpl(ostr, settings, state, frame);
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

void ASTColumnsReplaceTransformer::replaceChildren(ASTPtr & node, const ASTPtr & replacement, const String & name)
{
    for (auto & child : node->children)
    {
        if (const auto * id = child->as<ASTIdentifier>())
        {
            if (id->shortName() == name)
                child = replacement->clone();
        }
        else
            replaceChildren(child, replacement, name);
    }
}

void ASTColumnsReplaceTransformer::transform(ASTs & nodes) const
{
    std::map<String, ASTPtr> replace_map;
    for (const auto & replace_child : children)
    {
        auto & replacement = replace_child->as<Replacement &>();
        if (replace_map.find(replacement.name) != replace_map.end())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Expressions in columns transformer REPLACE should not contain the same replacement more than once");
        replace_map.emplace(replacement.name, replacement.children[0]);
    }

    for (auto & column : nodes)
    {
        if (const auto * id = column->as<ASTIdentifier>())
        {
            auto replace_it = replace_map.find(id->shortName());
            if (replace_it != replace_map.end())
            {
                column = replace_it->second;
                column->setAlias(replace_it->first);
                replace_map.erase(replace_it);
            }
        }
        else if (auto * ast_with_alias = dynamic_cast<ASTWithAlias *>(column.get()))
        {
            auto replace_it = replace_map.find(ast_with_alias->alias);
            if (replace_it != replace_map.end())
            {
                auto new_ast = replace_it->second->clone();
                ast_with_alias->alias = ""; // remove the old alias as it's useless after replace transformation
                replaceChildren(new_ast, column, replace_it->first);
                column = new_ast;
                column->setAlias(replace_it->first);
                replace_map.erase(replace_it);
            }
        }
    }

    if (is_strict && !replace_map.empty())
    {
        String expected_columns;
        for (auto & elem: replace_map)
        {
            if (!expected_columns.empty())
                expected_columns += ", ";
            expected_columns += elem.first;
        }
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Columns transformer REPLACE expects following column(s) : {}",
            expected_columns);
    }

}

}
