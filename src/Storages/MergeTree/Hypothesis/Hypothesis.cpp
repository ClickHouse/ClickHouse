#include "Hypothesis.hpp"
#include <memory>
#include <Parsers/IAST.h>
#include <Poco/Exception.h>
#include "IO/ReadHelpers.h"
#include "IO/WriteBuffer.h"
#include "IO/WriteBufferFromString.h"
#include "IO/WriteHelpers.h"
#include "Parsers/ASTExpressionList.h"
#include "Parsers/ASTFunction.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTLiteral.h"
#include "Parsers/ExpressionListParsers.h"
#include "Storages/MergeTree/Hypothesis/Token.hpp"

namespace DB::Hypothesis
{

std::string Hypothesis::toString() const
{
    WriteBufferFromOwnString buf;
    this->writeText(buf);
    return std::move(buf.str());
}


void Hypothesis::parseText(IParser::Pos & pos)
{
    ParserExpressionList parser(false);
    ASTPtr node;
    Expected expected;
    parser.parse(pos, node, expected);
    auto expr_list = std::static_pointer_cast<ASTExpressionList>(node);
    if (expr_list->children.size() != 1)
    {
        throw Poco::Exception("Only one expression is allowed");
    }
    auto tmp_concat_fn = expr_list->children[0];
    if (tmp_concat_fn->getID() != "Function_concat")
    {
        throw Poco::Exception("Only concat is supported");
    }
    auto concat_fn = std::static_pointer_cast<ASTFunction>(tmp_concat_fn);
    auto args = std::static_pointer_cast<ASTExpressionList>(concat_fn->children.at(0));
    for (const auto & arg : args->children)
    {
        if (arg->getID().starts_with("Identifier"))
        {
            auto identifier = std::static_pointer_cast<ASTIdentifier>(arg);
            this->tokens.emplace_back(createTransformerToken("identity", identifier->name()));
            continue;
        }
        if (arg->getID().starts_with("Literal"))
        {
            auto literal = std::static_pointer_cast<ASTLiteral>(arg);
            this->tokens.emplace_back(createConstToken(literal->value.safeGet<String>()));
            continue;
        }
        if (arg->getID().starts_with("Function"))
        {
            auto function = std::static_pointer_cast<ASTFunction>(arg);
            auto function_args = std::static_pointer_cast<ASTExpressionList>(function->arguments);
            if (function_args->children.size() != 1)
            {
                throw Poco::Exception("Currently only transformers are supported");
            }
            auto identifier = std::static_pointer_cast<ASTIdentifier>(function_args->children.at(0));
            this->tokens.emplace_back(createTransformerToken(function->name, identifier->name()));
            continue;
        }
        throw Poco::Exception("Unknown argument for concat: {}", arg->dumpTree());
    }
}

void Hypothesis::writeText(WriteBuffer & buf) const
{
    writeString("concat(", buf);
    size_t idx = 0;
    for (const auto & token : tokens)
    {
        switch (token->getType())
        {
            case TokenType::Transformer: {
                auto transformer = static_pointer_cast<const TransformerToken>(token);
                writeString(transformer->getTransformerName(), buf);
                writeChar('(', buf);
                writeString(transformer->getColumnName(), buf);
                writeChar(')', buf);
            }
            break;
            case TokenType::Const: {
                auto const_token = static_pointer_cast<const ConstToken>(token);
                writeQuotedString(const_token->getValue(), buf);
            }
            break;
        }
        if (idx + 1 != tokens.size())
        {
            writeChar(',', buf);
            writeChar(' ', buf);
        }
        ++idx;
    }
    writeChar(')', buf);
}

bool Hypothesis::operator==(const Hypothesis & other) const
{
    if (getName() != other.getName())
    {
        return false;
    }
    if (tokens.size() != other.tokens.size())
    {
        return false;
    }
    for (size_t i = 0; i < tokens.size(); ++i)
    {
        if (*tokens[i] != *other.tokens[i])
        {
            return false;
        }
    }
    return true;
}

Hypothesis HypothesisBuilder::constuctHypothesis() &&
{
    Hypothesis hypothesis;
    if (!name)
    {
        throw std::runtime_error("Target column name is not set");
    }
    hypothesis.name = std::move(name);
    hypothesis.tokens = std::move(tokens);
    return hypothesis;
}


void HypothesisList::readText(ReadBuffer & buf)
{
    assertString("Deduced columns: ", buf);
    size_t groups_size = 0;
    DB::readText(groups_size, buf);
    assertChar('\n', buf);
    assertChar('\n', buf);
    for (size_t group = 0; group < groups_size; ++group)
    {
        assertString("Column Name: ", buf);
        std::string tmp_col_name;
        readStringUntilNewlineInto(tmp_col_name, buf);
        auto col_name = std::make_shared<std::string>(std::move(tmp_col_name));
        assertChar('\n', buf);
        assertString("Hypothesis count ", buf);
        size_t hypothesis_in_group = 0;
        DB::readText(hypothesis_in_group, buf);
        assertChar(':', buf);
        assertChar('\n', buf);
        for (size_t i = 0; i < hypothesis_in_group; ++i)
        {
            assertChar('-', buf);
            assertChar(' ', buf);
            std::string text;
            readStringUntilNewlineInto(text, buf);
            Tokens tokens(text.data(), text.data() + text.size());
            IParser::Pos pos(tokens, 100, 100);
            Hypothesis hypothesis(col_name);
            hypothesis.parseText(pos);
            this->push_back(std::move(hypothesis));
            assertChar('\n', buf);
        }
        assertChar('\n', buf);
    }
}
void HypothesisList::writeText(WriteBuffer & buf) const
{
    auto grouped_hypothesis = this->groupByColumnName();
    writeString("Deduced columns: ", buf);
    DB::writeText(grouped_hypothesis.size(), buf);
    writeChar('\n', buf);
    writeChar('\n', buf);
    for (const auto & [name, hypothesis_list] : grouped_hypothesis)
    {
        writeString("Column Name: ", buf);
        writeString(name, buf);
        writeChar('\n', buf);
        writeString("Hypothesis count ", buf);
        DB::writeText(hypothesis_list.size(), buf);
        writeChar(':', buf);
        writeChar('\n', buf);
        for (const auto & hypothesis : hypothesis_list)
        {
            writeChar('-', buf);
            writeChar(' ', buf);
            hypothesis.writeText(buf);
            writeChar('\n', buf);
        }
        writeChar('\n', buf);
    }
}

HypothesisList HypothesisList::filterColumnName(std::string_view col_name) const
{
    HypothesisList result;
    for (const auto & hypothesis : *this)
    {
        if (hypothesis.getName() == col_name)
        {
            result.push_back(hypothesis);
        }
    }
    return result;
}
std::vector<std::pair<std::string, HypothesisList>> HypothesisList::groupByColumnName() const
{
    std::vector<std::pair<std::string, HypothesisList>> result;
    std::unordered_map<std::string, size_t> name_to_pos_map;
    for (const auto & hypothesis : *this)
    {
        const auto & name = hypothesis.getName();
        if (!name_to_pos_map.contains(name))
        {
            name_to_pos_map[name] = result.size();
            result.emplace_back(name, HypothesisList());
        }
        result[name_to_pos_map[name]].second.push_back(hypothesis);
    }
    return result;
}
}
