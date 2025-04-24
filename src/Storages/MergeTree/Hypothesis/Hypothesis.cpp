#include "Hypothesis.hpp"
#include "IO/ReadHelpers.h"
#include "IO/WriteBuffer.h"
#include "IO/WriteBufferFromString.h"
#include "IO/WriteHelpers.h"
#include "Storages/MergeTree/Hypothesis/Token.hpp"

namespace DB::Hypothesis
{

std::string Hypothesis::toString() const
{
    WriteBufferFromOwnString buf;
    this->writeText(buf);
    return std::move(buf.str());
}


void Hypothesis::readText(ReadBuffer & buf)
{
    char next = ' ';
    bool finish = false;
    while (!finish && buf.peek(next))
    {
        switch (next)
        {
            case '$': {
                assertChar('$', buf);
                std::string res;
                readDoubleQuotedString(res, buf);
                this->tokens.emplace_back(new IdentityToken(std::move(res)));
                break;
            }
            case 'c': {
                assertChar('c', buf);
                std::string res;
                readDoubleQuotedString(res, buf);
                this->tokens.emplace_back(new ConstToken(std::move(res)));
                break;
            }
            case 'T':
                assertChar('T', buf);
                throw std::runtime_error("Transformers are not yet implemented in hypothesis");
            default: {
                finish = true;
                break;
            }
        }
    }
}

void Hypothesis::writeText(WriteBuffer & buf) const
{
    for (const auto & token : tokens)
    {
        switch (token->getType())
        {
            case TokenType::Identity: {
                writeChar('$', buf);
                auto identity = static_pointer_cast<const IdentityToken>(token);
                writeDoubleQuotedString(identity->getName(), buf);
            }
            break;
            case TokenType::Const: {
                auto const_token = static_pointer_cast<const ConstToken>(token);
                writeChar('c', buf);
                writeDoubleQuotedString(const_token->getValue(), buf);
            }
            break;
        }
    }
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
            Hypothesis hypothesis(col_name);
            hypothesis.readText(buf);
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
