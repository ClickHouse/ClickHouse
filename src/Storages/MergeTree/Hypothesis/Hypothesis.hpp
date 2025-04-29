#pragma once
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include "Parsers/IParser.h"
#include "Token.hpp"

#include <list>
#include <string>
#include <string_view>
#include <vector>

namespace DB::Hypothesis
{

class HypothesisBuilder;

class Hypothesis
{
public:
    Hypothesis() = default;
    explicit Hypothesis(std::shared_ptr<const std::string> col_name)
        : name(col_name)
    {
    }

    size_t getSize() const { return tokens.size(); }

    const std::string & getName() const { return *name; }

    TokenPtr getToken(size_t idx) const { return tokens[idx]; }

    std::string toString() const;

    void parseText(IParser::Pos & pos);
    void writeText(WriteBuffer & buf) const;

    bool operator==(const Hypothesis & other) const;

private:
    std::shared_ptr<const std::string> name = nullptr;
    std::vector<TokenPtr> tokens;

    friend class HypothesisBuilder;
};

class HypothesisBuilder
{
public:
    HypothesisBuilder() = default;

    void setTargetColumn(std::string_view name_) { name = std::make_shared<const std::string>(name_); }

    void addToken(TokenPtr token) { tokens.emplace_back(token); }

    Hypothesis constuctHypothesis() &&;

    bool empty() const { return tokens.empty(); }

    TokenPtr back() const { return tokens.back(); }

private:
    std::shared_ptr<const std::string> name = nullptr;
    std::vector<TokenPtr> tokens;
};


class HypothesisList : public std::list<Hypothesis>
{
public:
    HypothesisList() = default;

    HypothesisList(std::initializer_list<Hypothesis> init)
        : std::list<Hypothesis>(init)
    {
    }

    void readText(ReadBuffer & buf);
    void writeText(WriteBuffer & buf) const;

    HypothesisList filterColumnName(std::string_view col_name) const;
    std::vector<std::pair<std::string, HypothesisList>> groupByColumnName() const;
};
}

template <>
struct std::hash<DB::Hypothesis::Hypothesis>
{
    size_t operator()(const DB::Hypothesis::Hypothesis & hypothesis) const noexcept
    {
        size_t h = std::hash<std::string>{}(hypothesis.getName());
        h ^= hypothesis.getSize();
        for (size_t i = 0; i < hypothesis.getSize(); ++i)
        {
            h ^= std::hash<DB::Hypothesis::IToken>{}(*hypothesis.getToken(i));
        }
        return h;
    }
};
