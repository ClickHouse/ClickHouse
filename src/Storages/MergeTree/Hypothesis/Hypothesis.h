#pragma once
#include "Token.hpp"

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

    size_t getSize() const { return tokens.size(); }

    const std::string & getName() const { return *name; }

    TokenPtr getToken(size_t idx) const { return tokens[idx]; }

    std::string toString() const
    {
        std::string result;
        for (const auto & token : tokens)
        {
            switch (token->getType())
            {
                case TokenType::Identity: {
                    auto identity = static_pointer_cast<const IdentityToken>(token);
                    result += "${" + identity->getName() + "}";
                }
                break;
                case TokenType::Const: {
                    auto const_token = static_pointer_cast<const ConstToken>(token);
                    result += '"' + const_token->getValue() + '"';
                }
                break;
            }
        }
        return result;
    }

private:
    std::shared_ptr<std::string> name = nullptr;
    std::vector<TokenPtr> tokens;

    friend class HypothesisBuilder;
};

class HypothesisBuilder
{
public:
    HypothesisBuilder() = default;

    void setTargetColumn(std::string_view name_) { name = std::make_shared<std::string>(name_); }

    void addToken(TokenPtr token) { tokens.emplace_back(token); }

    Hypothesis constuctHypothesis() &&
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

    bool empty() const { return tokens.empty(); }

    TokenPtr back() const { return tokens.back(); }

    std::string toString() const
    {
        std::string result;
        for (const auto & token : tokens)
        {
            switch (token->getType())
            {
                case TokenType::Identity: {
                    auto identity = static_pointer_cast<const IdentityToken>(token);
                    result += "${" + identity->getName() + "}";
                }
                break;
                case TokenType::Const: {
                    auto const_token = static_pointer_cast<const ConstToken>(token);
                    result += '"' + const_token->getValue() + '"';
                }
                break;
            }
        }
        return result;
    }

private:
    std::shared_ptr<std::string> name = nullptr;
    std::vector<TokenPtr> tokens;
};


using HypothesisVec = std::vector<Hypothesis>;
}
