

#include "Token.hpp"
#include <string>
#include <variant>
#include <fmt/format.h>

namespace DB::Hypothesis
{

TransformerToken::TransformerToken(std::string transformer_name_, std::string col_name)
    : IToken(TokenType::Transformer)
    , transformer_name(std::move(transformer_name_))
{
    nameOrIdx = std::move(col_name);
}

TransformerToken::TransformerToken(std::string transformer_name_, size_t idx, IndexMapper index_mapper)
    : IToken(TokenType::Transformer)
    , transformer_name(std::move(transformer_name_))
{
    assert(idx_ < names_->size());
    nameOrIdx = idx;
    names = index_mapper;
}

const std::string & TransformerToken::getColumnName() const
{
    if (names)
    {
        return (*names)[std::get<0>(nameOrIdx)];
    }
    return std::get<1>(nameOrIdx);
}

const std::string & TransformerToken::getTransformerName() const
{
    return transformer_name;
}

std::string TransformerToken::toString() const
{
    return fmt::format("{}({})", getTransformerName(), getColumnName());
}

size_t TransformerToken::getHash() const
{
    size_t h = static_cast<size_t>(getType());
    h ^= std::hash<std::string>{}(transformer_name);
    if (std::holds_alternative<size_t>(nameOrIdx))
    {
        h ^= std::get<0>(nameOrIdx);
        h ^= std::hash<std::string>{}((*names)[std::get<0>(nameOrIdx)]);
    }
    else
    {
        h ^= std::hash<std::string>{}(std::get<1>(nameOrIdx));
    }
    return h;
}
bool TransformerToken::operator==(const IToken & other) const
{
    if (other.getType() != getType())
    {
        return false;
    }
    const auto * identity = dynamic_cast<const TransformerToken *>(&other);
    if (identity->nameOrIdx != nameOrIdx)
    {
        return false;
    }
    if (transformer_name != identity->transformer_name) {
        return false;
    }
    if (std::holds_alternative<size_t>(nameOrIdx))
    {
        auto idx = std::get<0>(nameOrIdx);
        if (identity->names->at(idx) != this->names->at(idx))
        {
            return false;
        }
    }
    else
    {
        if (std::get<1>(nameOrIdx) != std::get<1>(identity->nameOrIdx))
        {
            return false;
        }
    }
    return true;
}

ConstToken::ConstToken(std::string value_)
    : IToken(TokenType::Const)
    , value(std::move(value_))
{
}

const std::string & ConstToken::getValue() const
{
    return value;
}

size_t ConstToken::getHash() const
{
    return static_cast<size_t>(getType()) ^ std::hash<std::string>{}(value);
}

bool ConstToken::operator==(const IToken & other) const
{
    if (other.getType() != getType())
    {
        return false;
    }
    const auto * const_token = dynamic_cast<const ConstToken *>(&other);
    if (value != const_token->value)
    {
        return false;
    }
    return true;
}

TokenPtr createTransformerToken(std::string transformer_name, std::string column_name)
{
    return std::make_shared<TransformerToken>(std::move(transformer_name), std::move(column_name));
}
TokenPtr createTransformerToken(std::string transformer_name, size_t idx, TransformerToken::IndexMapper index_mapper)
{
    return std::make_shared<TransformerToken>(std::move(transformer_name), idx, index_mapper);
}

TokenPtr createConstToken(std::string value)
{
    return std::make_shared<ConstToken>(std::move(value));
}


}
