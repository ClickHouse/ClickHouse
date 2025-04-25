

#include "Token.hpp"
#include <string>
#include <variant>

namespace DB::Hypothesis
{

IdentityToken::IdentityToken(std::string name_)
    : IToken(TokenType::Identity)
{
    nameOrIdx = std::move(name_);
}

IdentityToken::IdentityToken(size_t idx_, IndexMapper names_)
    : IToken(TokenType::Identity)
{
    assert(idx_ < names_->size());
    nameOrIdx = idx_;
    names = names_;
}

const std::string & IdentityToken::getName() const
{
    if (names)
    {
        return (*names)[std::get<0>(nameOrIdx)];
    }
    return std::get<1>(nameOrIdx);
}
size_t IdentityToken::getHash() const
{
    size_t h = static_cast<size_t>(getType());
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
bool IdentityToken::operator==(const IToken & other) const
{
    if (other.getType() != getType())
    {
        return false;
    }
    const auto * identity = dynamic_cast<const IdentityToken *>(&other);
    if (identity->nameOrIdx != nameOrIdx)
    {
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

TokenPtr createIdentityToken(std::string column_name)
{
    return std::make_shared<IdentityToken>(column_name);
}
TokenPtr createIdentityToken(size_t idx, IdentityToken::IndexMapper index_mapper)
{
    return std::make_shared<IdentityToken>(idx, index_mapper);
}

TokenPtr createConstToken(std::string value)
{
    return std::make_shared<ConstToken>(std::move(value));
}


}
