

#include "Token.hpp"

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

ConstToken::ConstToken(std::string value_)
    : IToken(TokenType::Const)
    , value(std::move(value_))
{
}

const std::string & ConstToken::getValue() const
{
    return value;
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
