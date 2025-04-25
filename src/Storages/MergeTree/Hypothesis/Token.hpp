#pragma once

#include <cassert>
#include <memory>
#include <string>
#include <variant>
#include <vector>
namespace DB::Hypothesis
{

enum class TokenType
{
    Identity = 0,
    Const = 1,
    // TODO:
    // Transformer = 2
};

class IToken
{
public:
    explicit IToken(TokenType type_)
        : type(type_)
    {
    }

    TokenType getType() const { return type; }

    virtual ~IToken() = default;

    virtual bool operator==(const IToken & other) const = 0;

    virtual size_t getHash() const = 0;

private:
    TokenType type;
};

using TokenPtr = std::shared_ptr<const IToken>;
using MutableTokenPtr = std::shared_ptr<IToken>;

class IdentityToken : public IToken
{
public:
    using IndexMapper = std::shared_ptr<const std::vector<std::string>>;

    explicit IdentityToken(std::string name_);

    IdentityToken(size_t idx_, IndexMapper names_);

    const std::string & getName() const;

    bool operator==(const IToken & other) const override;

    size_t getHash() const override;

private:
    std::variant<size_t, std::string> nameOrIdx;
    std::shared_ptr<const std::vector<std::string>> names;
};

using IdentityTokenPtr = std::shared_ptr<const IdentityToken>;

TokenPtr createIdentityToken(std::string column_name);
TokenPtr createIdentityToken(size_t idx, IdentityToken::IndexMapper index_mapper);

class ConstToken : public IToken
{
public:
    explicit ConstToken(std::string value_);

    const std::string & getValue() const;

    size_t getHash() const override;

    bool operator==(const IToken & other) const override;

private:
    std::string value;
};

TokenPtr createConstToken(std::string value);

}

namespace std
{
template <>
struct hash<DB::Hypothesis::IToken>
{
    size_t operator()(const DB::Hypothesis::IToken & token) const noexcept { return token.getHash(); }
};
}
