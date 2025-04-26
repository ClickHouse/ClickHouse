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
    Transformer = 0,
    Const = 1,
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

class TransformerToken : public IToken
{
public:
    using IndexMapper = std::shared_ptr<const std::vector<std::string>>;

    TransformerToken(std::string transformer_name_, std::string col_name);

    TransformerToken(std::string transformer_name_, size_t idx, IndexMapper index_mapper);

    const std::string & getColumnName() const;

    const std::string & getTransformerName() const;

    std::string toString() const;

    bool operator==(const IToken & other) const override;

    size_t getHash() const override;

private:
    std::string transformer_name;
    std::variant<size_t, std::string> nameOrIdx;
    std::shared_ptr<const std::vector<std::string>> names;
};

using TransformerTokenPtr = std::shared_ptr<const TransformerToken>;

TokenPtr createTransformerToken(std::string transformer_name, std::string column_name);
TokenPtr createTransformerToken(std::string transformer_name, size_t idx, TransformerToken::IndexMapper index_mapper);

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
