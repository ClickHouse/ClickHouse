#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ITokenizer.h>
#include <absl/container/flat_hash_map.h>

namespace DB
{

enum class HasAnyAllTokensMode : uint8_t
{
    Any,
    All
};

struct HasAnyTokensTraits
{
    static constexpr String name = "hasAnyTokens";
    static constexpr HasAnyAllTokensMode mode = HasAnyAllTokensMode::Any;
};

struct HasAllTokensTraits
{
    static constexpr String name = "hasAllTokens";
    static constexpr HasAnyAllTokensMode mode = HasAnyAllTokensMode::All;
};

/// Map needle into a position (for bitmap operations).
using TokensWithPosition = absl::flat_hash_map<String, UInt64>;

template <class HasTokensTraits>
class ExecutableFunctionHasAnyAllTokens : public IExecutableFunction
{
public:
    static constexpr auto name = HasTokensTraits::name;

    explicit ExecutableFunctionHasAnyAllTokens(
        std::shared_ptr<const ITokenizer> tokenizer_, const TokensWithPosition & search_tokens_)
        : tokenizer(std::move(tokenizer_))
        , search_tokens(std::move(search_tokens_))
    {
    }

    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;

private:
    std::shared_ptr<const ITokenizer> tokenizer;
    const TokensWithPosition & search_tokens;
};

template <class HasTokensTraits>
class FunctionBaseHasAnyAllTokens : public IFunctionBase
{
public:
    static constexpr auto name = HasTokensTraits::name;

    FunctionBaseHasAnyAllTokens(
        std::shared_ptr<const ITokenizer> tokenizer_,
        TokensWithPosition search_tokens_,
        DataTypes argument_types_,
        DataTypePtr result_type_)
        : tokenizer(std::move(tokenizer_))
        , search_tokens(std::move(search_tokens_))
        , argument_types(std::move(argument_types_))
        , result_type(std::move(result_type_))
    {
    }

    String getName() const override { return name; }
    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return result_type; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override;

private:
    std::shared_ptr<const ITokenizer> tokenizer;
    TokensWithPosition search_tokens;
    DataTypes argument_types;
    DataTypePtr result_type;
};

template <class HasTokensTraits>
class FunctionHasAnyAllTokensOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = HasTokensTraits::name;

    static FunctionOverloadResolverPtr create(ContextPtr context)
    {
        return std::make_unique<FunctionHasAnyAllTokensOverloadResolver<HasTokensTraits>>(context);
    }

    explicit FunctionHasAnyAllTokensOverloadResolver(ContextPtr context);

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override;
};
}
