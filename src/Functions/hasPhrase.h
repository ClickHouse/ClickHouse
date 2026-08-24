#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct ITokenizer;

class ExecutableFunctionHasPhrase : public IExecutableFunction
{
public:
    static constexpr auto name = "hasPhrase";

    ExecutableFunctionHasPhrase(
        std::shared_ptr<const ITokenizer> tokenizer_, std::vector<String> phrase_tokens_, std::vector<size_t> failure_table_)
        : tokenizer(std::move(tokenizer_))
        , phrase_tokens(std::move(phrase_tokens_))
        , failure_table(std::move(failure_table_))
    {
    }

    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;

private:
    std::shared_ptr<const ITokenizer> tokenizer;
    std::vector<String> phrase_tokens;
    std::vector<size_t> failure_table;
};

class FunctionBaseHasPhrase : public IFunctionBase
{
public:
    static constexpr auto name = "hasPhrase";

    FunctionBaseHasPhrase(
        std::shared_ptr<const ITokenizer> tokenizer_,
        std::vector<String> phrase_tokens_,
        DataTypes argument_types_,
        DataTypePtr result_type_)
        : tokenizer(std::move(tokenizer_))
        , phrase_tokens(std::move(phrase_tokens_))
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
    std::vector<String> phrase_tokens;
    DataTypes argument_types;
    DataTypePtr result_type;
};

class FunctionHasPhraseOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "hasPhrase";

    static FunctionOverloadResolverPtr create(ContextPtr context)
    {
        return std::make_unique<FunctionHasPhraseOverloadResolver>(context);
    }

    explicit FunctionHasPhraseOverloadResolver(ContextPtr context);

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override;
};

}
