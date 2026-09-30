#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Common/VectorWithMemoryTracking.h>

#include <vector>

namespace DB
{

struct ITokenizer;

enum class HasAnyAllPhrasesMode : uint8_t
{
    Any,
    All,
};

struct HasAnyPhrasesTraits
{
    static constexpr auto name = "hasAnyPhrases";
    static constexpr HasAnyAllPhrasesMode mode = HasAnyAllPhrasesMode::Any;
};

struct HasAllPhrasesTraits
{
    static constexpr auto name = "hasAllPhrases";
    static constexpr HasAnyAllPhrasesMode mode = HasAnyAllPhrasesMode::All;
};

/// A single phrase: its ordered token sequence and the matching KMP failure table.
struct PhrasePattern
{
    VectorWithMemoryTracking<String> tokens;
    VectorWithMemoryTracking<size_t> failure;
};

using PhrasePatterns = std::vector<PhrasePattern>;

/// Precomputed phrases plus a flag telling whether the result is unconditionally 0.
/// `result_is_always_zero` covers:
///   - an empty phrase array (`hasAnyPhrases([])` / `hasAllPhrases([])`);
///   - for `hasAllPhrases`, any phrase that tokenizes to nothing (it can never match);
///   - for `hasAnyPhrases`, all phrases tokenizing to nothing.
struct HasAnyAllPhrasesState
{
    PhrasePatterns patterns;
    bool result_is_always_zero = false;
};

template <class Traits>
class ExecutableFunctionHasAnyAllPhrases final : public IExecutableFunction
{
public:
    static constexpr auto name = Traits::name;

    ExecutableFunctionHasAnyAllPhrases(std::shared_ptr<const ITokenizer> tokenizer_, const HasAnyAllPhrasesState & state_)
        : tokenizer(std::move(tokenizer_))
        , state(state_)
    {
    }

    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;

private:
    std::shared_ptr<const ITokenizer> tokenizer;
    const HasAnyAllPhrasesState & state;
};

template <class Traits>
class FunctionBaseHasAnyAllPhrases final : public IFunctionBase
{
public:
    static constexpr auto name = Traits::name;

    FunctionBaseHasAnyAllPhrases(
        std::shared_ptr<const ITokenizer> tokenizer_,
        HasAnyAllPhrasesState state_,
        DataTypes argument_types_,
        DataTypePtr result_type_)
        : tokenizer(std::move(tokenizer_))
        , state(std::move(state_))
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
    HasAnyAllPhrasesState state;
    DataTypes argument_types;
    DataTypePtr result_type;
};

template <class Traits>
class FunctionHasAnyAllPhrasesOverloadResolver final : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = Traits::name;

    static FunctionOverloadResolverPtr create(ContextPtr context)
    {
        return std::make_unique<FunctionHasAnyAllPhrasesOverloadResolver<Traits>>(context);
    }

    explicit FunctionHasAnyAllPhrasesOverloadResolver(ContextPtr context);

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override;
};

}
