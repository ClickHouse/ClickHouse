#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ITokenExtractor.h>
#include <absl/container/flat_hash_map.h>

namespace DB
{

enum class HasAnyAllTokensMode : uint8_t
{
    Any,
    All
};

namespace traits
{
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
}

/// Map needle into a position (for bitmap operations).
using TokensWithPosition = absl::flat_hash_map<String, UInt64>;

template <class HasTokensTraits>
class FunctionHasAnyAllTokens : public IFunction
{
public:
    static constexpr auto name = HasTokensTraits::name;

    static FunctionPtr create(ContextPtr context);
    explicit FunctionHasAnyAllTokens<HasTokensTraits>(ContextPtr context);

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    void setTokenExtractor(std::unique_ptr<ITokenExtractor> new_token_extractor);
    void setSearchTokens(const std::vector<String> & new_search_tokens);

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;

private:
    const bool allow_experimental_full_text_index;
    std::unique_ptr<ITokenExtractor> token_extractor;
    std::optional<TokensWithPosition> search_tokens;
};
}
