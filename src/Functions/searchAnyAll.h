#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ITokenExtractor.h>
#include <absl/container/flat_hash_map.h>

namespace DB
{

enum class SearchAnyAllMode : uint8_t
{
    Any,
    All
};

namespace traits
{
struct SearchAnyTraits
{
    static constexpr String name = "searchAny";
    static constexpr SearchAnyAllMode mode = SearchAnyAllMode::Any;
};

struct SearchAllTraits
{
    static constexpr String name = "searchAll";
    static constexpr SearchAnyAllMode mode = SearchAnyAllMode::All;
};
}

/// Map needle into a position (for bitmap operations).
using FunctionSearchNeedles = absl::flat_hash_map<String, UInt64>;

template <class SearchTraits>
class FunctionSearchImpl : public IFunction
{
public:
    static constexpr auto name = SearchTraits::name;

    static FunctionPtr create(ContextPtr context);
    explicit FunctionSearchImpl<SearchTraits>(ContextPtr context);

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    void setTokenExtractor(std::unique_ptr<ITokenExtractor> new_token_extractor_);
    void setSearchTokens(const std::vector<String> & tokens);

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;

private:
    const bool allow_experimental_full_text_index;
    std::unique_ptr<ITokenExtractor> token_extractor;
    std::optional<FunctionSearchNeedles> needles;
};
}
