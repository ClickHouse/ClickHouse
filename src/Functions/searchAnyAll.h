#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/GinFilter.h>

namespace DB
{

namespace traits
{
struct SearchAnyTraits
{
    static constexpr String name = "searchAny";
    static constexpr GinSearchMode search_mode = GinSearchMode::Any;
};

struct SearchAllTraits
{
    static constexpr String name = "searchAll";
    static constexpr GinSearchMode search_mode = GinSearchMode::All;
};
}

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

    void setGinFilterParameters(const GinFilterParameters & params);

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;

private:
    const bool allow_experimental_full_text_index;
    std::optional<GinFilterParameters> parameters;
};
}
