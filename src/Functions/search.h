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

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionSearchImpl>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isVariadic() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    void setGinFilterParameters(GinFilterParameters params)
    {
        /// Index parameters can be set multiple times.
        /// This happens exactly in a case that same searchAny/searchAll query is used again.
        /// This is fine because the parameters would be same.
        if (parameters.has_value() && params != parameters.value())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Function '{}': Different index parameters are set.", getName());
        parameters = std::move(params);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;
private:
    std::optional<GinFilterParameters> parameters;
};
}
