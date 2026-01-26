#include "config.h"

#if USE_H3

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>
#include <base/range.h>

#include <h3api.h>


namespace DB
{
namespace
{

class FunctionH3GetRes0Indexes final : public IFunction
{
public:
    static constexpr auto name = "h3GetRes0Indexes";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3GetRes0Indexes>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return result_type->createColumn();

        std::vector<H3Index> res0_indexes;
        const auto cell_count = res0CellCount();
        res0_indexes.resize(cell_count);
        getRes0Cells(res0_indexes.data());

        Array res_indexes;
        res_indexes.insert(res_indexes.end(), res0_indexes.begin(), res0_indexes.end());

        return result_type->createColumnConst(input_rows_count, res_indexes);
    }
};

}

REGISTER_FUNCTION(H3GetRes0Indexes)
{
    FunctionDocumentation::Description description = R"(
Returns an array of all the resolution 0 [H3](#h3-index) indices.
    )";
    FunctionDocumentation::Syntax syntax = "h3GetRes0Indexes()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns an array of all resolution 0 H3 indices.",
        {"Array(UInt64)"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Get all resolution 0 H3 indices",
            "SELECT h3GetRes0Indexes() AS indexes",
            R"(
┌─indexes─────────────────────────────────────┐
│ [576495936675512319,576531121047601151,...] │
└─────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionH3GetRes0Indexes>(documentation);
}

}

#endif
