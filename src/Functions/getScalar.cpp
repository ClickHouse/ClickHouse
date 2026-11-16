#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Interpreters/Context.h>
#include <Common/Macros.h>
#include <Core/Block.h>
#include <Core/Field.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** Get scalar value of sub queries from query context via IASTHash.
  */
class FunctionGetScalar : public IFunction, WithContext
{
public:
    static constexpr auto name = "__getScalar";
    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionGetScalar>(context_);
    }

    explicit FunctionGetScalar(ContextPtr context_) : WithContext(context_) {}

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool useDefaultImplementationForLowCardinalityColumns() const override
    {
        return false;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool isServerConstant() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1 || !isString(arguments[0].type) || !arguments[0].column || !isColumnConst(*arguments[0].column))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} accepts one const string argument", getName());
        auto scalar_name = assert_cast<const ColumnConst &>(*arguments[0].column).getValue<String>();
        ContextPtr query_context = getContext()->hasQueryContext() ? getContext()->getQueryContext() : getContext();
        scalar = query_context->getScalar(scalar_name).getByPosition(0);
        return scalar.type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        return ColumnConst::create(scalar.column, input_rows_count);
    }

private:
    mutable ColumnWithTypeAndName scalar;
};


/** Get special scalar values
  */
class FunctionGetSpecialScalar : public IFunction
{
public:
    static FunctionPtr create(ContextPtr context_, const char * function_name_, const char * scalar_name_)
    {
        return std::make_shared<FunctionGetSpecialScalar>(context_, function_name_, scalar_name_);
    }

    static ColumnWithTypeAndName createScalar(ContextPtr context_, const char * scalar_name_)
    {
        if (auto block = context_->tryGetSpecialScalar(scalar_name_))
            return block->getByPosition(0);
        if (context_->hasQueryContext())
        {
            if (context_->getQueryContext()->hasScalar(scalar_name_))
                return context_->getQueryContext()->getScalar(scalar_name_).getByPosition(0);
        }
        return {DataTypeUInt32().createColumnConst(1, 0), std::make_shared<DataTypeUInt32>(), scalar_name_};
    }

    FunctionGetSpecialScalar(ContextPtr context_, const char * function_name_, const char * scalar_name_)
        : scalar(createScalar(context_, scalar_name_)), is_distributed(context_->isDistributed()), function_name(function_name_)
    {
    }

    String getName() const override
    {
        return function_name;
    }

    bool isDeterministic() const override { return false; }

    bool isServerConstant() const override { return true; }

    bool isSuitableForConstantFolding() const override { return !is_distributed; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override
    {
        return scalar.type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto result = ColumnConst::create(scalar.column, input_rows_count);

        if (!isSuitableForConstantFolding())
            return result->convertToFullColumnIfConst();

        return result;
    }

private:
    ColumnWithTypeAndName scalar;
    bool is_distributed;
    const char * function_name;
};

}

REGISTER_FUNCTION(GetScalar)
{
    factory.registerFunction<FunctionGetScalar>(FunctionDocumentation::INTERNAL_FUNCTION_DOCS);

    FunctionDocumentation::Description description_shardNum = R"(
Returns the index of a shard which processes a part of data in a distributed query.
Indices begin from `1`.
If a query is not distributed then a constant value `0` is returned.
)";
    FunctionDocumentation::Syntax syntax_shardNum = "shardNum()";
    FunctionDocumentation::Arguments arguments_shardNum = {};
    FunctionDocumentation::ReturnedValue returned_value_shardNum = {"Returns the shard index or a constant `0`.", {"UInt32"}};
    FunctionDocumentation::Examples examples_shardNum = {
    {
        "Usage example",
        R"(
CREATE TABLE shard_num_example (dummy UInt8)
ENGINE=Distributed(test_cluster_two_shards_localhost, system, one, dummy);
SELECT dummy, shardNum(), shardCount() FROM shard_num_example;
        )",
        R"(
┌─dummy─┬─shardNum()─┬─shardCount()─┐
│     0 │          1 │            2 │
│     0 │          2 │            2 │
└───────┴────────────┴──────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_shardNum = {21, 9};
    FunctionDocumentation::Category category_shardNum = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_shardNum = {description_shardNum, syntax_shardNum, arguments_shardNum, {}, returned_value_shardNum, examples_shardNum, introduced_in_shardNum, category_shardNum};

    factory.registerFunction("shardNum", [](ContextPtr context){ return FunctionGetSpecialScalar::create(context, "shardNum", "_shard_num"); }, documentation_shardNum);

    FunctionDocumentation::Description description_shardCount = R"(
Returns the total number of shards for a distributed query.
If a query is not distributed then constant value `0` is returned.
)";
    FunctionDocumentation::Syntax syntax_shardCount = "shardCount()";
    FunctionDocumentation::Arguments arguments_shardCount = {};
    FunctionDocumentation::ReturnedValue returned_value_shardCount = {"Returns the total number of shards or `0`.", {"UInt32"}};
    FunctionDocumentation::Examples examples_shardCount = {
    {
        "Usage example",
        R"(
-- See shardNum() example above which also demonstrates shardCount()
CREATE TABLE shard_count_example (dummy UInt8)
ENGINE=Distributed(test_cluster_two_shards_localhost, system, one, dummy);
SELECT shardCount() FROM shard_count_example;
        )",
        R"(
┌─shardCount()─┐
│            2 │
│            2 │
└──────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_shardCount = {21, 9};
    FunctionDocumentation::Category category_shardCount = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_shardCount = {description_shardCount, syntax_shardCount, arguments_shardCount, {}, returned_value_shardCount, examples_shardCount, introduced_in_shardCount, category_shardCount};

    factory.registerFunction("shardCount", [](ContextPtr context){ return FunctionGetSpecialScalar::create(context, "shardCount", "_shard_count"); }, documentation_shardCount);
}

}
