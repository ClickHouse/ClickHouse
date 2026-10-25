#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/PreparedSets.h>
#include <Storages/MergeTree/MergeTreeIndexBloomFilter.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <string>
#include <tuple>
#include <utility>
#include <vector>

using namespace DB;

namespace DB
{
namespace Setting
{
extern const SettingsBool compile_expressions;
extern const SettingsUInt64 min_count_to_compile_expression;
}
}

namespace
{

const ActionsDAG::Node & addArrayLiteral(
    ActionsDAG & dag,
    const DataTypePtr & array_type,
    UInt64 value)
{
    return dag.addColumn(
        array_type->createColumnConst(0, Field(Array{Field(value)})), array_type, "literal_array", true);
}

bool canPruneSafeColumnWithIndexCast(
    const DataTypePtr & source_type,
    const DataTypePtr & destination_type,
    bool remove_cast_factory_handle = false,
    const String & cast_name = "CAST")
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto string_type = std::make_shared<DataTypeString>();

    ActionsDAG query_dag;
    const auto & query_input = query_dag.addInput("x", uint64_type);
    const auto & query_value = query_dag.addColumn(uint64_type->createColumnConst(0, Field(UInt64(1))), uint64_type, "query_value", true);
    const auto & predicate
        = query_dag.addFunction(FunctionFactory::instance().get("equals", context), {&query_input, &query_value}, "predicate");

    ActionsDAG index_dag;
    const auto & index_input = index_dag.addInput("x", uint64_type);
    const auto & source_input = index_dag.addInput("payload", source_type);
    const auto & type_literal
        = index_dag.addColumn(string_type->createColumnConst(0, Field(destination_type->getName())), string_type, "target_type", true);
    const auto cast_resolver = FunctionFactory::instance().get(cast_name, context);
    FunctionBasePtr cast_function = cast_resolver->build({
        {nullptr, source_type, "payload"},
        {type_literal.column, string_type, "target_type"},
    });
    if (remove_cast_factory_handle)
        cast_function->setFactoryHandle(nullptr);
    const auto & cast_node = index_dag.addFunction(cast_function, {&source_input, &type_literal}, "context_cast");

    const auto & safe_output = index_dag.addAlias(index_input, "x");
    const auto & cast_output = index_dag.addAlias(cast_node, "context_cast");
    index_dag.getOutputs().clear();
    index_dag.getOutputs().push_back(&safe_output);
    index_dag.getOutputs().push_back(&cast_output);

    Block header(
        ColumnsWithTypeAndName{
            {uint64_type->createColumn(), uint64_type, "x"},
            {destination_type->createColumn(), destination_type, "context_cast"},
        });
    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return !condition.alwaysUnknownOrTrue();
}

bool canPruneWithFloatOutput(const DataTypePtr & source_type, const Field & source_value, bool use_cast, bool add_plus)
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto float64_type = std::make_shared<DataTypeFloat64>();
    const auto string_type = std::make_shared<DataTypeString>();
    EXPECT_FALSE(add_plus && !use_cast);

    ActionsDAG query_dag;
    const auto & query_input = query_dag.addInput("x", uint64_type);
    const auto & query_value = query_dag.addColumn(uint64_type->createColumnConst(0, Field(UInt64(1))), uint64_type, "query_value", true);
    const auto & predicate
        = query_dag.addFunction(FunctionFactory::instance().get("equals", context), {&query_input, &query_value}, "predicate");

    ActionsDAG index_dag;
    const auto & index_input = index_dag.addInput("x", uint64_type);
    const ActionsDAG::Node * cast_node = nullptr;
    const ActionsDAG::Node * folded_node = nullptr;
    if (use_cast)
    {
        const auto & source_literal
            = index_dag.addColumn(source_type->createColumnConst(0, source_value), source_type, "source_literal", true);
        const auto & target_type
            = index_dag.addColumn(string_type->createColumnConst(0, Field(String("Float64"))), string_type, "target_type", true);
        cast_node = &index_dag.addFunction(
            FunctionFactory::instance().get("CAST", context), {&source_literal, &target_type}, add_plus ? "constant_cast" : "folded_value");
        folded_node = cast_node;
    }
    else
        folded_node = &index_dag.addColumn(source_type->createColumnConst(0, source_value), source_type, "folded_value", true);

    if (add_plus)
    {
        const auto & zero = index_dag.addColumn(float64_type->createColumnConst(0, Field(Float64(0))), float64_type, "zero", true);
        folded_node = &index_dag.addFunction(FunctionFactory::instance().get("plus", context), {folded_node, &zero}, "folded_plus");
    }

    index_dag.getOutputs().clear();
    index_dag.getOutputs().push_back(&index_input);
    index_dag.getOutputs().push_back(folded_node);
    index_dag.removeUnusedActions();

    EXPECT_EQ(index_dag.getOutputs()[1], folded_node);
    EXPECT_EQ(index_dag.getOutputs()[1]->result_name, folded_node->result_name);
    const auto expect_folded_column = [](const ActionsDAG::Node & node, bool has_conversion)
    {
        EXPECT_EQ(node.type, ActionsDAG::ActionType::COLUMN);
        EXPECT_NE(node.column, nullptr);
        if (has_conversion)
            EXPECT_NE(node.function_base, nullptr);
        else
            EXPECT_EQ(node.function_base, nullptr);
        EXPECT_TRUE(node.children.empty());
    };
    expect_folded_column(*folded_node, use_cast);

    Block header(
        ColumnsWithTypeAndName{
            {uint64_type->createColumn(), uint64_type, "x"},
            {float64_type->createColumn(), float64_type, folded_node->result_name},
        });
    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return !condition.alwaysUnknownOrTrue();
}

bool canPruneSafeColumnWithDateTimeConversion(const String & function_name, const DataTypePtr & source_type, bool include_timezone = false)
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto string_type = std::make_shared<DataTypeString>();

    ActionsDAG query_dag;
    const auto & query_input = query_dag.addInput("x", uint64_type);
    const auto & query_value = query_dag.addColumn(uint64_type->createColumnConst(1, Field(UInt64(1))), uint64_type, "query_value", true);
    const auto & predicate
        = query_dag.addFunction(FunctionFactory::instance().get("equals", context), {&query_input, &query_value}, "predicate");

    ActionsDAG index_dag;
    const auto & safe_input = index_dag.addInput("x", uint64_type);
    const auto & source_input = index_dag.addInput("payload", source_type);
    ColumnsWithTypeAndName function_arguments{{nullptr, source_type, "payload"}};
    ActionsDAG::NodeRawConstPtrs function_children{&source_input};

    if (function_name == "toDateTime64")
    {
        const auto scale_type = std::make_shared<DataTypeUInt8>();
        const auto & scale = index_dag.addColumn(scale_type->createColumnConst(0, Field(UInt64(3))), scale_type, "scale", true);
        function_arguments.emplace_back(scale.column, scale_type, "scale");
        function_children.push_back(&scale);
    }
    if (include_timezone)
    {
        const auto & timezone = index_dag.addColumn(string_type->createColumnConst(0, Field(String("UTC"))), string_type, "timezone", true);
        function_arguments.emplace_back(timezone.column, string_type, "timezone");
        function_children.push_back(&timezone);
    }

    const auto & converted = index_dag.addFunction(
        FunctionFactory::instance().get(function_name, context)->build(function_arguments), function_children, "converted");
    const auto & safe_output = index_dag.addAlias(safe_input, "x");
    const auto & converted_output = index_dag.addAlias(converted, "converted");
    index_dag.getOutputs().clear();
    index_dag.getOutputs().push_back(&safe_output);
    index_dag.getOutputs().push_back(&converted_output);

    Block header(
        ColumnsWithTypeAndName{
            {uint64_type->createColumn(), uint64_type, "x"},
            {converted.result_type->createColumn(), converted.result_type, "converted"},
        });
    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return !condition.alwaysUnknownOrTrue();
}

bool canPruneSafeColumnWithNativeConversion(const String & function_name, const DataTypePtr & source_type)
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();

    ActionsDAG query_dag;
    const auto & query_input = query_dag.addInput("x", uint64_type);
    const auto & query_value = query_dag.addColumn(uint64_type->createColumnConst(1, Field(UInt64(1))), uint64_type, "query_value", true);
    const auto & predicate
        = query_dag.addFunction(FunctionFactory::instance().get("equals", context), {&query_input, &query_value}, "predicate");

    ActionsDAG index_dag;
    const auto & safe_input = index_dag.addInput("x", uint64_type);
    const auto & source_input = index_dag.addInput("payload", source_type);
    const auto converted = FunctionFactory::instance().get(function_name, context)->build({{nullptr, source_type, "payload"}});
    const auto & converted_node = index_dag.addFunction(converted, {&source_input}, "converted");
    const auto & safe_output = index_dag.addAlias(safe_input, "x");
    const auto & converted_output = index_dag.addAlias(converted_node, "converted");
    index_dag.getOutputs().clear();
    index_dag.getOutputs().push_back(&safe_output);
    index_dag.getOutputs().push_back(&converted_output);

    Block header(
        ColumnsWithTypeAndName{
            {uint64_type->createColumn(), uint64_type, "x"},
            {converted_node.result_type->createColumn(), converted_node.result_type, "converted"},
        });
    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return !condition.alwaysUnknownOrTrue();
}

struct SpoofCallbacks
{
    size_t get_name = 0;
    size_t update_hash = 0;
};

class SpoofBloomCastFunction final : public IFunction
{
public:
    explicit SpoofBloomCastFunction(std::shared_ptr<SpoofCallbacks> callbacks_, String name_ = "CAST", size_t arguments_ = 2)
        : callbacks(std::move(callbacks_))
        , name(std::move(name_))
        , arguments(arguments_)
    {
    }

    size_t getNumberOfArguments() const override { return arguments; }

    String getName() const override
    {
        ++callbacks->get_name;
        return name;
    }

    void updateHash(SipHash &) const override { ++callbacks->update_hash; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

protected:
    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t) const override { return nullptr; }

private:
    std::shared_ptr<SpoofCallbacks> callbacks;
    String name;
    size_t arguments;
};

bool constructorDeclinesCanonicalCastSpoofWithoutCallbacks(const String & cast_name = "CAST")
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto string_type = std::make_shared<DataTypeString>();
    const auto datetime_type = std::make_shared<DataTypeDateTime>("UTC");

    ActionsDAG query_dag;
    const auto & query_input = query_dag.addInput("x", uint64_type);
    const auto & query_value = query_dag.addColumn(uint64_type->createColumnConst(0, Field(UInt64(1))), uint64_type, "query_value", true);
    const auto & predicate
        = query_dag.addFunction(FunctionFactory::instance().get("equals", context), {&query_input, &query_value}, "predicate");

    ActionsDAG index_dag;
    const auto & safe_input = index_dag.addInput("x", uint64_type);
    const auto & source_input = index_dag.addInput("payload", string_type);
    const auto & target_literal
        = index_dag.addColumn(string_type->createColumnConst(0, Field(String("DateTime('UTC')"))), string_type, "target_type", true);
    auto callbacks = std::make_shared<SpoofCallbacks>();
    auto spoof = std::make_shared<FunctionToFunctionBaseAdaptor>(
        std::make_shared<SpoofBloomCastFunction>(callbacks), DataTypes{string_type, string_type}, datetime_type);
    spoof->setFactoryHandle(FunctionFactory::instance().tryGetCanonicalCreator(cast_name));
    const auto & spoof_node = index_dag.addFunction(spoof, {&source_input, &target_literal}, "spoof_cast");
    const auto & safe_output = index_dag.addAlias(safe_input, "x");
    const auto & spoof_output = index_dag.addAlias(spoof_node, "spoof_cast");
    index_dag.getOutputs().clear();
    index_dag.getOutputs().push_back(&safe_output);
    index_dag.getOutputs().push_back(&spoof_output);
    callbacks->get_name = 0;
    callbacks->update_hash = 0;

    Block header(
        ColumnsWithTypeAndName{
            {uint64_type->createColumn(), uint64_type, "x"},
            {datetime_type->createColumn(), datetime_type, "spoof_cast"},
        });
    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return condition.alwaysUnknownOrTrue() && callbacks->get_name == 0 && callbacks->update_hash == 0;
}

bool constructorDeclinesCanonicalDateTimeConversionSpoofWithoutCallbacks()
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto string_type = std::make_shared<DataTypeString>();
    const auto datetime_type = std::make_shared<DataTypeDateTime>("UTC");

    ActionsDAG query_dag;
    const auto & query_input = query_dag.addInput("x", uint64_type);
    const auto & query_value = query_dag.addColumn(uint64_type->createColumnConst(0, Field(UInt64(1))), uint64_type, "query_value", true);
    const auto & predicate
        = query_dag.addFunction(FunctionFactory::instance().get("equals", context), {&query_input, &query_value}, "predicate");

    ActionsDAG index_dag;
    const auto & safe_input = index_dag.addInput("x", uint64_type);
    const auto & source_input = index_dag.addInput("payload", string_type);
    auto callbacks = std::make_shared<SpoofCallbacks>();
    auto spoof = std::make_shared<FunctionToFunctionBaseAdaptor>(
        std::make_shared<SpoofBloomCastFunction>(callbacks, "toDateTime", 1), DataTypes{string_type}, datetime_type);
    spoof->setFactoryHandle(FunctionFactory::instance().tryGetCanonicalCreator("toDateTime"));
    const auto & spoof_node = index_dag.addFunction(spoof, {&source_input}, "spoof_to_datetime");
    const auto & safe_output = index_dag.addAlias(safe_input, "x");
    const auto & spoof_output = index_dag.addAlias(spoof_node, "spoof_to_datetime");
    index_dag.getOutputs().clear();
    index_dag.getOutputs().push_back(&safe_output);
    index_dag.getOutputs().push_back(&spoof_output);
    callbacks->get_name = 0;
    callbacks->update_hash = 0;

    Block header(
        ColumnsWithTypeAndName{
            {uint64_type->createColumn(), uint64_type, "x"},
            {datetime_type->createColumn(), datetime_type, "spoof_to_datetime"},
        });
    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return condition.alwaysUnknownOrTrue() && callbacks->get_name == 0 && callbacks->update_hash == 0;
}

bool constructorDeclinesDateTimeCastInFunctionCaptureBody()
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto string_type = std::make_shared<DataTypeString>();
    const auto datetime_type = std::make_shared<DataTypeDateTime>("UTC");
    const auto array_string_type = std::make_shared<DataTypeArray>(string_type);
    const auto array_datetime_type = std::make_shared<DataTypeArray>(datetime_type);

    ActionsDAG query_dag;
    const auto & query_input = query_dag.addInput("x", uint64_type);
    const auto & query_value = query_dag.addColumn(uint64_type->createColumnConst(0, Field(UInt64(1))), uint64_type, "query_value", true);
    const auto & predicate
        = query_dag.addFunction(FunctionFactory::instance().get("equals", context), {&query_input, &query_value}, "predicate");

    ActionsDAG lambda_dag;
    const auto & captured_input = lambda_dag.addInput("captured", string_type);
    const auto & target_type
        = lambda_dag.addColumn(string_type->createColumnConst(0, Field(String("DateTime('UTC')"))), string_type, "target_type", true);
    const auto cast_resolver = FunctionFactory::instance().get("CAST", context);
    const auto cast_function = cast_resolver->build({
        {nullptr, string_type, "captured"},
        {target_type.column, string_type, "target_type"},
    });
    const auto & cast_node = lambda_dag.addFunction(cast_function, {&captured_input, &target_type}, "date_time_cast");
    const auto return_name = cast_node.result_name;
    lambda_dag.getOutputs().push_back(&cast_node);

    const auto lambda_capture = std::make_shared<FunctionCaptureOverloadResolver>(
        std::move(lambda_dag),
        ExpressionActionsSettings(context),
        Names{"captured"},
        NamesAndTypesList{{"v", string_type}},
        datetime_type,
        return_name,
        /* allow_constant_folding= */ true);

    ActionsDAG index_dag;
    const auto & safe_input = index_dag.addInput("x", uint64_type);
    const auto & captured_value = index_dag.addInput("captured", string_type);
    const auto & lambda = index_dag.addFunction(lambda_capture, {&captured_value}, "lambda");
    const auto & array_input = index_dag.addInput("values", array_string_type);
    const auto & mapped = index_dag.addFunction(FunctionFactory::instance().get("arrayMap", context), {&lambda, &array_input}, "mapped");
    const auto & safe_output = index_dag.addAlias(safe_input, "x");
    const auto & mapped_output = index_dag.addAlias(mapped, "mapped");
    index_dag.getOutputs().clear();
    index_dag.getOutputs().push_back(&safe_output);
    index_dag.getOutputs().push_back(&mapped_output);

    Block header(
        ColumnsWithTypeAndName{
            {uint64_type->createColumn(), uint64_type, "x"},
            {array_datetime_type->createColumn(), array_datetime_type, "mapped"},
        });
    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return condition.alwaysUnknownOrTrue();
}

bool constructorDeclinesDateTimeConversionInFunctionCaptureBody()
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto string_type = std::make_shared<DataTypeString>();
    const auto datetime_type = std::make_shared<DataTypeDateTime>("UTC");
    const auto array_string_type = std::make_shared<DataTypeArray>(string_type);
    const auto array_datetime_type = std::make_shared<DataTypeArray>(datetime_type);

    ActionsDAG lambda_dag;
    const auto & captured_input = lambda_dag.addInput("captured", string_type);
    const auto & converted = lambda_dag.addFunction(
        FunctionFactory::instance().get("toDateTime", context)->build({{nullptr, string_type, "captured"}}),
        {&captured_input},
        "date_time_conversion");
    const auto return_name = converted.result_name;
    lambda_dag.getOutputs().push_back(&converted);

    const auto lambda_capture = std::make_shared<FunctionCaptureOverloadResolver>(
        std::move(lambda_dag),
        ExpressionActionsSettings(context),
        Names{"captured"},
        NamesAndTypesList{{"v", string_type}},
        datetime_type,
        return_name,
        /* allow_constant_folding= */ true);

    ActionsDAG query_dag;
    const auto & query_input = query_dag.addInput("x", uint64_type);
    const auto & query_value = query_dag.addColumn(uint64_type->createColumnConst(0, Field(UInt64(1))), uint64_type, "query_value", true);
    const auto & predicate
        = query_dag.addFunction(FunctionFactory::instance().get("equals", context), {&query_input, &query_value}, "predicate");

    ActionsDAG index_dag;
    const auto & safe_input = index_dag.addInput("x", uint64_type);
    const auto & captured_value = index_dag.addInput("captured", string_type);
    const auto & lambda = index_dag.addFunction(lambda_capture, {&captured_value}, "lambda");
    const auto & array_input = index_dag.addInput("values", array_string_type);
    const auto & mapped = index_dag.addFunction(FunctionFactory::instance().get("arrayMap", context), {&lambda, &array_input}, "mapped");
    const auto & safe_output = index_dag.addAlias(safe_input, "x");
    const auto & mapped_output = index_dag.addAlias(mapped, "mapped");
    index_dag.getOutputs().clear();
    index_dag.getOutputs().push_back(&safe_output);
    index_dag.getOutputs().push_back(&mapped_output);

    Block header(
        ColumnsWithTypeAndName{
            {uint64_type->createColumn(), uint64_type, "x"},
            {array_datetime_type->createColumn(), array_datetime_type, "mapped"},
        });
    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return condition.alwaysUnknownOrTrue();
}

bool constructorDeclinesNativeConversionInFunctionCaptureBody()
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto string_type = std::make_shared<DataTypeString>();
    const auto float64_type = std::make_shared<DataTypeFloat64>();
    const auto array_string_type = std::make_shared<DataTypeArray>(string_type);
    const auto array_float64_type = std::make_shared<DataTypeArray>(float64_type);

    ActionsDAG lambda_dag;
    const auto & captured_input = lambda_dag.addInput("captured", string_type);
    const auto & converted = lambda_dag.addFunction(
        FunctionFactory::instance().get("toFloat64", context)->build({{nullptr, string_type, "captured"}}),
        {&captured_input},
        "float_conversion");
    lambda_dag.getOutputs().push_back(&converted);

    const auto lambda_capture = std::make_shared<FunctionCaptureOverloadResolver>(
        std::move(lambda_dag),
        ExpressionActionsSettings(context),
        Names{"captured"},
        NamesAndTypesList{{"v", string_type}},
        float64_type,
        converted.result_name,
        /* allow_constant_folding= */ true);

    ActionsDAG query_dag;
    const auto & query_input = query_dag.addInput("x", uint64_type);
    const auto & query_value = query_dag.addColumn(uint64_type->createColumnConst(0, Field(UInt64(1))), uint64_type, "query_value", true);
    const auto & predicate
        = query_dag.addFunction(FunctionFactory::instance().get("equals", context), {&query_input, &query_value}, "predicate");

    ActionsDAG index_dag;
    const auto & safe_input = index_dag.addInput("x", uint64_type);
    const auto & captured_value = index_dag.addInput("captured", string_type);
    const auto & lambda = index_dag.addFunction(lambda_capture, {&captured_value}, "lambda");
    const auto & array_input = index_dag.addInput("values", array_string_type);
    const auto & mapped = index_dag.addFunction(FunctionFactory::instance().get("arrayMap", context), {&lambda, &array_input}, "mapped");
    const auto & safe_output = index_dag.addAlias(safe_input, "x");
    const auto & mapped_output = index_dag.addAlias(mapped, "mapped");
    index_dag.getOutputs().clear();
    index_dag.getOutputs().push_back(&safe_output);
    index_dag.getOutputs().push_back(&mapped_output);

    Block header(
        ColumnsWithTypeAndName{
            {uint64_type->createColumn(), uint64_type, "x"},
            {array_float64_type->createColumn(), array_float64_type, "mapped"},
        });
    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return condition.alwaysUnknownOrTrue();
}

bool canPruneTupleArrayJoinInWithSharedBudget(bool first_component_mismatches)
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto array_type = std::make_shared<DataTypeArray>(uint64_type);

    ActionsDAG query_dag;
    ActionsDAG index_dag;
    const auto & query_input = query_dag.addInput("a", array_type);
    const auto & index_input = index_dag.addInput("a", array_type);

    const UInt64 query_first_value = first_component_mismatches ? 18 : 17;
    const UInt64 query_second_value = first_component_mismatches ? 17 : 18;

    const auto & query_first_literal = addArrayLiteral(query_dag, array_type, query_first_value);
    const auto & query_second_literal = addArrayLiteral(query_dag, array_type, query_second_value);
    const auto & query_first_array = query_dag.addFunction(
        FunctionFactory::instance().get("arrayConcat", context),
        {&query_input, &query_first_literal}, "query_first_array");
    const auto & query_second_array = query_dag.addFunction(
        FunctionFactory::instance().get("arrayConcat", context),
        {&query_input, &query_second_literal}, "query_second_array");
    const auto & query_first_join = query_dag.addArrayJoin(query_first_array, "query_first_join");
    const auto & query_second_join = query_dag.addArrayJoin(query_second_array, "query_second_join");
    const auto & query_tuple = query_dag.addFunction(
        FunctionFactory::instance().get("tuple", context), {&query_first_join, &query_second_join}, "query_tuple");

    const auto & index_literal = addArrayLiteral(index_dag, array_type, 17);
    const auto & index_array = index_dag.addFunction(
        FunctionFactory::instance().get("arrayConcat", context),
        {&index_input, &index_literal}, "index_array");

    /// The matcher budget is 4096. Each output costs at least one unit, so one extra
    /// non-matching output exhausts the first tuple component's shared atom budget.
    constexpr size_t output_count = 4097;
    ColumnsWithTypeAndName header_columns;
    header_columns.reserve(output_count);
    /// Deliberately make the query's RPNBuilder expression name miss the header and use positional fallback.
    for (size_t position = 0; position < output_count; ++position)
    {
        const auto output_name = "index_output_" + std::to_string(position);
        const auto & output = index_dag.addAlias(index_array, output_name);
        index_dag.getOutputs().push_back(&output);
        header_columns.emplace_back(array_type->createColumn(), array_type, output_name);
    }
    Block header(std::move(header_columns));

    auto first_set_value = ColumnUInt64::create();
    first_set_value->insertValue(query_first_value);
    auto second_set_value = ColumnUInt64::create();
    second_set_value->insertValue(query_second_value);
    ColumnsWithTypeAndName set_columns{
        {std::move(first_set_value), uint64_type, "first"},
        {std::move(second_set_value), uint64_type, "second"},
    };
    auto future_set = std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash{}, nullptr, std::move(set_columns), false, SizeLimits{});
    const auto & set_node = query_dag.addColumn(
        ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0), std::make_shared<DataTypeSet>(), "set");
    const auto & predicate = query_dag.addFunction(
        FunctionFactory::instance().get("in", context), {&query_tuple, &set_node}, "predicate");

    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return !condition.alwaysUnknownOrTrue();
}

bool canPruneDecimalToStringIn(const DataTypePtr & query_decimal_type, const DataTypePtr & index_decimal_type)
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto string_type = std::make_shared<DataTypeString>();

    ActionsDAG query_dag;
    ActionsDAG index_dag;
    const auto & query_input = query_dag.addInput("x", query_decimal_type);
    const auto & index_input = index_dag.addInput("x", index_decimal_type);
    const auto & query_to_string = query_dag.addFunction(
        FunctionFactory::instance().get("toString", context), {&query_input}, "query_to_string");
    const auto & index_to_string = index_dag.addFunction(
        FunctionFactory::instance().get("toString", context), {&index_input}, "index_to_string");
    /// DirectIN resolves the indexed expression by the canonical function column name.
    const auto & indexed_output = index_dag.addAlias(index_to_string, "toString(x)");
    index_dag.getOutputs().push_back(&indexed_output);

    ColumnsWithTypeAndName header_columns{{string_type->createColumn(), string_type, "toString(x)"}};
    Block header(std::move(header_columns));

    auto set_value = ColumnString::create();
    set_value->insertData("3", 1);
    ColumnsWithTypeAndName set_columns{{std::move(set_value), string_type, "value"}};
    auto future_set = std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash{}, nullptr, std::move(set_columns), false, SizeLimits{});
    const auto & set_node = query_dag.addColumn(
        ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0), std::make_shared<DataTypeSet>(), "set");
    const auto & predicate = query_dag.addFunction(
        FunctionFactory::instance().get("in", context), {&query_to_string, &set_node}, "predicate");

    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return !condition.alwaysUnknownOrTrue();
}

struct TestFormatDateTimeMatch
{
    bool can_prune;
    bool query_is_direct_format_date_time;
    bool index_is_direct_format_date_time;
    bool output_name_matches;
};

TestFormatDateTimeMatch canPruneFormatDateTimeIn()
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto datetime_type = std::make_shared<DataTypeDateTime>("UTC");
    const auto string_type = std::make_shared<DataTypeString>();

    ActionsDAG query_dag;
    ActionsDAG index_dag;
    const auto & query_input = query_dag.addInput("ts", datetime_type);
    const auto & index_input = index_dag.addInput("ts", datetime_type);
    const auto & query_format = query_dag.addColumn(string_type->createColumnConst(0, Field(String("%F"))), string_type, "format", true);
    const auto & index_format = index_dag.addColumn(string_type->createColumnConst(0, Field(String("%F"))), string_type, "format", true);
    const auto format_date_time = FunctionFactory::instance().get("formatDateTime", context);
    const auto & query_formatted = query_dag.addFunction(format_date_time, {&query_input, &query_format}, "query_formatted");
    const auto & index_formatted = index_dag.addFunction(format_date_time, {&index_input, &index_format}, "index_formatted");

    const auto output_name = RPNBuilderTreeNode(&query_formatted, context).getColumnName();
    const auto & index_output = index_dag.addAlias(index_formatted, output_name);
    index_dag.getOutputs().push_back(&index_output);
    Block header(ColumnsWithTypeAndName{{string_type->createColumn(), string_type, output_name}});

    auto set_value = ColumnString::create();
    set_value->insertData("2020-01-01", 10);
    ColumnsWithTypeAndName set_columns{{std::move(set_value), string_type, "value"}};
    auto future_set = std::make_shared<FutureSetFromTuple>(FutureSet::Hash{}, nullptr, std::move(set_columns), false, SizeLimits{});
    const auto & set_node
        = query_dag.addColumn(ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0), std::make_shared<DataTypeSet>(), "set");
    const auto & predicate
        = query_dag.addFunction(FunctionFactory::instance().get("in", context), {&query_formatted, &set_node}, "predicate");

    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    const auto * canonical_creator = FunctionFactory::instance().tryGetCanonicalCreator("formatDateTime");
    return {
        .can_prune = !condition.alwaysUnknownOrTrue(),
        .query_is_direct_format_date_time = query_formatted.function_base->getFactoryHandle() == canonical_creator,
        .index_is_direct_format_date_time = index_formatted.function_base->getFactoryHandle() == canonical_creator,
        .output_name_matches = output_name == RPNBuilderTreeNode(&index_formatted, context).getColumnName(),
    };
}

bool canPruneLowerStringIn()
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto string_type = std::make_shared<DataTypeString>();

    ActionsDAG query_dag;
    ActionsDAG index_dag;
    const auto & query_input = query_dag.addInput("s", string_type);
    const auto & index_input = index_dag.addInput("s", string_type);
    const auto lower = FunctionFactory::instance().get("lower", context);
    const auto & query_lower = query_dag.addFunction(lower, {&query_input}, "query_lower");
    const auto & index_lower = index_dag.addFunction(lower, {&index_input}, "index_lower");
    const auto output_name = RPNBuilderTreeNode(&query_lower, context).getColumnName();
    const auto & index_output = index_dag.addAlias(index_lower, output_name);
    index_dag.getOutputs().push_back(&index_output);
    Block header(ColumnsWithTypeAndName{{string_type->createColumn(), string_type, output_name}});

    auto set_value = ColumnString::create();
    set_value->insertData("x", 1);
    ColumnsWithTypeAndName set_columns{{std::move(set_value), string_type, "value"}};
    auto future_set = std::make_shared<FutureSetFromTuple>(FutureSet::Hash{}, nullptr, std::move(set_columns), false, SizeLimits{});
    const auto & set_node
        = query_dag.addColumn(ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0), std::make_shared<DataTypeSet>(), "set");
    const auto & predicate = query_dag.addFunction(FunctionFactory::instance().get("in", context), {&query_lower, &set_node}, "predicate");

    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return !condition.alwaysUnknownOrTrue();
}

bool canPruneUnaryFunctionIn(const String & function_name, const DataTypePtr & argument_type)
{
    tryRegisterFunctions();
    const auto context = getContext().context;

    ActionsDAG query_dag;
    ActionsDAG index_dag;
    const auto & query_input = query_dag.addInput("x", argument_type);
    const auto & index_input = index_dag.addInput("x", argument_type);
    const auto & query_function
        = query_dag.addFunction(FunctionFactory::instance().get(function_name, context), {&query_input}, "query_function");
    const auto & index_function
        = index_dag.addFunction(FunctionFactory::instance().get(function_name, context), {&index_input}, "index_function");

    const auto result_type = query_function.result_type;
    const auto output_name = RPNBuilderTreeNode(&query_function, context).getColumnName();
    const auto & index_output = index_dag.addAlias(index_function, output_name);
    index_dag.getOutputs().push_back(&index_output);
    Block header(ColumnsWithTypeAndName{{result_type->createColumn(), result_type, output_name}});

    auto set_value = result_type->createColumn();
    set_value->insertDefault();
    ColumnsWithTypeAndName set_columns{{std::move(set_value), result_type, "value"}};
    auto future_set = std::make_shared<FutureSetFromTuple>(FutureSet::Hash{}, nullptr, std::move(set_columns), false, SizeLimits{});
    const auto & set_node
        = query_dag.addColumn(ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0), std::make_shared<DataTypeSet>(), "set");
    const auto & predicate
        = query_dag.addFunction(FunctionFactory::instance().get("in", context), {&query_function, &set_node}, "predicate");

    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return !condition.alwaysUnknownOrTrue();
}

struct TestConstantArgumentInResult
{
    bool can_prune;
    bool query_is_native_tuple_constant;
    bool index_is_native_tuple_constant;
    bool query_is_native_array_nothing;
    bool index_is_native_array_nothing;
};

bool isNativeTupleConstant(const ActionsDAG::Node & node)
{
    const auto * tuple_type = node.result_type ? typeid_cast<const DataTypeTuple *>(node.result_type.get()) : nullptr;
    if (!tuple_type || typeid(*tuple_type) != typeid(DataTypeTuple) || node.result_type->getCustomName()
        || node.result_type->getCustomSerialization())
        return false;

    const auto * constant = node.column ? typeid_cast<const ColumnConst *>(node.column.get()) : nullptr;
    if (!constant)
        return false;

    const IColumn & data = constant->getDataColumn();
    const auto * tuple = typeid_cast<const ColumnTuple *>(&data);
    if (!tuple || typeid(data) != typeid(ColumnTuple) || tuple->size() != 1
        || tuple->getColumns().size() != tuple_type->getElements().size())
        return false;

    for (size_t i = 0; i < tuple->getColumns().size(); ++i)
        if (tuple->getColumn(i).size() != 1)
            return false;

    return true;
}

bool isNativeEmptyArrayNothingConstant(const ActionsDAG::Node & node)
{
    const auto * array_type = node.result_type ? typeid_cast<const DataTypeArray *>(node.result_type.get()) : nullptr;
    if (!array_type || typeid(*array_type) != typeid(DataTypeArray) || node.result_type->getCustomName()
        || node.result_type->getCustomSerialization())
        return false;

    const auto & nested_type = array_type->getNestedType();
    const IDataType * nested_type_raw = nested_type.get();
    if (!nested_type_raw || typeid(*nested_type_raw) != typeid(DataTypeNothing) || nested_type->getCustomName()
        || nested_type->getCustomSerialization())
        return false;

    const auto * constant = node.column ? typeid_cast<const ColumnConst *>(node.column.get()) : nullptr;
    if (!constant)
        return false;

    const IColumn & data = constant->getDataColumn();
    const auto * array = typeid_cast<const ColumnArray *>(&data);
    if (!array || typeid(data) != typeid(ColumnArray) || array->size() != 1 || array->getOffsets().size() != 1
        || array->getOffsets()[0] != 0)
        return false;

    const IColumn * nested = array->getDataPtr().get();
    return nested && typeid(*nested) == typeid(ColumnNothing) && nested->size() == 0;
}

TestConstantArgumentInResult canPruneCityHash64WithConstantArgument(
    const DataTypePtr & query_constant_type,
    const Field & query_constant_value,
    const DataTypePtr & index_constant_type,
    const Field & index_constant_value)
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();

    ActionsDAG query_dag;
    ActionsDAG index_dag;
    const auto & query_input = query_dag.addInput("x", uint64_type);
    const auto & index_input = index_dag.addInput("x", uint64_type);
    const auto & query_constant
        = query_dag.addColumn(query_constant_type->createColumnConst(0, query_constant_value), query_constant_type, "query_constant", true);
    const auto & index_constant
        = index_dag.addColumn(index_constant_type->createColumnConst(0, index_constant_value), index_constant_type, "index_constant", true);
    const auto city_hash = FunctionFactory::instance().get("cityHash64", context);
    const auto & query_hash = query_dag.addFunction(city_hash, {&query_input, &query_constant}, "query_hash");
    const auto & index_hash = index_dag.addFunction(city_hash, {&index_input, &index_constant}, "index_hash");

    /// Force the index output name to the query expression name so negative controls exercise typed constant
    /// comparison rather than succeeding or failing because the expression names differ.
    const auto output_name = RPNBuilderTreeNode(&query_hash, context).getColumnName();
    const auto & index_output = index_dag.addAlias(index_hash, output_name);
    index_dag.getOutputs().push_back(&index_output);

    auto set_value = ColumnUInt64::create();
    set_value->insertValue(0);
    ColumnsWithTypeAndName set_columns{{std::move(set_value), uint64_type, "value"}};
    auto future_set = std::make_shared<FutureSetFromTuple>(FutureSet::Hash{}, nullptr, std::move(set_columns), false, SizeLimits{});
    const auto & set_node
        = query_dag.addColumn(ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0), std::make_shared<DataTypeSet>(), "set");
    const auto & predicate = query_dag.addFunction(FunctionFactory::instance().get("in", context), {&query_hash, &set_node}, "predicate");

    Block header(ColumnsWithTypeAndName{{uint64_type->createColumn(), uint64_type, output_name}});
    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return {
        .can_prune = !condition.alwaysUnknownOrTrue(),
        .query_is_native_tuple_constant = isNativeTupleConstant(query_constant),
        .index_is_native_tuple_constant = isNativeTupleConstant(index_constant),
        .query_is_native_array_nothing = isNativeEmptyArrayNothingConstant(query_constant),
        .index_is_native_array_nothing = isNativeEmptyArrayNothingConstant(index_constant),
    };
}

bool canPruneLambdaInWithFunctionCount(size_t function_count)
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto array_type = std::make_shared<DataTypeArray>(uint64_type);

    const auto add_mapped_array = [&](ActionsDAG & dag, const String & prefix) -> const ActionsDAG::Node &
    {
        ActionsDAG lambda_dag;
        const auto & lambda_argument = lambda_dag.addInput("v", uint64_type);
        const auto & one = lambda_dag.addColumn(
            uint64_type->createColumnConst(0, Field(UInt64(1))), uint64_type, "one", true);
        const auto plus = FunctionFactory::instance().get("plus", context);

        /// Keep the returned expression small while storing more body DAG nodes than the matcher's
        /// shared work budget can admit. Every node must be budgeted, including nodes not in outputs.
        const ActionsDAG::Node * output = nullptr;
        for (size_t i = 0; i < function_count; ++i)
            output = &lambda_dag.addFunction(plus, {&lambda_argument, &one}, "unused_" + std::to_string(i));

        /// Keep a native Nullable(Nothing) leaf in the matched body. In the oversized case it is
        /// deliberately after the budget-consuming nodes, so exhaustion must decline the whole match.
        const auto nullable_nothing_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
        const auto & null_constant = lambda_dag.addColumn(
            nullable_nothing_type->createColumnConst(0, Field(Null())), nullable_nothing_type, "nullable_null", true);
        const auto & is_null = lambda_dag.addFunction(
            FunctionFactory::instance().get("isNull", context), {&null_constant}, "nullable_null_check");
        output = &lambda_dag.addFunction(plus, {&lambda_argument, &is_null}, "nullable_result");
        lambda_dag.getOutputs().clear();
        lambda_dag.getOutputs().push_back(output);
        const auto return_name = output->result_name;

        const auto lambda_capture = std::make_shared<FunctionCaptureOverloadResolver>(
            std::move(lambda_dag),
            ExpressionActionsSettings(context),
            Names{},
            NamesAndTypesList{{"v", uint64_type}},
            uint64_type,
            return_name,
            /* allow_constant_folding= */ true);

        const auto & array_input = dag.addInput("a", array_type);
        const auto & lambda = dag.addFunction(lambda_capture, {}, prefix + "lambda");
        return dag.addFunction(
            FunctionFactory::instance().get("arrayMap", context), {&lambda, &array_input}, prefix + "mapped");
    };

    ActionsDAG query_dag;
    ActionsDAG index_dag;
    const auto & query_mapped = add_mapped_array(query_dag, "query_");
    const auto & index_mapped = add_mapped_array(index_dag, "index_");
    const auto index_name = RPNBuilderTreeNode(&index_mapped, context).getColumnName();
    const auto & index_output = index_dag.addAlias(index_mapped, index_name);
    index_dag.getOutputs().clear();
    index_dag.getOutputs().push_back(&index_output);

    auto set_value = array_type->createColumn();
    set_value->insert(Field(Array{Field(UInt64(4))}));
    ColumnsWithTypeAndName set_columns{{std::move(set_value), array_type, "value"}};
    auto future_set = std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash{}, nullptr, std::move(set_columns), false, SizeLimits{});
    const auto & set_node = query_dag.addColumn(
        ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0), std::make_shared<DataTypeSet>(), "set");
    const auto & predicate = query_dag.addFunction(
        FunctionFactory::instance().get("in", context), {&query_mapped, &set_node}, "predicate");

    Block header(ColumnsWithTypeAndName{{array_type->createColumn(), array_type, index_name}});
    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return !condition.alwaysUnknownOrTrue();
}

enum class TestLambdaCarrier
{
    ColumnFunction,
    FunctionCapture,
};

enum class TestIndexOutputName
{
    Canonical,
    QueryNameCollision,
    DistinctAlias,
};

struct TestLambdaSpec
{
    TestLambdaCarrier carrier = TestLambdaCarrier::ColumnFunction;
    bool capture_value = false;
    UInt64 captured_value = 0;
    UInt64 body_value = 1;
    bool capture_is_constant = true;
    bool allow_constant_folding = true;
    String capture_name = "captured";
    String body_capture_input_name = {};
    String outer_capture_name = "captured";
    String body_output_name = "result";
    bool override_return_name = false;
    String return_name_override = {};
    bool compile_expressions = false;
    bool add_compilable_chain = false;
    bool bypass_lambda_snapshot = false;
};

struct TestLambdaInResult
{
    bool can_prune;
    bool query_is_column_function;
    bool index_is_column_function;
    bool query_is_function_capture;
    bool index_is_function_capture;
    bool query_allows_constant_folding;
    bool index_allows_constant_folding;
    bool canonical_names_equal;
    bool header_name_matches_query;
    bool header_name_matches_index;
    bool query_has_compiled_runtime_action;
    bool index_has_compiled_runtime_action;
    bool query_has_uncompiled_semantic_snapshot;
    bool index_has_uncompiled_semantic_snapshot;
};

struct TestMappedLambda
{
    const ActionsDAG::Node * lambda;
    const ActionsDAG::Node * mapped_array;
};

#if USE_EMBEDDED_COMPILER
struct TestCompiledContextDependentLambdaResult
{
    bool runtime_has_compiled_action;
    bool has_uncompiled_semantic_snapshot;
    bool condition_declines_pruning;
};
#endif

bool isColumnFunctionLambda(const ActionsDAG::Node & node)
{
    if (node.type != ActionsDAG::ActionType::COLUMN || !node.column)
        return false;

    const auto * column_function = typeid_cast<const ColumnFunction *>(&node.column->getDataColumn());
    return column_function && typeid_cast<const FunctionExpression *>(column_function->getFunction().get());
}

bool isFunctionCaptureLambda(const ActionsDAG::Node & node)
{
    return node.type == ActionsDAG::ActionType::FUNCTION
        && node.function_base
        && typeid_cast<const FunctionCapture *>(node.function_base.get());
}

const LambdaCapture * getNativeLambdaCapture(const ActionsDAG::Node & node)
{
    if (node.type == ActionsDAG::ActionType::COLUMN && node.column)
    {
        const auto * column_function = typeid_cast<const ColumnFunction *>(&node.column->getDataColumn());
        if (!column_function)
            return nullptr;
        const auto * function_expression = typeid_cast<const FunctionExpression *>(column_function->getFunction().get());
        return function_expression ? &function_expression->getCapture() : nullptr;
    }

    if (node.type == ActionsDAG::ActionType::FUNCTION && node.function_base)
    {
        const auto * function_capture = typeid_cast<const FunctionCapture *>(node.function_base.get());
        return function_capture ? &function_capture->getCapture() : nullptr;
    }

    return nullptr;
}

const ActionsDAG * getNativeLambdaActionsDAG(const ActionsDAG::Node & node, bool semantic_analysis)
{
    if (node.type == ActionsDAG::ActionType::COLUMN && node.column)
    {
        const auto * column_function = typeid_cast<const ColumnFunction *>(&node.column->getDataColumn());
        if (!column_function)
            return nullptr;
        const auto * function_expression = typeid_cast<const FunctionExpression *>(column_function->getFunction().get());
        if (!function_expression)
            return nullptr;
        return semantic_analysis ? &function_expression->getActionsDAGForSemanticAnalysis() : &function_expression->getAcionsDAG();
    }

    if (node.type == ActionsDAG::ActionType::FUNCTION && node.function_base)
    {
        const auto * function_capture = typeid_cast<const FunctionCapture *>(node.function_base.get());
        if (!function_capture)
            return nullptr;
        return semantic_analysis ? &function_capture->getActionsDAGForSemanticAnalysis() : &function_capture->getAcionsDAG();
    }

    return nullptr;
}

bool hasCompiledFunction(const ActionsDAG * dag)
{
    if (!dag)
        return false;

    for (const auto & node : dag->getNodes())
        if (node.is_function_compiled)
            return true;

    return false;
}

ExpressionActionsSettings getLambdaActionsSettings(const ContextPtr & context, bool compile_expressions)
{
    if (!compile_expressions)
        return ExpressionActionsSettings(context);

    Settings settings = context->getSettingsRef();
    settings[Setting::compile_expressions] = true;
    settings[Setting::min_count_to_compile_expression] = 0;
    return ExpressionActionsSettings(settings, CompileExpressions::yes);
}

TestMappedLambda addMappedArrayWithLambda(
    ActionsDAG & dag,
    const String & prefix,
    const TestLambdaSpec & spec,
    const ContextPtr & context,
    const DataTypePtr & uint64_type,
    const DataTypePtr & array_type)
{
    ActionsDAG lambda_dag;
    const auto & lambda_argument = lambda_dag.addInput("v", uint64_type);
    const ActionsDAG::Node * captured_input = nullptr;
    if (spec.capture_value)
    {
        /// A duplicated capture/argument name is deliberately malformed metadata. Reuse the
        /// existing input so construction reaches the matcher without ActionsDAG rejecting it.
        if (spec.carrier == TestLambdaCarrier::ColumnFunction && spec.capture_name == "v")
            captured_input = &lambda_argument;
        else
            captured_input = &lambda_dag.addInput(
                spec.body_capture_input_name.empty() ? spec.capture_name : spec.body_capture_input_name,
                uint64_type);
    }

    const auto & body_value = lambda_dag.addColumn(
        uint64_type->createColumnConst(0, Field(UInt64(spec.body_value))), uint64_type, "body_value", true);
    const ActionsDAG::Node * output = nullptr;
    if (spec.add_compilable_chain)
    {
        const auto & intermediate = lambda_dag.addFunction(
            FunctionFactory::instance().get("plus", context),
            {&lambda_argument, captured_input ? captured_input : &body_value},
            "jit_intermediate");
        const auto & zero = lambda_dag.addColumn(uint64_type->createColumnConst(0, Field(UInt64(0))), uint64_type, "jit_identity", true);
        output = &lambda_dag.addFunction(FunctionFactory::instance().get("plus", context), {&intermediate, &zero}, spec.body_output_name);
    }
    else
    {
        output = &lambda_dag.addFunction(
            FunctionFactory::instance().get("plus", context),
            {&lambda_argument, captured_input ? captured_input : &body_value},
            spec.body_output_name);
    }
    lambda_dag.getOutputs().push_back(output);

    const Names captured_names = spec.capture_value ? Names{spec.capture_name} : Names{};
    const auto lambda_capture = std::make_shared<LambdaCapture>(LambdaCapture{
        .captured_names = captured_names,
        .captured_types = spec.capture_value ? DataTypes{uint64_type} : DataTypes{},
        .lambda_arguments = NamesAndTypesList{{"v", uint64_type}},
        .return_name = spec.override_return_name ? spec.return_name_override : output->result_name,
        .return_type = uint64_type,
        .allow_constant_folding = spec.allow_constant_folding,
    });
    const auto return_name = spec.override_return_name ? spec.return_name_override : output->result_name;

    const auto & array_input = dag.addInput("a", array_type);
    const ActionsDAG::Node * lambda_node = nullptr;
    if (spec.carrier == TestLambdaCarrier::ColumnFunction)
    {
        ColumnsWithTypeAndName captures;
        if (spec.capture_value)
        {
            captures.emplace_back(
                uint64_type->createColumnConst(1, Field(UInt64(spec.captured_value))),
                uint64_type,
                spec.capture_name);
        }

        const auto actions_settings = getLambdaActionsSettings(context, spec.compile_expressions);
        auto expression_actions = spec.bypass_lambda_snapshot
            ? std::make_shared<ExpressionActions>(std::move(lambda_dag), actions_settings)
            : createLambdaExpressionActions(std::move(lambda_dag), actions_settings, *lambda_capture);
        FunctionBasePtr function = std::make_shared<FunctionExpression>(lambda_capture, std::move(expression_actions));
        auto column_function = ColumnFunction::create(1, std::move(function), captures);
        auto constant_column_function = ColumnConst::create(std::move(column_function), 0);
        lambda_node = &dag.addColumn(
            std::move(constant_column_function),
            std::make_shared<DataTypeFunction>(DataTypes{uint64_type}, uint64_type),
            prefix + "lambda",
            true);
    }
    else
    {
        auto lambda_resolver = std::make_shared<FunctionCaptureOverloadResolver>(
            std::move(lambda_dag),
            getLambdaActionsSettings(context, spec.compile_expressions),
            captured_names,
            NamesAndTypesList{{"v", uint64_type}},
            uint64_type,
            return_name,
            /* allow_constant_folding= */ spec.allow_constant_folding);
        if (spec.capture_value)
        {
            const ActionsDAG::Node * captured_column = nullptr;
            if (spec.capture_is_constant)
            {
                captured_column = &dag.addColumn(
                    uint64_type->createColumnConst(1, Field(UInt64(spec.captured_value))),
                    uint64_type,
                    prefix + "capture_value",
                    true);
            }
            else
                captured_column = &dag.addInput(spec.outer_capture_name, uint64_type);
            lambda_node = &dag.addFunction(lambda_resolver, {captured_column}, prefix + "lambda");
        }
        else
            lambda_node = &dag.addFunction(lambda_resolver, {}, prefix + "lambda");
    }

    const auto & mapped_array = dag.addFunction(
        FunctionFactory::instance().get("arrayMap", context), {lambda_node, &array_input}, prefix + "mapped");
    return {lambda_node, &mapped_array};
}

#if USE_EMBEDDED_COMPILER
TestCompiledContextDependentLambdaResult constructorDeclinesDateTimeConversionInPartiallyCompiledFunctionCaptureBody()
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto string_type = std::make_shared<DataTypeString>();
    const auto datetime_type = std::make_shared<DataTypeDateTime>("UTC");
    const auto tuple_type = std::make_shared<DataTypeTuple>(DataTypes{datetime_type, uint64_type});
    const auto array_uint64_type = std::make_shared<DataTypeArray>(uint64_type);
    const auto array_tuple_type = std::make_shared<DataTypeArray>(tuple_type);

    ActionsDAG lambda_dag;
    const auto & captured_input = lambda_dag.addInput("captured", string_type);
    const auto & lambda_argument = lambda_dag.addInput("v", uint64_type);
    const auto & converted = lambda_dag.addFunction(
        FunctionFactory::instance().get("toDateTime", context)->build({{nullptr, string_type, "captured"}}),
        {&captured_input},
        "date_time_conversion");
    const auto & one = lambda_dag.addColumn(uint64_type->createColumnConst(0, Field(UInt64(1))), uint64_type, "one", true);
    const auto & intermediate
        = lambda_dag.addFunction(FunctionFactory::instance().get("plus", context), {&lambda_argument, &one}, "jit_intermediate");
    const auto & zero = lambda_dag.addColumn(uint64_type->createColumnConst(0, Field(UInt64(0))), uint64_type, "jit_identity", true);
    const auto & safe_compiled_result
        = lambda_dag.addFunction(FunctionFactory::instance().get("plus", context), {&intermediate, &zero}, "safe_compiled_result");
    const auto & lambda_result
        = lambda_dag.addFunction(FunctionFactory::instance().get("tuple", context), {&converted, &safe_compiled_result}, "lambda_result");
    lambda_dag.getOutputs().push_back(&lambda_result);
    const String lambda_return_name = lambda_result.result_name;

    auto lambda_resolver = std::make_shared<FunctionCaptureOverloadResolver>(
        std::move(lambda_dag),
        getLambdaActionsSettings(context, /*compile_expressions=*/true),
        Names{"captured"},
        NamesAndTypesList{{"v", uint64_type}},
        tuple_type,
        lambda_return_name,
        /* allow_constant_folding= */ false);

    ActionsDAG query_dag;
    const auto & query_input = query_dag.addInput("x", uint64_type);
    const auto & query_value = query_dag.addColumn(uint64_type->createColumnConst(0, Field(UInt64(1))), uint64_type, "query_value", true);
    const auto & predicate
        = query_dag.addFunction(FunctionFactory::instance().get("equals", context), {&query_input, &query_value}, "predicate");

    ActionsDAG index_dag;
    const auto & safe_input = index_dag.addInput("x", uint64_type);
    const auto & captured_value = index_dag.addInput("captured", string_type);
    const auto & lambda = index_dag.addFunction(lambda_resolver, {&captured_value}, "lambda");
    const auto & array_input = index_dag.addInput("values", array_uint64_type);
    const auto & mapped = index_dag.addFunction(FunctionFactory::instance().get("arrayMap", context), {&lambda, &array_input}, "mapped");
    const auto & safe_output = index_dag.addAlias(safe_input, "x");
    const auto & mapped_output = index_dag.addAlias(mapped, "mapped");
    index_dag.getOutputs().clear();
    index_dag.getOutputs().push_back(&safe_output);
    index_dag.getOutputs().push_back(&mapped_output);

    const auto * runtime_dag = getNativeLambdaActionsDAG(lambda, /*semantic_analysis=*/false);
    const auto * semantic_dag = getNativeLambdaActionsDAG(lambda, /*semantic_analysis=*/true);
    const auto * capture = getNativeLambdaCapture(lambda);
    const bool runtime_has_compiled_action = hasCompiledFunction(runtime_dag);
    const bool has_uncompiled_semantic_snapshot
        = capture && capture->uncompiled_actions_dag && semantic_dag && !hasCompiledFunction(semantic_dag);

    Block header(
        ColumnsWithTypeAndName{
            {uint64_type->createColumn(), uint64_type, "x"},
            {array_tuple_type->createColumn(), array_tuple_type, "mapped"},
        });
    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return {
        .runtime_has_compiled_action = runtime_has_compiled_action,
        .has_uncompiled_semantic_snapshot = has_uncompiled_semantic_snapshot,
        .condition_declines_pruning = condition.alwaysUnknownOrTrue(),
    };
}
#endif

TestLambdaInResult canPruneLambdaInWithCarriers(
    const TestLambdaSpec & query_spec,
    const TestLambdaSpec & index_spec,
    TestIndexOutputName output_name_mode = TestIndexOutputName::Canonical)
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto array_type = std::make_shared<DataTypeArray>(uint64_type);

    ActionsDAG query_dag;
    ActionsDAG index_dag;
    const auto query_mapping = addMappedArrayWithLambda(
        query_dag, "query_", query_spec, context, uint64_type, array_type);
    const auto index_mapping = addMappedArrayWithLambda(
        index_dag, "index_", index_spec, context, uint64_type, array_type);
    const auto & query_lambda_array = *query_mapping.mapped_array;
    const auto & index_lambda_array = *index_mapping.mapped_array;

    const auto query_name = RPNBuilderTreeNode(&query_lambda_array, context).getColumnName();
    const auto index_name = RPNBuilderTreeNode(&index_lambda_array, context).getColumnName();
    String output_name;
    switch (output_name_mode)
    {
        case TestIndexOutputName::Canonical:
            output_name = index_name;
            break;
        case TestIndexOutputName::QueryNameCollision:
            /// Force the header lookup to succeed so negative controls reach structural comparison.
            output_name = query_name;
            break;
        case TestIndexOutputName::DistinctAlias:
            /// Force direct-IN's positional fallback while proving natural names remain unmodified.
            output_name = "__distinct_test_index_alias";
            break;
    }
    const auto & index_output = index_dag.addAlias(index_lambda_array, output_name);
    index_dag.getOutputs().push_back(&index_output);

    auto set_value = array_type->createColumn();
    set_value->insert(Field(Array{Field(UInt64(4))}));
    ColumnsWithTypeAndName set_columns{{std::move(set_value), array_type, "value"}};
    auto future_set = std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash{}, nullptr, std::move(set_columns), false, SizeLimits{});
    const auto & set_node = query_dag.addColumn(
        ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0), std::make_shared<DataTypeSet>(), "set");
    const auto & predicate = query_dag.addFunction(
        FunctionFactory::instance().get("in", context), {&query_lambda_array, &set_node}, "predicate");

    Block header(ColumnsWithTypeAndName{{array_type->createColumn(), array_type, output_name}});
    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
    return {
        .can_prune = !condition.alwaysUnknownOrTrue(),
        .query_is_column_function = isColumnFunctionLambda(*query_mapping.lambda),
        .index_is_column_function = isColumnFunctionLambda(*index_mapping.lambda),
        .query_is_function_capture = isFunctionCaptureLambda(*query_mapping.lambda),
        .index_is_function_capture = isFunctionCaptureLambda(*index_mapping.lambda),
        .query_allows_constant_folding
        = getNativeLambdaCapture(*query_mapping.lambda) && getNativeLambdaCapture(*query_mapping.lambda)->allow_constant_folding,
        .index_allows_constant_folding
        = getNativeLambdaCapture(*index_mapping.lambda) && getNativeLambdaCapture(*index_mapping.lambda)->allow_constant_folding,
        .canonical_names_equal = query_name == index_name,
        .header_name_matches_query = header.has(query_name),
        .header_name_matches_index = output_name == index_name,
        .query_has_compiled_runtime_action = hasCompiledFunction(getNativeLambdaActionsDAG(*query_mapping.lambda, false)),
        .index_has_compiled_runtime_action = hasCompiledFunction(getNativeLambdaActionsDAG(*index_mapping.lambda, false)),
        .query_has_uncompiled_semantic_snapshot = getNativeLambdaCapture(*query_mapping.lambda)
            && getNativeLambdaCapture(*query_mapping.lambda)->uncompiled_actions_dag
            && !hasCompiledFunction(getNativeLambdaActionsDAG(*query_mapping.lambda, true)),
        .index_has_uncompiled_semantic_snapshot = getNativeLambdaCapture(*index_mapping.lambda)
            && getNativeLambdaCapture(*index_mapping.lambda)->uncompiled_actions_dag
            && !hasCompiledFunction(getNativeLambdaActionsDAG(*index_mapping.lambda, true)),
    };
}

enum class TestNullableNothingCustomization
{
    None,
    WrapperName,
    WrapperSerialization,
    NestedName,
    NestedSerialization,
};

DataTypePtr makeTestNullableNothingType(TestNullableNothingCustomization customization)
{
    auto nested_type = std::make_shared<DataTypeNothing>();
    if (customization == TestNullableNothingCustomization::NestedName)
        nested_type->setCustomization(std::make_unique<DataTypeCustomDesc>(
            std::make_unique<DataTypeCustomFixedName>("CustomNothing")));
    else if (customization == TestNullableNothingCustomization::NestedSerialization)
        nested_type->setCustomization(std::make_unique<DataTypeCustomDesc>(nullptr, nested_type->getDefaultSerialization()));

    auto nullable_type = std::make_shared<DataTypeNullable>(nested_type);
    if (customization == TestNullableNothingCustomization::WrapperName)
        nullable_type->setCustomization(std::make_unique<DataTypeCustomDesc>(
            std::make_unique<DataTypeCustomFixedName>("CustomNullableNothing")));
    else if (customization == TestNullableNothingCustomization::WrapperSerialization)
        nullable_type->setCustomization(std::make_unique<DataTypeCustomDesc>(nullptr, nullable_type->getDefaultSerialization()));
    return nullable_type;
}

ColumnConstPtr makeTestNullableConstant(const DataTypePtr & type, bool null_map_is_set)
{
    if (type->onlyNull() && !null_map_is_set)
    {
        auto nested_column = ColumnNothing::create(1);
        auto null_map = ColumnUInt8::create();
        null_map->getData().push_back(UInt8{0});
        auto nullable_column = ColumnNullable::create(std::move(nested_column), std::move(null_map));
        return ColumnConst::create(std::move(nullable_column), 0);
    }

    return type->createColumnConst(0, Field(Null()));
}

struct TestNullableNothingInResult
{
    bool can_prune;
    bool query_is_null_function_reaches_leaf;
    bool index_is_null_function_reaches_leaf;
    bool query_has_native_nullable_nothing_column;
    bool index_has_native_nullable_nothing_column;
    bool query_null_map_is_set;
    bool index_null_map_is_set;
    bool query_leaf_has_custom_name;
    bool index_leaf_has_custom_name;
    bool query_leaf_has_custom_serialization;
    bool index_leaf_has_custom_serialization;
    bool query_nested_has_custom_name;
    bool index_nested_has_custom_name;
    bool query_nested_has_custom_serialization;
    bool index_nested_has_custom_serialization;
    bool query_is_nullable_string;
    bool index_is_nullable_string;
    bool canonical_names_equal;
    bool header_name_matches_query;
};

TestNullableNothingInResult canPruneNullableNothingIn(
    const DataTypePtr & query_null_type,
    bool query_null_map_is_set,
    const DataTypePtr & index_null_type,
    bool index_null_map_is_set)
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto array_type = std::make_shared<DataTypeArray>(uint64_type);

    const auto build_expression = [&](ActionsDAG & dag, const DataTypePtr & null_type, bool null_map_is_set, const String & prefix)
    {
        const auto & null_constant = dag.addColumn(
            makeTestNullableConstant(null_type, null_map_is_set), null_type, "null_constant", true);
        const auto & is_null = dag.addFunction(
            FunctionFactory::instance().get("isNull", context), {&null_constant}, "is_null_value");
        const auto & array_input = dag.addInput("a", array_type);
        const auto & expression = dag.addFunction(
            FunctionFactory::instance().get("if", context), {&is_null, &array_input, &array_input}, prefix + "result");
        return std::tuple{&expression, &is_null, &null_constant};
    };

    ActionsDAG query_dag;
    ActionsDAG index_dag;
    const auto [query_expression, query_is_null, query_null_constant] =
        build_expression(query_dag, query_null_type, query_null_map_is_set, "query_");
    const auto [index_expression, index_is_null, index_null_constant] =
        build_expression(index_dag, index_null_type, index_null_map_is_set, "index_");

    const auto query_name = RPNBuilderTreeNode(query_expression, context).getColumnName();
    const auto index_name = RPNBuilderTreeNode(index_expression, context).getColumnName();
    /// Make direct-IN reach the structural comparison even when the natural names differ.
    const auto & index_output = index_dag.addAlias(*index_expression, query_name);
    index_dag.getOutputs().push_back(&index_output);

    auto set_value = array_type->createColumn();
    set_value->insert(Field(Array{Field(UInt64(4))}));
    ColumnsWithTypeAndName set_columns{{std::move(set_value), array_type, "value"}};
    auto future_set = std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash{}, nullptr, std::move(set_columns), false, SizeLimits{});
    const auto & set_node = query_dag.addColumn(
        ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0), std::make_shared<DataTypeSet>(), "set");
    const auto & predicate = query_dag.addFunction(
        FunctionFactory::instance().get("in", context), {query_expression, &set_node}, "predicate");

    Block header(ColumnsWithTypeAndName{{array_type->createColumn(), array_type, query_name}});
    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});

    const auto inspect_leaf = [](const ActionsDAG::Node & root, const ActionsDAG::Node & is_null, const ActionsDAG::Node & leaf)
    {
        const bool reaches_leaf = root.type == ActionsDAG::ActionType::FUNCTION && root.children.size() == 3
            && root.children[0] == &is_null && is_null.type == ActionsDAG::ActionType::FUNCTION
            && is_null.children.size() == 1 && is_null.children[0] == &leaf
            && leaf.type == ActionsDAG::ActionType::COLUMN && leaf.children.empty() && leaf.column;

        const auto * nullable_type = typeid_cast<const DataTypeNullable *>(leaf.result_type.get());
        const auto * constant = typeid_cast<const ColumnConst *>(leaf.column.get());
        const auto * nullable_column = constant
            ? typeid_cast<const ColumnNullable *>(&constant->getDataColumn())
            : nullptr;
        const IColumn * nested_column = nullable_column ? &nullable_column->getNestedColumn() : nullptr;
        const auto null_map_column = nullable_column ? nullable_column->getNullMapColumnPtr() : ColumnPtr{};
        const IColumn * null_map = null_map_column.get();
        const auto * nested_nothing = nullable_type
            ? typeid_cast<const DataTypeNothing *>(nullable_type->getNestedType().get())
            : nullptr;
        const auto * nested_string = nullable_type
            ? typeid_cast<const DataTypeString *>(nullable_type->getNestedType().get())
            : nullptr;
        const bool native_nullable_nothing = nullable_type && nested_nothing && constant && nullable_column
            && typeid(*nullable_type) == typeid(DataTypeNullable)
            && typeid(*nested_nothing) == typeid(DataTypeNothing)
            && typeid(*nullable_column) == typeid(ColumnNullable)
            && nested_column && typeid(*nested_column) == typeid(ColumnNothing)
            && null_map && typeid(*null_map) == typeid(ColumnUInt8)
            && nullable_column->size() == 1 && nested_column->size() == 1
            && nullable_column->getNullMapData().size() == 1;

        return std::tuple{
            reaches_leaf,
            native_nullable_nothing,
            native_nullable_nothing && nullable_column->getNullMapData()[0] == ColumnNullable::IS_NULL_MASK,
            leaf.result_type && leaf.result_type->getCustomName(),
            leaf.result_type && leaf.result_type->getCustomSerialization(),
            nullable_type && nullable_type->getNestedType()->getCustomName(),
            nullable_type && nullable_type->getNestedType()->getCustomSerialization(),
            nullable_type && nested_string && typeid(*nullable_type) == typeid(DataTypeNullable)
                && typeid(*nested_string) == typeid(DataTypeString)};
    };

    const auto [query_reaches_leaf, query_native_column, query_has_null, query_custom_name, query_custom_serialization,
        query_nested_custom_name, query_nested_custom_serialization, query_is_nullable_string] =
        inspect_leaf(*query_expression, *query_is_null, *query_null_constant);
    const auto [index_reaches_leaf, index_native_column, index_has_null, index_custom_name, index_custom_serialization,
        index_nested_custom_name, index_nested_custom_serialization, index_is_nullable_string] =
        inspect_leaf(*index_expression, *index_is_null, *index_null_constant);

    return {
        .can_prune = !condition.alwaysUnknownOrTrue(),
        .query_is_null_function_reaches_leaf = query_reaches_leaf,
        .index_is_null_function_reaches_leaf = index_reaches_leaf,
        .query_has_native_nullable_nothing_column = query_native_column,
        .index_has_native_nullable_nothing_column = index_native_column,
        .query_null_map_is_set = query_has_null,
        .index_null_map_is_set = index_has_null,
        .query_leaf_has_custom_name = query_custom_name,
        .index_leaf_has_custom_name = index_custom_name,
        .query_leaf_has_custom_serialization = query_custom_serialization,
        .index_leaf_has_custom_serialization = index_custom_serialization,
        .query_nested_has_custom_name = query_nested_custom_name,
        .index_nested_has_custom_name = index_nested_custom_name,
        .query_nested_has_custom_serialization = query_nested_custom_serialization,
        .index_nested_has_custom_serialization = index_nested_custom_serialization,
        .query_is_nullable_string = query_is_nullable_string,
        .index_is_nullable_string = index_is_nullable_string,
        .canonical_names_equal = query_name == index_name,
        .header_name_matches_query = header.has(query_name),
    };
}

std::shared_ptr<FunctionCaptureOverloadResolver> makeIncrementLambda(
    const ContextPtr & context,
    const DataTypePtr & uint64_type,
    const String & argument_name,
    const String & output_name)
{
    ActionsDAG lambda_dag;
    const auto & argument = lambda_dag.addInput(argument_name, uint64_type);
    const auto & one = lambda_dag.addColumn(
        uint64_type->createColumnConst(0, Field(UInt64(1))), uint64_type, "one", true);
    const auto & output = lambda_dag.addFunction(
        FunctionFactory::instance().get("plus", context), {&argument, &one}, output_name);
    lambda_dag.getOutputs().push_back(&output);
    return std::make_shared<FunctionCaptureOverloadResolver>(
        std::move(lambda_dag),
        ExpressionActionsSettings(context),
        Names{},
        NamesAndTypesList{{argument_name, uint64_type}},
        uint64_type,
        output_name,
        /* allow_constant_folding= */ true);
}

struct TestNestedLambdaResult
{
    bool can_prune;
    bool mapped_array_is_supported_type;
    bool query_outer_is_function_capture;
    bool index_outer_is_function_capture;
    bool query_inner_is_function_capture;
    bool index_inner_is_function_capture;
};

TestNestedLambdaResult canPruneLambdaInBody(bool nested)
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto array_type = std::make_shared<DataTypeArray>(uint64_type);
    const auto array_of_arrays_type = std::make_shared<DataTypeArray>(array_type);

    const auto add_outer_map = [&](ActionsDAG & dag, const String & prefix)
    {
        ActionsDAG body_dag;
        const auto & argument = body_dag.addInput("v", array_type);
        const ActionsDAG::Node * output = nullptr;
        if (nested)
        {
            const auto inner_lambda = makeIncrementLambda(context, uint64_type, "x", prefix + "inner_result");
            const auto & inner = body_dag.addFunction(inner_lambda, {}, prefix + "inner_lambda");
            const auto & mapped = body_dag.addFunction(
                FunctionFactory::instance().get("arrayMap", context), {&inner, &argument}, prefix + "inner_mapped");
            output = &body_dag.addFunction(
                FunctionFactory::instance().get("arraySum", context), {&mapped}, prefix + "outer_result");
        }
        else
            output = &body_dag.addFunction(
                FunctionFactory::instance().get("arraySum", context), {&argument}, prefix + "outer_result");
        body_dag.getOutputs().push_back(output);

        const auto outer_lambda = std::make_shared<FunctionCaptureOverloadResolver>(
            std::move(body_dag),
            ExpressionActionsSettings(context),
            Names{},
            NamesAndTypesList{{"v", array_type}},
            uint64_type,
            prefix + "outer_result",
            /* allow_constant_folding= */ true);
        const auto & input = dag.addInput("a", array_of_arrays_type);
        const auto & outer = dag.addFunction(outer_lambda, {}, prefix + "outer_lambda");
        const auto & mapped_outer = dag.addFunction(
            FunctionFactory::instance().get("arrayMap", context), {&outer, &input}, prefix + "mapped_outer");
        return std::pair{&outer, &mapped_outer};
    };

    ActionsDAG query_dag;
    ActionsDAG index_dag;
    const auto [query_outer, query_mapped] = add_outer_map(query_dag, "query_");
    const auto [index_outer, index_mapped] = add_outer_map(index_dag, "index_");

    auto set_value = array_type->createColumn();
    set_value->insert(Field(Array{Field(UInt64(4))}));
    ColumnsWithTypeAndName set_columns{{std::move(set_value), array_type, "value"}};
    auto future_set = std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash{}, nullptr, std::move(set_columns), false, SizeLimits{});
    const auto & set_node = query_dag.addColumn(
        ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0), std::make_shared<DataTypeSet>(), "set");
    const auto & predicate = query_dag.addFunction(
        FunctionFactory::instance().get("in", context), {query_mapped, &set_node}, "predicate");

    const auto index_name = RPNBuilderTreeNode(index_mapped, context).getColumnName();
    const auto & index_output = index_dag.addAlias(*index_mapped, index_name);
    index_dag.getOutputs().push_back(&index_output);
    Block header(ColumnsWithTypeAndName{{array_type->createColumn(), array_type, index_name}});
    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});

    const auto * query_capture = typeid_cast<const FunctionCapture *>(query_outer->function_base.get());
    const auto * index_capture = typeid_cast<const FunctionCapture *>(index_outer->function_base.get());
    const auto find_inner_lambda = [](const FunctionCapture * outer) -> const ActionsDAG::Node *
    {
        if (!outer)
            return nullptr;
        for (const auto & node : outer->getAcionsDAG().getNodes())
            if (isFunctionCaptureLambda(node))
                return &node;
        return nullptr;
    };
    const auto * query_inner = find_inner_lambda(query_capture);
    const auto * index_inner = find_inner_lambda(index_capture);
    return {
        .can_prune = !condition.alwaysUnknownOrTrue(),
        .mapped_array_is_supported_type = query_mapped->result_type->equals(*array_type)
            && index_mapped->result_type->equals(*array_type),
        .query_outer_is_function_capture = query_capture != nullptr,
        .index_outer_is_function_capture = index_capture != nullptr,
        .query_inner_is_function_capture = query_inner && isFunctionCaptureLambda(*query_inner),
        .index_inner_is_function_capture = index_inner && isFunctionCaptureLambda(*index_inner),
    };
}

struct TestNestedCaptureResult
{
    bool can_prune;
    bool mapped_array_is_supported_type;
    bool query_outer_is_function_capture;
    bool index_outer_is_function_capture;
    bool query_inner_is_function_capture;
    bool index_inner_is_function_capture;
};

TestNestedCaptureResult canPruneLambdaInCaptureExpression(bool nested)
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto array_type = std::make_shared<DataTypeArray>(uint64_type);

    const auto add_outer_map = [&](ActionsDAG & dag, const String & prefix)
    {
        const auto & input = dag.addInput("a", array_type);
        const auto & capture_source = dag.addInput("capture_source", array_type);
        const ActionsDAG::Node * inner = nullptr;
        const ActionsDAG::Node * capture_expression = nullptr;
        if (nested)
        {
            const auto inner_lambda = makeIncrementLambda(context, uint64_type, "x", prefix + "inner_result");
            inner = &dag.addFunction(inner_lambda, {}, prefix + "inner_lambda");
            const auto & mapped_capture = dag.addFunction(
                FunctionFactory::instance().get("arrayMap", context), {inner, &capture_source}, prefix + "capture_map");
            capture_expression = &dag.addFunction(
                FunctionFactory::instance().get("arraySum", context), {&mapped_capture}, prefix + "capture_expression");
        }
        else
            capture_expression = &dag.addFunction(
                FunctionFactory::instance().get("arraySum", context), {&capture_source}, prefix + "capture_expression");

        ActionsDAG body_dag;
        const auto & argument = body_dag.addInput("v", uint64_type);
        const auto & captured = body_dag.addInput("captured", uint64_type);
        const auto & output = body_dag.addFunction(
            FunctionFactory::instance().get("plus", context), {&argument, &captured}, prefix + "outer_result");
        body_dag.getOutputs().push_back(&output);
        const auto outer_lambda = std::make_shared<FunctionCaptureOverloadResolver>(
            std::move(body_dag),
            ExpressionActionsSettings(context),
            Names{"captured"},
            NamesAndTypesList{{"v", uint64_type}},
            uint64_type,
            prefix + "outer_result",
            /* allow_constant_folding= */ true);
        const auto & outer = dag.addFunction(outer_lambda, {capture_expression}, prefix + "outer_lambda");
        const auto & mapped_outer = dag.addFunction(
            FunctionFactory::instance().get("arrayMap", context), {&outer, &input}, prefix + "mapped_outer");
        return std::tuple{&outer, &mapped_outer, inner};
    };

    ActionsDAG query_dag;
    ActionsDAG index_dag;
    const auto [query_outer, query_mapped, query_inner] = add_outer_map(query_dag, "query_");
    const auto [index_outer, index_mapped, index_inner] = add_outer_map(index_dag, "index_");

    auto set_value = array_type->createColumn();
    set_value->insert(Field(Array{Field(UInt64(4))}));
    ColumnsWithTypeAndName set_columns{{std::move(set_value), array_type, "value"}};
    auto future_set = std::make_shared<FutureSetFromTuple>(
        FutureSet::Hash{}, nullptr, std::move(set_columns), false, SizeLimits{});
    const auto & set_node = query_dag.addColumn(
        ColumnConst::create(ColumnSet::create(1, std::move(future_set)), 0), std::make_shared<DataTypeSet>(), "set");
    const auto & predicate = query_dag.addFunction(
        FunctionFactory::instance().get("in", context), {query_mapped, &set_node}, "predicate");

    const auto index_name = RPNBuilderTreeNode(index_mapped, context).getColumnName();
    const auto & index_output = index_dag.addAlias(*index_mapped, index_name);
    index_dag.getOutputs().push_back(&index_output);
    Block header(ColumnsWithTypeAndName{{array_type->createColumn(), array_type, index_name}});
    MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});

    return {
        .can_prune = !condition.alwaysUnknownOrTrue(),
        .mapped_array_is_supported_type = query_mapped->result_type->equals(*array_type)
            && index_mapped->result_type->equals(*array_type),
        .query_outer_is_function_capture = isFunctionCaptureLambda(*query_outer),
        .index_outer_is_function_capture = isFunctionCaptureLambda(*index_outer),
        .query_inner_is_function_capture = query_inner && isFunctionCaptureLambda(*query_inner),
        .index_inner_is_function_capture = index_inner && isFunctionCaptureLambda(*index_inner),
    };
}

}

TEST(MergeTreeIndexBloomFilter, DeclinesIndexExpressionDAGWhenContextScanBudgetIsExceeded)
{
    /// The constructor scans every indexed output before any predicate is emitted. This index has
    /// more outputs than the shared context-scan budget and must fail closed for either tuple order.
    EXPECT_FALSE(canPruneTupleArrayJoinInWithSharedBudget(true));
    EXPECT_FALSE(canPruneTupleArrayJoinInWithSharedBudget(false));
}

TEST(MergeTreeIndexBloomFilter, DeclinesSettingsDependentDateTimeCastsAcrossConsumersAndContainers)
{
    const auto string_type = std::make_shared<DataTypeString>();
    const auto datetime_type = std::make_shared<DataTypeDateTime>("UTC");
    const auto datetime64_type = std::make_shared<DataTypeDateTime64>(3, "UTC");
    const auto date_type = std::make_shared<DataTypeDate>();
    const auto date32_type = std::make_shared<DataTypeDate32>();
    const auto array_string_type = std::make_shared<DataTypeArray>(string_type);
    const auto array_datetime_type = std::make_shared<DataTypeArray>(datetime_type);
    const auto array_date32_type = std::make_shared<DataTypeArray>(date32_type);
    const auto array_uint64_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    const auto nullable_string_type = std::make_shared<DataTypeNullable>(string_type);
    const auto nullable_datetime_type = std::make_shared<DataTypeNullable>(datetime_type);
    const auto nested_array_string_type = std::make_shared<DataTypeArray>(nullable_string_type);
    const auto nested_array_datetime_type = std::make_shared<DataTypeArray>(nullable_datetime_type);
    const auto array_datetime64_type = std::make_shared<DataTypeArray>(datetime64_type);

    /// The helper gives the predicate a separate safe indexed column. A false result proves the
    /// constructor-wide guard declines all outputs, not only the CAST component.
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(string_type, datetime_type));
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(string_type, datetime64_type));
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(array_string_type, array_datetime_type));
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(nested_array_string_type, nested_array_datetime_type));
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(array_string_type, array_datetime64_type));
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(date_type, datetime_type));
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(date_type, datetime64_type));
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(date32_type, datetime_type));
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(date32_type, datetime64_type));
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(array_date32_type, array_datetime64_type));
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(array_uint64_type, array_datetime64_type));

    const std::vector<DataTypePtr> unsupported_timestamp_sources{
        std::make_shared<DataTypeInt8>(),
        std::make_shared<DataTypeInt16>(),
        std::make_shared<DataTypeInt32>(),
        std::make_shared<DataTypeInt64>(),
        std::make_shared<DataTypeInt128>(),
        std::make_shared<DataTypeInt256>(),
        std::make_shared<DataTypeUInt64>(),
        std::make_shared<DataTypeUInt128>(),
        std::make_shared<DataTypeUInt256>(),
        std::make_shared<DataTypeFloat32>(),
        std::make_shared<DataTypeDecimal32>(9, 2),
        datetime_type,
    };
    for (const auto & source_type : unsupported_timestamp_sources)
    {
        EXPECT_FALSE(canPruneSafeColumnWithIndexCast(source_type, datetime_type)) << source_type->getName();
        EXPECT_FALSE(canPruneSafeColumnWithIndexCast(source_type, datetime64_type)) << source_type->getName();
    }

    const std::vector<DataTypePtr> stable_timestamp_sources{
        std::make_shared<DataTypeUInt8>(),
        std::make_shared<DataTypeUInt16>(),
        std::make_shared<DataTypeUInt32>(),
    };
    for (const auto & source_type : stable_timestamp_sources)
    {
        EXPECT_TRUE(canPruneSafeColumnWithIndexCast(source_type, datetime_type)) << source_type->getName();
        EXPECT_TRUE(canPruneSafeColumnWithIndexCast(source_type, datetime64_type)) << source_type->getName();
    }
    EXPECT_TRUE(canPruneSafeColumnWithIndexCast(std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>()), array_datetime_type));

    /// Paired traversal must not confuse an unrelated String leaf and DateTime leaf in the same
    /// tuple. The direct predicate on x remains usable when the cast itself is context-independent.
    const auto tuple_source = std::make_shared<DataTypeTuple>(DataTypes{string_type, std::make_shared<DataTypeUInt32>()});
    const auto tuple_destination = std::make_shared<DataTypeTuple>(DataTypes{string_type, datetime_type});
    EXPECT_TRUE(canPruneSafeColumnWithIndexCast(tuple_source, tuple_destination));
    const auto unsafe_tuple_source = std::make_shared<DataTypeTuple>(DataTypes{string_type, std::make_shared<DataTypeUInt64>()});
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(unsafe_tuple_source, tuple_destination));

    /// This safe cast is a positive control for the constructor's normal pruning path.
    EXPECT_TRUE(canPruneSafeColumnWithIndexCast(std::make_shared<DataTypeUInt64>(), string_type));
}

TEST(MergeTreeIndexBloomFilter, AppliesClosedSafeLeafPolicyToNativeCasts)
{
    const auto string_type = std::make_shared<DataTypeString>();
    const auto fixed_string_type = std::make_shared<DataTypeFixedString>(8);
    const auto uint32_type = std::make_shared<DataTypeUInt32>();
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto float64_type = std::make_shared<DataTypeFloat64>();
    const auto datetime_type = std::make_shared<DataTypeDateTime>("UTC");
    const auto enum_type = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{{"known", 1}});
    const auto decimal_type = std::make_shared<DataTypeDecimal64>(18, 2);

    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(string_type, float64_type));
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(fixed_string_type, float64_type));
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(std::make_shared<DataTypeArray>(string_type), std::make_shared<DataTypeArray>(float64_type)));
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(datetime_type, string_type));
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(std::make_shared<DataTypeIPv4>(), string_type));
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(std::make_shared<DataTypeNullable>(string_type), string_type));

    const auto string_map = std::make_shared<DataTypeMap>(string_type, string_type);
    const auto float_map = std::make_shared<DataTypeMap>(string_type, float64_type);
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(string_map, float_map));

    EXPECT_TRUE(canPruneSafeColumnWithIndexCast(uint64_type, float64_type));
    EXPECT_TRUE(canPruneSafeColumnWithIndexCast(uint64_type, string_type));
    EXPECT_TRUE(canPruneSafeColumnWithIndexCast(decimal_type, string_type));
    EXPECT_TRUE(canPruneSafeColumnWithIndexCast(string_type, string_type));
    EXPECT_TRUE(canPruneSafeColumnWithIndexCast(fixed_string_type, fixed_string_type));
    EXPECT_TRUE(canPruneSafeColumnWithIndexCast(string_type, enum_type));
    EXPECT_TRUE(canPruneSafeColumnWithIndexCast(
        std::make_shared<DataTypeArray>(uint32_type), std::make_shared<DataTypeArray>(datetime_type)));
    EXPECT_TRUE(canPruneSafeColumnWithIndexCast(
        std::make_shared<DataTypeMap>(string_type, uint32_type), std::make_shared<DataTypeMap>(string_type, datetime_type)));

    const auto named_source = std::make_shared<DataTypeTuple>(DataTypes{float64_type, string_type}, Strings{"a", "b"});
    const auto named_destination = std::make_shared<DataTypeTuple>(DataTypes{float64_type, string_type}, Strings{"b", "a"});
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(named_source, named_destination));

    const auto same_named_destination = std::make_shared<DataTypeTuple>(DataTypes{float64_type, string_type}, Strings{"a", "b"});
    EXPECT_TRUE(canPruneSafeColumnWithIndexCast(named_source, same_named_destination));
}

TEST(MergeTreeIndexBloomFilter, DeclinesConstantFoldedFloatCastsWithoutConversionProvenance)
{
    const auto string_type = std::make_shared<DataTypeString>();
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto float64_type = std::make_shared<DataTypeFloat64>();

    /// removeUnusedActions turns folded CAST and plus nodes into COLUMN nodes while retaining
    /// function_base. Keep those nodes as direct outputs so the constructor sees that provenance.
    EXPECT_FALSE(canPruneWithFloatOutput(string_type, Field(String("0.1e-5")), true, false));
    EXPECT_FALSE(canPruneWithFloatOutput(string_type, Field(String("0.1e-5")), true, true));
    EXPECT_FALSE(canPruneWithFloatOutput(uint64_type, Field(UInt64(1)), true, false));

    /// A plain constant has no conversion provenance and must preserve the safe x=1 pruning path.
    EXPECT_TRUE(canPruneWithFloatOutput(float64_type, Field(Float64(1)), false, false));
}

TEST(MergeTreeIndexBloomFilter, PreservesFunctionProvenanceAcrossConstantFoldAndClone)
{
    tryRegisterFunctions();
    const auto context = getContext().context;
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto string_type = std::make_shared<DataTypeString>();
    const auto float64_type = std::make_shared<DataTypeFloat64>();
    const auto date_type = std::make_shared<DataTypeDate>();

    ActionsDAG query_dag;
    const auto & query_input = query_dag.addInput("x", uint64_type);
    const auto & query_value = query_dag.addColumn(uint64_type->createColumnConst(0, Field(UInt64(1))), uint64_type, "query_value", true);
    const auto & predicate
        = query_dag.addFunction(FunctionFactory::instance().get("equals", context), {&query_input, &query_value}, "predicate");

    const auto always_unknown = [&](const ActionsDAG & index_dag, const DataTypePtr & value_type, const String & value_name)
    {
        Block header(
            ColumnsWithTypeAndName{
                {uint64_type->createColumn(), uint64_type, "x"},
                {value_type->createColumn(), value_type, value_name},
            });
        MergeTreeIndexConditionBloomFilter condition(&predicate, context, header, index_dag, 4, {}, {});
        return condition.alwaysUnknownOrTrue();
    };

    const auto add_cast = [&](ActionsDAG & dag, const ActionsDAG::Node & source, const String & result_name) -> const ActionsDAG::Node &
    {
        const auto & target_type
            = dag.addColumn(string_type->createColumnConst(0, Field(String("Float64"))), string_type, "target_type", true);
        return dag.addFunction(FunctionFactory::instance().get("CAST", context), {&source, &target_type}, result_name);
    };

    ActionsDAG direct_cast_dag;
    const auto & direct_input = direct_cast_dag.addInput("x", uint64_type);
    const auto & direct_source
        = direct_cast_dag.addColumn(string_type->createColumnConst(0, Field(String("0.1e-5"))), string_type, "source", true);
    const auto & direct_cast = add_cast(direct_cast_dag, direct_source, "direct_cast");
    direct_cast_dag.getOutputs() = {&direct_input, &direct_cast};
    direct_cast_dag.removeUnusedActions();
    const auto * direct_output = direct_cast_dag.getOutputs()[1];
    EXPECT_EQ(direct_output->type, ActionsDAG::ActionType::COLUMN);
    EXPECT_NE(direct_output->function_base, nullptr);
    EXPECT_TRUE(direct_output->children.empty());
    EXPECT_TRUE(direct_output->has_folded_function) << "direct CAST";
    EXPECT_TRUE(always_unknown(direct_cast_dag, float64_type, direct_output->result_name));

    ActionsDAG nested_plus_dag;
    const auto & nested_input = nested_plus_dag.addInput("x", uint64_type);
    const auto & nested_source
        = nested_plus_dag.addColumn(string_type->createColumnConst(0, Field(String("0.1e-5"))), string_type, "source", true);
    const auto & nested_cast = add_cast(nested_plus_dag, nested_source, "nested_cast");
    const auto & zero = nested_plus_dag.addColumn(float64_type->createColumnConst(0, Field(Float64(0))), float64_type, "zero", true);
    const auto & nested_plus
        = nested_plus_dag.addFunction(FunctionFactory::instance().get("plus", context), {&nested_cast, &zero}, "nested_plus");
    nested_plus_dag.getOutputs() = {&nested_input, &nested_plus};
    nested_plus_dag.removeUnusedActions();
    const auto * nested_output = nested_plus_dag.getOutputs()[1];
    EXPECT_EQ(nested_output->type, ActionsDAG::ActionType::COLUMN);
    EXPECT_NE(nested_output->function_base, nullptr);
    EXPECT_TRUE(nested_output->children.empty());
    EXPECT_TRUE(nested_output->has_folded_function) << "nested plus";
    EXPECT_TRUE(always_unknown(nested_plus_dag, float64_type, nested_output->result_name));

    ActionsDAG alias_dag;
    const auto & alias_input = alias_dag.addInput("x", uint64_type);
    const auto & alias_source
        = alias_dag.addColumn(string_type->createColumnConst(0, Field(String("0.1e-5"))), string_type, "source", true);
    const auto & alias_cast = add_cast(alias_dag, alias_source, "alias_cast");
    const auto & first_alias = alias_dag.addAlias(alias_cast, "first_alias");
    const auto & second_alias = alias_dag.addAlias(first_alias, "second_alias");
    alias_dag.getOutputs() = {&alias_input, &second_alias};
    alias_dag.removeUnusedActions();
    const auto * alias_output = alias_dag.getOutputs()[1];
    EXPECT_EQ(alias_output->type, ActionsDAG::ActionType::COLUMN);
    EXPECT_NE(alias_output->column, nullptr);
    EXPECT_TRUE(alias_output->children.empty());
    EXPECT_TRUE(alias_output->has_folded_function) << "CAST hidden under alias chain";
    EXPECT_TRUE(always_unknown(alias_dag, float64_type, alias_output->result_name)) << "folded alias must not permit pruning";

    auto clone = alias_dag.clone();
    const auto * cloned_alias_output = clone.getOutputs()[1];
    EXPECT_TRUE(cloned_alias_output->has_folded_function) << "ordinary DAG clone";
    EXPECT_TRUE(always_unknown(clone, float64_type, cloned_alias_output->result_name));

    auto * mutable_alias_output = const_cast<ActionsDAG::Node *>(alias_output);
    const auto original_marker = mutable_alias_output->has_folded_function;
    const auto original_hash = mutable_alias_output->getHash();
    mutable_alias_output->has_folded_function = !original_marker;
    EXPECT_EQ(mutable_alias_output->getHash(), original_hash) << "transient provenance must not affect DAG identity";
    mutable_alias_output->has_folded_function = original_marker;

    ActionsDAG today_dag;
    const auto & today_input = today_dag.addInput("x", uint64_type);
    const auto & today_function = today_dag.addFunction(FunctionFactory::instance().get("today", context), {}, "today");
    const auto & today_alias = today_dag.addAlias(today_function, "today_alias");
    const auto & today_alias_chain = today_dag.addAlias(today_alias, "today_alias_chain");
    today_dag.getOutputs() = {&today_input, &today_alias_chain};
    today_dag.removeUnusedActions();
    const auto * today_output = today_dag.getOutputs()[1];
    EXPECT_EQ(today_output->type, ActionsDAG::ActionType::COLUMN);
    EXPECT_NE(today_output->column, nullptr);
    EXPECT_TRUE(today_output->children.empty());
    EXPECT_TRUE(today_output->has_folded_function) << "zero-argument today() hidden under aliases";
    EXPECT_TRUE(always_unknown(today_dag, date_type, today_output->result_name))
        << "zero-argument function provenance must not permit pruning";

    auto today_clone = today_dag.clone();
    const auto * cloned_today_output = today_clone.getOutputs()[1];
    EXPECT_TRUE(cloned_today_output->has_folded_function) << "zero-argument alias marker survives clone";
    EXPECT_TRUE(always_unknown(today_clone, date_type, cloned_today_output->result_name));

    ActionsDAG literal_alias_dag;
    const auto & literal_input = literal_alias_dag.addInput("x", uint64_type);
    const auto & literal
        = literal_alias_dag.addColumn(float64_type->createColumnConst(0, Field(Float64(1))), float64_type, "literal", true);
    const auto & literal_alias = literal_alias_dag.addAlias(literal, "literal_alias");
    literal_alias_dag.getOutputs() = {&literal_input, &literal_alias};
    literal_alias_dag.removeUnusedActions();
    const auto * literal_output = literal_alias_dag.getOutputs()[1];
    EXPECT_EQ(literal_output->type, ActionsDAG::ActionType::COLUMN);
    EXPECT_EQ(literal_output->function_base, nullptr);
    EXPECT_FALSE(literal_output->has_folded_function) << "an alias of a plain literal has no function provenance";
    EXPECT_FALSE(always_unknown(literal_alias_dag, float64_type, literal_output->result_name));
    EXPECT_TRUE(canPruneWithFloatOutput(float64_type, Field(Float64(1)), false, false));
}

TEST(MergeTreeIndexBloomFilter, DeclinesSettingsCapturingConversionsOutsideClosedWhitelist)
{
    const auto string_type = std::make_shared<DataTypeString>();
    const auto uint64_type = std::make_shared<DataTypeUInt64>();

    EXPECT_FALSE(canPruneSafeColumnWithNativeConversion("toFloat64", string_type));
    EXPECT_FALSE(canPruneSafeColumnWithNativeConversion("toFloat64OrZero", string_type));
    EXPECT_FALSE(canPruneSafeColumnWithNativeConversion("toFloat64OrNull", string_type));
    EXPECT_FALSE(canPruneSafeColumnWithNativeConversion("parseDateTimeBestEffort", string_type));
    EXPECT_FALSE(canPruneSafeColumnWithNativeConversion("toFloat64", uint64_type));

    EXPECT_TRUE(canPruneSafeColumnWithNativeConversion("toString", uint64_type));
    EXPECT_TRUE(canPruneSafeColumnWithNativeConversion("toUInt8", uint64_type));
}

TEST(MergeTreeIndexBloomFilter, DeclinesNativeSettingsCapturingConversionInFunctionCaptureBody)
{
    EXPECT_TRUE(constructorDeclinesNativeConversionInFunctionCaptureBody());
}

TEST(MergeTreeIndexBloomFilter, AuthenticatesNativeCastByExactImplementationInsteadOfFactoryHandle)
{
    EXPECT_FALSE(canPruneSafeColumnWithIndexCast(std::make_shared<DataTypeString>(), std::make_shared<DataTypeDateTime>("UTC"), true));
    EXPECT_TRUE(canPruneSafeColumnWithIndexCast(std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeString>(), true));
    EXPECT_FALSE(
        canPruneSafeColumnWithIndexCast(std::make_shared<DataTypeString>(), std::make_shared<DataTypeDateTime>("UTC"), false, "_CAST"));
    EXPECT_TRUE(canPruneSafeColumnWithIndexCast(std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeString>(), false, "_CAST"));
}

TEST(MergeTreeIndexBloomFilter, DeclinesCanonicalCastSpoofBeforeCallingVirtualHooks)
{
    EXPECT_TRUE(constructorDeclinesCanonicalCastSpoofWithoutCallbacks());
    EXPECT_TRUE(constructorDeclinesCanonicalCastSpoofWithoutCallbacks("_CAST"));
}

TEST(MergeTreeIndexBloomFilter, DeclinesCanonicalDateTimeConversionSpoofBeforeCallingVirtualHooks)
{
    EXPECT_TRUE(constructorDeclinesCanonicalDateTimeConversionSpoofWithoutCallbacks());
}

TEST(MergeTreeIndexBloomFilter, DeclinesNativeDateTimeConversionsForTextInputsAndDateSources)
{
    const std::vector<String> date_conversions{"toDateTime", "toDateTime32", "toDateTime64", "toUnixTimestamp"};
    const std::vector<DataTypePtr> text_types{
        std::make_shared<DataTypeString>(),
        std::make_shared<DataTypeFixedString>(19),
    };

    for (const auto & function_name : date_conversions)
        for (const auto & source_type : text_types)
            EXPECT_FALSE(canPruneSafeColumnWithDateTimeConversion(function_name, source_type))
                << function_name << " " << source_type->getName();

    const auto nullable_string = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    const auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    EXPECT_FALSE(canPruneSafeColumnWithDateTimeConversion("toDateTime", nullable_string));
    EXPECT_FALSE(canPruneSafeColumnWithDateTimeConversion("toDateTime", low_cardinality_string));
    EXPECT_FALSE(canPruneSafeColumnWithDateTimeConversion("toDateTime", std::make_shared<DataTypeDate>()));
    EXPECT_FALSE(canPruneSafeColumnWithDateTimeConversion("toDateTime64", std::make_shared<DataTypeDate32>()));
}

TEST(MergeTreeIndexBloomFilter, PreservesNativeDateTimeConversionPruningForIntegerSources)
{
    const std::vector<DataTypePtr> stable_integer_types{
        std::make_shared<DataTypeUInt8>(),
        std::make_shared<DataTypeUInt16>(),
        std::make_shared<DataTypeUInt32>(),
    };
    for (const auto & function_name : {"toDateTime", "toDateTime32", "toDateTime64", "toUnixTimestamp"})
        for (const auto & source_type : stable_integer_types)
            EXPECT_TRUE(canPruneSafeColumnWithDateTimeConversion(function_name, source_type))
                << function_name << " " << source_type->getName();

    /// The first argument controls parsing mode. A timezone parameter and DateTime64 scale must
    /// not turn a stable native integer conversion into a text-input hazard.
    const auto uint32_type = std::make_shared<DataTypeUInt32>();
    EXPECT_TRUE(canPruneSafeColumnWithDateTimeConversion("toDateTime", uint32_type, true));
    EXPECT_TRUE(canPruneSafeColumnWithDateTimeConversion("toDateTime32", uint32_type, true));
    EXPECT_TRUE(canPruneSafeColumnWithDateTimeConversion("toDateTime64", uint32_type, true));
    EXPECT_TRUE(canPruneSafeColumnWithDateTimeConversion("toDateTime", std::make_shared<DataTypeNullable>(uint32_type)));
    EXPECT_TRUE(canPruneSafeColumnWithDateTimeConversion("toDateTime", std::make_shared<DataTypeLowCardinality>(uint32_type)));
}

TEST(MergeTreeIndexBloomFilter, DeclinesWideAndSignedNativeDateTimeConversionInputs)
{
    const std::vector<String> date_conversions{"toDateTime", "toDateTime32", "toDateTime64", "toUnixTimestamp"};
    const std::vector<DataTypePtr> unsupported_integer_types{
        std::make_shared<DataTypeInt8>(),
        std::make_shared<DataTypeInt16>(),
        std::make_shared<DataTypeInt32>(),
        std::make_shared<DataTypeInt64>(),
        std::make_shared<DataTypeInt128>(),
        std::make_shared<DataTypeInt256>(),
        std::make_shared<DataTypeUInt64>(),
        std::make_shared<DataTypeUInt128>(),
        std::make_shared<DataTypeUInt256>(),
    };

    for (const auto & function_name : date_conversions)
        for (const auto & source_type : unsupported_integer_types)
            EXPECT_FALSE(canPruneSafeColumnWithDateTimeConversion(function_name, source_type))
                << function_name << " " << source_type->getName();
}

TEST(MergeTreeIndexBloomFilter, ScansNativeFunctionCaptureBodiesForContextDependentCasts)
{
    EXPECT_TRUE(constructorDeclinesDateTimeCastInFunctionCaptureBody());
    EXPECT_TRUE(constructorDeclinesDateTimeConversionInFunctionCaptureBody());
}

TEST(MergeTreeIndexBloomFilter, MatchesDecimalParametersForToStringIN)
{
    EXPECT_TRUE(canPruneDecimalToStringIn(
        std::make_shared<DataTypeDecimal64>(10, 2), std::make_shared<DataTypeDecimal64>(10, 2)));
    EXPECT_FALSE(canPruneDecimalToStringIn(
        std::make_shared<DataTypeDecimal64>(18, 2), std::make_shared<DataTypeDecimal64>(10, 2)));
    EXPECT_FALSE(canPruneDecimalToStringIn(
        std::make_shared<DataTypeDecimal64>(10, 3), std::make_shared<DataTypeDecimal64>(10, 2)));
}

TEST(MergeTreeIndexBloomFilter, MatchesToUInt8INOnlyForPrimitiveIntegerInputs)
{
    EXPECT_TRUE(canPruneUnaryFunctionIn("toUInt8", std::make_shared<DataTypeUInt64>()));
    EXPECT_FALSE(canPruneUnaryFunctionIn("toUInt8", std::make_shared<DataTypeFloat32>()));
}

TEST(MergeTreeIndexBloomFilter, DeclinesLambdaBodyWhenSharedExpressionMatchBudgetIsExceeded)
{
    EXPECT_TRUE(canPruneLambdaInWithFunctionCount(1));
    EXPECT_FALSE(canPruneLambdaInWithFunctionCount(4090));
}

TEST(MergeTreeIndexBloomFilter, MatchesNativeLambdaCarriersOnlyWhenEquivalent)
{
    const TestLambdaSpec equal_column_function{
        .carrier = TestLambdaCarrier::ColumnFunction,
        .capture_value = false,
        .captured_value = 0,
        .body_value = 1,
    };
    const TestLambdaSpec captured_column_function{
        .carrier = TestLambdaCarrier::ColumnFunction,
        .capture_value = true,
        .captured_value = 1,
        .body_value = 0,
    };

    const auto equal_result = canPruneLambdaInWithCarriers(equal_column_function, equal_column_function);
    EXPECT_TRUE(equal_result.query_is_column_function);
    EXPECT_TRUE(equal_result.index_is_column_function);
    EXPECT_TRUE(equal_result.can_prune);

    const auto equal_captured_result = canPruneLambdaInWithCarriers(captured_column_function, captured_column_function);
    EXPECT_TRUE(equal_captured_result.query_is_column_function);
    EXPECT_TRUE(equal_captured_result.index_is_column_function);
    EXPECT_TRUE(equal_captured_result.can_prune);

    /// A non-folded FunctionCapture uses a FUNCTION action while a constant-folded lambda uses a COLUMN
    /// containing ColumnFunction. Both carry the same typed literal capture and equivalent lambda body.
    const TestLambdaSpec captured_function_capture{
        .carrier = TestLambdaCarrier::FunctionCapture,
        .capture_value = true,
        .captured_value = 1,
        .body_value = 0,
        .allow_constant_folding = false,
        .capture_name = "offset_value",
        .body_output_name = "index_result",
    };
    auto captured_column_function_with_generated_name = captured_column_function;
    captured_column_function_with_generated_name.capture_name = "__table1.offset_value";
    captured_column_function_with_generated_name.body_output_name = "query_result";
    const auto mixed_result = canPruneLambdaInWithCarriers(
        captured_column_function_with_generated_name,
        captured_function_capture,
        TestIndexOutputName::DistinctAlias);
    EXPECT_TRUE(mixed_result.query_is_column_function);
    EXPECT_TRUE(mixed_result.index_is_function_capture);
    EXPECT_TRUE(mixed_result.query_allows_constant_folding);
    EXPECT_FALSE(mixed_result.index_allows_constant_folding);
    EXPECT_TRUE(mixed_result.canonical_names_equal);
    EXPECT_FALSE(mixed_result.header_name_matches_query);
    EXPECT_FALSE(mixed_result.header_name_matches_index);
    EXPECT_TRUE(mixed_result.can_prune);

    auto different_capture = captured_column_function;
    different_capture.captured_value = 2;
    const auto capture_result = canPruneLambdaInWithCarriers(
        captured_column_function, different_capture, TestIndexOutputName::QueryNameCollision);
    EXPECT_TRUE(capture_result.query_is_column_function);
    EXPECT_TRUE(capture_result.index_is_column_function);
    EXPECT_FALSE(capture_result.can_prune);

    auto different_body = equal_column_function;
    different_body.body_value = 2;
    const auto body_result = canPruneLambdaInWithCarriers(
        equal_column_function, different_body, TestIndexOutputName::QueryNameCollision);
    EXPECT_TRUE(body_result.query_is_column_function);
    EXPECT_TRUE(body_result.index_is_column_function);
    EXPECT_FALSE(body_result.can_prune);

    auto different_mixed_capture = captured_function_capture;
    different_mixed_capture.captured_value = 2;
    const auto mixed_capture_mismatch = canPruneLambdaInWithCarriers(
        captured_column_function_with_generated_name,
        different_mixed_capture,
        TestIndexOutputName::QueryNameCollision);
    EXPECT_TRUE(mixed_capture_mismatch.query_is_column_function);
    EXPECT_TRUE(mixed_capture_mismatch.index_is_function_capture);
    EXPECT_FALSE(mixed_capture_mismatch.can_prune);

    auto different_mixed_body = TestLambdaSpec{
        .carrier = TestLambdaCarrier::FunctionCapture,
        .body_value = 2,
        .body_output_name = "index_result",
    };
    const auto mixed_body_mismatch = canPruneLambdaInWithCarriers(
        equal_column_function, different_mixed_body, TestIndexOutputName::QueryNameCollision);
    EXPECT_TRUE(mixed_body_mismatch.query_is_column_function);
    EXPECT_TRUE(mixed_body_mismatch.index_is_function_capture);
    EXPECT_FALSE(mixed_body_mismatch.can_prune);

    auto same_body_capture_by_input = TestLambdaSpec{
        .carrier = TestLambdaCarrier::FunctionCapture,
        .capture_value = true,
        .capture_is_constant = false,
        .capture_name = "__table1.offset_value",
        .outer_capture_name = "offset_value",
        .body_output_name = "query_result",
    };
    auto same_body_capture_index = same_body_capture_by_input;
    same_body_capture_index.capture_name = "offset_value";
    same_body_capture_index.outer_capture_name = "offset_value";
    same_body_capture_index.body_output_name = "index_result";
    const auto same_input_capture = canPruneLambdaInWithCarriers(
        same_body_capture_by_input,
        same_body_capture_index,
        TestIndexOutputName::DistinctAlias);
    EXPECT_TRUE(same_input_capture.query_is_function_capture);
    EXPECT_TRUE(same_input_capture.index_is_function_capture);
    EXPECT_TRUE(same_input_capture.canonical_names_equal);
    EXPECT_FALSE(same_input_capture.header_name_matches_query);
    EXPECT_FALSE(same_input_capture.header_name_matches_index);
    EXPECT_TRUE(same_input_capture.can_prune);

    auto different_outer_capture = same_body_capture_index;
    different_outer_capture.outer_capture_name = "other_offset_value";
    const auto input_capture_mismatch = canPruneLambdaInWithCarriers(
        same_body_capture_by_input,
        different_outer_capture,
        TestIndexOutputName::QueryNameCollision);
    EXPECT_TRUE(input_capture_mismatch.query_is_function_capture);
    EXPECT_TRUE(input_capture_mismatch.index_is_function_capture);
    EXPECT_FALSE(input_capture_mismatch.can_prune);

    auto wrong_return_binding = equal_column_function;
    wrong_return_binding.override_return_name = true;
    wrong_return_binding.return_name_override = "different_result";
    const auto wrong_return = canPruneLambdaInWithCarriers(
        equal_column_function, wrong_return_binding, TestIndexOutputName::QueryNameCollision);
    EXPECT_TRUE(wrong_return.index_is_column_function);
    EXPECT_FALSE(wrong_return.can_prune);

    auto missing_return_binding = equal_column_function;
    missing_return_binding.override_return_name = true;
    missing_return_binding.return_name_override.clear();
    const auto missing_return = canPruneLambdaInWithCarriers(
        equal_column_function, missing_return_binding, TestIndexOutputName::QueryNameCollision);
    EXPECT_TRUE(missing_return.index_is_column_function);
    EXPECT_FALSE(missing_return.can_prune);

    /// The native FunctionExpression constructor accepts capture metadata whose name is absent from
    /// the body INPUT list. The structural matcher must reject it after seeing real ColumnFunction
    /// carriers, rather than accepting a partial slot map.
    auto missing_capture_input = captured_column_function;
    missing_capture_input.body_capture_input_name = "unbound_capture";
    const auto missing_capture = canPruneLambdaInWithCarriers(
        captured_column_function,
        missing_capture_input,
        TestIndexOutputName::QueryNameCollision);
    EXPECT_TRUE(missing_capture.query_is_column_function);
    EXPECT_TRUE(missing_capture.index_is_column_function);
    EXPECT_FALSE(missing_capture.can_prune);

    /// A capture named like the lambda argument creates an ambiguous own-side binding. Reuse the
    /// same native INPUT node so the malformed FunctionExpression reaches the matcher.
    auto duplicate_capture_argument_name = captured_column_function;
    duplicate_capture_argument_name.capture_name = "v";
    const auto ambiguous_capture = canPruneLambdaInWithCarriers(
        captured_column_function,
        duplicate_capture_argument_name,
        TestIndexOutputName::QueryNameCollision);
    EXPECT_TRUE(ambiguous_capture.query_is_column_function);
    EXPECT_TRUE(ambiguous_capture.index_is_column_function);
    EXPECT_FALSE(ambiguous_capture.can_prune);
}

#if USE_EMBEDDED_COMPILER
TEST(MergeTreeIndexBloomFilter, MatchesCompiledNativeLambdaCarriersUsingSemanticBodies)
{
    for (const auto carrier : {TestLambdaCarrier::ColumnFunction, TestLambdaCarrier::FunctionCapture})
    {
        TestLambdaSpec compiled_spec;
        compiled_spec.carrier = carrier;
        compiled_spec.allow_constant_folding = carrier == TestLambdaCarrier::ColumnFunction;
        compiled_spec.compile_expressions = true;
        compiled_spec.add_compilable_chain = true;

        const auto compiled_match = canPruneLambdaInWithCarriers(compiled_spec, compiled_spec);
        EXPECT_TRUE(compiled_match.can_prune);
        EXPECT_TRUE(compiled_match.query_has_compiled_runtime_action);
        EXPECT_TRUE(compiled_match.index_has_compiled_runtime_action);
        EXPECT_TRUE(compiled_match.query_has_uncompiled_semantic_snapshot);
        EXPECT_TRUE(compiled_match.index_has_uncompiled_semantic_snapshot);

        auto uncompiled_spec = compiled_spec;
        uncompiled_spec.compile_expressions = false;
        const auto uncompiled_match = canPruneLambdaInWithCarriers(uncompiled_spec, uncompiled_spec);
        EXPECT_TRUE(uncompiled_match.can_prune);
        EXPECT_EQ(compiled_match.can_prune, uncompiled_match.can_prune);
        EXPECT_FALSE(uncompiled_match.query_has_compiled_runtime_action);
        EXPECT_FALSE(uncompiled_match.index_has_compiled_runtime_action);

        TestLambdaSpec other_interpreted_carrier = uncompiled_spec;
        other_interpreted_carrier.carrier
            = carrier == TestLambdaCarrier::ColumnFunction ? TestLambdaCarrier::FunctionCapture : TestLambdaCarrier::ColumnFunction;
        other_interpreted_carrier.allow_constant_folding = other_interpreted_carrier.carrier == TestLambdaCarrier::ColumnFunction;

        const auto compiled_query
            = canPruneLambdaInWithCarriers(compiled_spec, other_interpreted_carrier, TestIndexOutputName::QueryNameCollision);
        EXPECT_TRUE(compiled_query.can_prune);
        EXPECT_TRUE(compiled_query.query_has_compiled_runtime_action);
        EXPECT_FALSE(compiled_query.index_has_compiled_runtime_action);

        const auto compiled_index
            = canPruneLambdaInWithCarriers(other_interpreted_carrier, compiled_spec, TestIndexOutputName::QueryNameCollision);
        EXPECT_TRUE(compiled_index.can_prune);
        EXPECT_FALSE(compiled_index.query_has_compiled_runtime_action);
        EXPECT_TRUE(compiled_index.index_has_compiled_runtime_action);

        auto mismatched_body = compiled_spec;
        mismatched_body.body_value = 2;
        const auto compiled_mismatch
            = canPruneLambdaInWithCarriers(compiled_spec, mismatched_body, TestIndexOutputName::QueryNameCollision);
        EXPECT_TRUE(compiled_mismatch.query_has_compiled_runtime_action);
        EXPECT_TRUE(compiled_mismatch.index_has_compiled_runtime_action);
        EXPECT_TRUE(compiled_mismatch.query_has_uncompiled_semantic_snapshot);
        EXPECT_TRUE(compiled_mismatch.index_has_uncompiled_semantic_snapshot);
        EXPECT_FALSE(compiled_mismatch.can_prune);
    }
}

TEST(MergeTreeIndexBloomFilter, ScansUncompiledBodyOfPartiallyCompiledLambdaForContextDependentConversion)
{
    const auto result = constructorDeclinesDateTimeConversionInPartiallyCompiledFunctionCaptureBody();
    EXPECT_TRUE(result.runtime_has_compiled_action);
    EXPECT_TRUE(result.has_uncompiled_semantic_snapshot);
    EXPECT_TRUE(result.condition_declines_pruning);
}

TEST(MergeTreeIndexBloomFilter, CompiledLambdaWithoutSemanticSnapshotDeclinesPruning)
{
    TestLambdaSpec opaque_compiled_spec;
    opaque_compiled_spec.carrier = TestLambdaCarrier::ColumnFunction;
    opaque_compiled_spec.allow_constant_folding = true;
    opaque_compiled_spec.compile_expressions = true;
    opaque_compiled_spec.add_compilable_chain = true;
    opaque_compiled_spec.bypass_lambda_snapshot = true;

    const auto result = canPruneLambdaInWithCarriers(opaque_compiled_spec, opaque_compiled_spec, TestIndexOutputName::QueryNameCollision);
    EXPECT_TRUE(result.query_has_compiled_runtime_action);
    EXPECT_TRUE(result.index_has_compiled_runtime_action);
    EXPECT_FALSE(result.query_has_uncompiled_semantic_snapshot);
    EXPECT_FALSE(result.index_has_uncompiled_semantic_snapshot);
    EXPECT_FALSE(result.can_prune);
}
#endif

TEST(MergeTreeIndexBloomFilter, MatchesOnlyEquivalentNativeNullableNothingConstants)
{
    const auto query_type = makeTestNullableNothingType(TestNullableNothingCustomization::None);
    const auto index_type = makeTestNullableNothingType(TestNullableNothingCustomization::None);
    ASSERT_NE(query_type.get(), index_type.get());

    const auto equal_nulls = canPruneNullableNothingIn(query_type, true, index_type, true);
    EXPECT_TRUE(equal_nulls.query_is_null_function_reaches_leaf);
    EXPECT_TRUE(equal_nulls.index_is_null_function_reaches_leaf);
    EXPECT_TRUE(equal_nulls.query_has_native_nullable_nothing_column);
    EXPECT_TRUE(equal_nulls.index_has_native_nullable_nothing_column);
    EXPECT_TRUE(equal_nulls.query_null_map_is_set);
    EXPECT_TRUE(equal_nulls.index_null_map_is_set);
    EXPECT_FALSE(equal_nulls.query_leaf_has_custom_name);
    EXPECT_FALSE(equal_nulls.index_leaf_has_custom_name);
    EXPECT_FALSE(equal_nulls.query_leaf_has_custom_serialization);
    EXPECT_FALSE(equal_nulls.index_leaf_has_custom_serialization);
    EXPECT_TRUE(equal_nulls.header_name_matches_query);
    EXPECT_TRUE(equal_nulls.can_prune);

    /// A native ColumnNullable with ColumnNothing storage but a zero null bit is not a valid
    /// Nullable(Nothing) NULL value and must not compare equal to an actual NULL constant.
    const auto non_null_payload = canPruneNullableNothingIn(query_type, false, index_type, true);
    EXPECT_TRUE(non_null_payload.query_is_null_function_reaches_leaf);
    EXPECT_TRUE(non_null_payload.index_is_null_function_reaches_leaf);
    EXPECT_TRUE(non_null_payload.query_has_native_nullable_nothing_column);
    EXPECT_TRUE(non_null_payload.index_has_native_nullable_nothing_column);
    EXPECT_FALSE(non_null_payload.query_null_map_is_set);
    EXPECT_TRUE(non_null_payload.index_null_map_is_set);
    EXPECT_FALSE(non_null_payload.can_prune);

    /// The same SQL NULL marker with a different nested type remains unsupported.
    const auto nullable_string_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    const auto wrong_nested_type = canPruneNullableNothingIn(query_type, true, nullable_string_type, true);
    EXPECT_TRUE(wrong_nested_type.query_is_null_function_reaches_leaf);
    EXPECT_TRUE(wrong_nested_type.index_is_null_function_reaches_leaf);
    EXPECT_TRUE(wrong_nested_type.query_has_native_nullable_nothing_column);
    EXPECT_TRUE(wrong_nested_type.index_is_nullable_string);
    EXPECT_FALSE(wrong_nested_type.can_prune);

    const TestNullableNothingCustomization customizations[]{
        TestNullableNothingCustomization::WrapperName,
        TestNullableNothingCustomization::WrapperSerialization,
        TestNullableNothingCustomization::NestedName,
        TestNullableNothingCustomization::NestedSerialization,
    };
    for (const auto customization : customizations)
    {
        const auto customized_type = makeTestNullableNothingType(customization);
        const auto customized = canPruneNullableNothingIn(query_type, true, customized_type, true);
        EXPECT_TRUE(customized.query_is_null_function_reaches_leaf);
        EXPECT_TRUE(customized.index_is_null_function_reaches_leaf);
        EXPECT_TRUE(customized.index_has_native_nullable_nothing_column);
        EXPECT_FALSE(customized.can_prune);

        switch (customization)
        {
            case TestNullableNothingCustomization::WrapperName:
                EXPECT_TRUE(customized.index_leaf_has_custom_name);
                break;
            case TestNullableNothingCustomization::WrapperSerialization:
                EXPECT_TRUE(customized.index_leaf_has_custom_serialization);
                break;
            case TestNullableNothingCustomization::NestedName:
                EXPECT_TRUE(customized.index_nested_has_custom_name);
                break;
            case TestNullableNothingCustomization::NestedSerialization:
                EXPECT_TRUE(customized.index_nested_has_custom_serialization);
                break;
            case TestNullableNothingCustomization::None:
                FAIL() << "The customized-type test list must not include the uncustomized type";
        }
    }
}

TEST(MergeTreeIndexBloomFilter, DeclinesNestedNativeLambdaInBodyAndCaptureExpressions)
{
    /// Existing native FunctionCapture controls show the outer Array(UInt64) carrier can match.
    auto same_function_capture = TestLambdaSpec{
        .carrier = TestLambdaCarrier::FunctionCapture,
        .body_value = 1,
    };
    const auto positive = canPruneLambdaInWithCarriers(same_function_capture, same_function_capture);
    EXPECT_TRUE(positive.query_is_function_capture);
    EXPECT_TRUE(positive.index_is_function_capture);
    EXPECT_TRUE(positive.can_prune);

    const auto body_without_nested_lambda = canPruneLambdaInBody(false);
    EXPECT_TRUE(body_without_nested_lambda.mapped_array_is_supported_type);
    EXPECT_TRUE(body_without_nested_lambda.query_outer_is_function_capture);
    EXPECT_TRUE(body_without_nested_lambda.index_outer_is_function_capture);
    EXPECT_FALSE(body_without_nested_lambda.query_inner_is_function_capture);
    EXPECT_FALSE(body_without_nested_lambda.index_inner_is_function_capture);
    EXPECT_TRUE(body_without_nested_lambda.can_prune);

    const auto nested_body = canPruneLambdaInBody(true);
    EXPECT_TRUE(nested_body.mapped_array_is_supported_type);
    EXPECT_TRUE(nested_body.query_outer_is_function_capture);
    EXPECT_TRUE(nested_body.index_outer_is_function_capture);
    EXPECT_TRUE(nested_body.query_inner_is_function_capture);
    EXPECT_TRUE(nested_body.index_inner_is_function_capture);
    EXPECT_FALSE(nested_body.can_prune);

    const auto capture_without_nested_lambda = canPruneLambdaInCaptureExpression(false);
    EXPECT_TRUE(capture_without_nested_lambda.mapped_array_is_supported_type);
    EXPECT_TRUE(capture_without_nested_lambda.query_outer_is_function_capture);
    EXPECT_TRUE(capture_without_nested_lambda.index_outer_is_function_capture);
    EXPECT_FALSE(capture_without_nested_lambda.query_inner_is_function_capture);
    EXPECT_FALSE(capture_without_nested_lambda.index_inner_is_function_capture);
    EXPECT_TRUE(capture_without_nested_lambda.can_prune);

    const auto nested_capture = canPruneLambdaInCaptureExpression(true);
    EXPECT_TRUE(nested_capture.mapped_array_is_supported_type);
    EXPECT_TRUE(nested_capture.query_outer_is_function_capture);
    EXPECT_TRUE(nested_capture.index_outer_is_function_capture);
    EXPECT_TRUE(nested_capture.query_inner_is_function_capture);
    EXPECT_TRUE(nested_capture.index_inner_is_function_capture);
    EXPECT_FALSE(nested_capture.can_prune);
}

TEST(MergeTreeIndexBloomFilter, MatchesOnlyExactOneLevelTupleConstantsInDirectIN)
{
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto string_type = std::make_shared<DataTypeString>();
    const auto tuple_type = std::make_shared<DataTypeTuple>(DataTypes{uint64_type, string_type});
    const Field tuple_value = Tuple{Field(UInt64(17)), Field(String("last"))};

    const auto equal = canPruneCityHash64WithConstantArgument(tuple_type, tuple_value, tuple_type, tuple_value);
    EXPECT_TRUE(equal.query_is_native_tuple_constant);
    EXPECT_TRUE(equal.index_is_native_tuple_constant);
    EXPECT_TRUE(equal.can_prune);

    const Field changed_last_value = Tuple{Field(UInt64(17)), Field(String("changed"))};
    EXPECT_FALSE(canPruneCityHash64WithConstantArgument(tuple_type, tuple_value, tuple_type, changed_last_value).can_prune);

    const auto uint8_type = std::make_shared<DataTypeUInt8>();
    const auto int8_type = std::make_shared<DataTypeInt8>();
    const auto uint8_tuple_type = std::make_shared<DataTypeTuple>(DataTypes{uint8_type, string_type});
    const auto int8_tuple_type = std::make_shared<DataTypeTuple>(DataTypes{int8_type, string_type});
    const Field uint8_tuple_value = Tuple{Field(UInt64(17)), Field(String("last"))};
    const Field int8_tuple_value = Tuple{Field(Int64(17)), Field(String("last"))};
    EXPECT_FALSE(canPruneCityHash64WithConstantArgument(uint8_tuple_type, uint8_tuple_value, int8_tuple_type, int8_tuple_value).can_prune);

    const auto nested_tuple_type = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeTuple>(DataTypes{uint64_type})});
    const Field nested_tuple_value = Tuple{Field(Tuple{Field(UInt64(17))})};
    EXPECT_FALSE(
        canPruneCityHash64WithConstantArgument(nested_tuple_type, nested_tuple_value, nested_tuple_type, nested_tuple_value).can_prune);

    const auto array_tuple_type = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeArray>(uint64_type)});
    const Field array_tuple_value = Tuple{Field(Array{Field(UInt64(17))})};
    EXPECT_FALSE(
        canPruneCityHash64WithConstantArgument(array_tuple_type, array_tuple_value, array_tuple_type, array_tuple_value).can_prune);

    const auto nullable_tuple_type = std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeNullable>(uint64_type)});
    const Field nullable_tuple_value = Tuple{Field(Null())};
    EXPECT_FALSE(
        canPruneCityHash64WithConstantArgument(nullable_tuple_type, nullable_tuple_value, nullable_tuple_type, nullable_tuple_value)
            .can_prune);

    auto custom_tuple_type = std::make_shared<DataTypeTuple>(DataTypes{uint64_type, string_type});
    custom_tuple_type->setCustomization(std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeCustomFixedName>("CustomTuple")));
    EXPECT_FALSE(canPruneCityHash64WithConstantArgument(custom_tuple_type, tuple_value, custom_tuple_type, tuple_value).can_prune);
}

TEST(MergeTreeIndexBloomFilter, SharesExpressionMatchBudgetAcrossTupleConstantLeaves)
{
    const auto string_type = std::make_shared<DataTypeString>();
    const String leaf_value(700, 'x');

    const auto two_leaf_tuple_type = std::make_shared<DataTypeTuple>(DataTypes{string_type, string_type});
    const Field two_leaf_tuple_value = Tuple{Field(leaf_value), Field(leaf_value)};
    const auto below_budget
        = canPruneCityHash64WithConstantArgument(two_leaf_tuple_type, two_leaf_tuple_value, two_leaf_tuple_type, two_leaf_tuple_value);
    EXPECT_TRUE(below_budget.query_is_native_tuple_constant);
    EXPECT_TRUE(below_budget.index_is_native_tuple_constant);
    EXPECT_TRUE(below_budget.can_prune);

    const auto three_leaf_tuple_type = std::make_shared<DataTypeTuple>(DataTypes{string_type, string_type, string_type});
    const Field three_leaf_tuple_value = Tuple{Field(leaf_value), Field(leaf_value), Field(leaf_value)};
    const auto above_budget = canPruneCityHash64WithConstantArgument(
        three_leaf_tuple_type, three_leaf_tuple_value, three_leaf_tuple_type, three_leaf_tuple_value);
    EXPECT_TRUE(above_budget.query_is_native_tuple_constant);
    EXPECT_TRUE(above_budget.index_is_native_tuple_constant);
    EXPECT_FALSE(above_budget.can_prune);
}

TEST(MergeTreeIndexBloomFilter, MatchesOnlyNativeEmptyArrayNothingConstants)
{
    const auto nothing_type = std::make_shared<DataTypeNothing>();
    const auto array_nothing_type = std::make_shared<DataTypeArray>(nothing_type);
    const Field empty_array = Array{};

    const auto native_equal = canPruneCityHash64WithConstantArgument(array_nothing_type, empty_array, array_nothing_type, empty_array);
    EXPECT_TRUE(native_equal.query_is_native_array_nothing);
    EXPECT_TRUE(native_equal.index_is_native_array_nothing);
    EXPECT_TRUE(native_equal.can_prune);

    const auto array_uint8_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>());
    const auto different_element_type
        = canPruneCityHash64WithConstantArgument(array_nothing_type, empty_array, array_uint8_type, empty_array);
    EXPECT_TRUE(different_element_type.query_is_native_array_nothing);
    EXPECT_FALSE(different_element_type.index_is_native_array_nothing);
    EXPECT_FALSE(different_element_type.can_prune);

    auto custom_array_nothing_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeNothing>());
    custom_array_nothing_type->setCustomization(
        std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeCustomFixedName>("CustomEmptyArray")));
    const auto custom_representation
        = canPruneCityHash64WithConstantArgument(array_nothing_type, empty_array, custom_array_nothing_type, empty_array);
    EXPECT_TRUE(custom_representation.query_is_native_array_nothing);
    EXPECT_FALSE(custom_representation.index_is_native_array_nothing);
    EXPECT_FALSE(custom_representation.can_prune);
}

TEST(MergeTreeIndexBloomFilter, DeclinesUnreviewedFormatDateTimeAndRetainsLowerPruning)
{
    const auto format_date_time = canPruneFormatDateTimeIn();
    EXPECT_TRUE(format_date_time.query_is_direct_format_date_time);
    EXPECT_TRUE(format_date_time.index_is_direct_format_date_time);
    EXPECT_TRUE(format_date_time.output_name_matches);
    EXPECT_FALSE(format_date_time.can_prune);

    EXPECT_TRUE(canPruneLowerStringIn());
}

TEST(MergeTreeIndexBloomFilter, MatchesAuditedPrimitiveIntegerUnaryDirectIN)
{
    const auto int64_type = std::make_shared<DataTypeInt64>();
    const auto uint64_type = std::make_shared<DataTypeUInt64>();

    EXPECT_TRUE(canPruneUnaryFunctionIn("abs", int64_type));
    EXPECT_TRUE(canPruneUnaryFunctionIn("negate", int64_type));
    EXPECT_TRUE(canPruneUnaryFunctionIn("sign", int64_type));
    EXPECT_TRUE(canPruneUnaryFunctionIn("bitNot", uint64_type));
}

TEST(MergeTreeIndexBloomFilter, DeclinesNonIntegerUnaryArithmeticDirectIN)
{
    const auto float64_type = std::make_shared<DataTypeFloat64>();
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const auto tuple_type = std::make_shared<DataTypeTuple>(DataTypes{uint64_type});

    EXPECT_FALSE(canPruneUnaryFunctionIn("abs", float64_type));
    EXPECT_FALSE(canPruneUnaryFunctionIn("negate", tuple_type));
    EXPECT_FALSE(canPruneUnaryFunctionIn("sign", float64_type));
    EXPECT_FALSE(canPruneUnaryFunctionIn("bitNot", std::make_shared<DataTypeString>()));
}
