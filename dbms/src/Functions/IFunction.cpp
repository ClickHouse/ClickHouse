#include <Common/config.h>
#include <Common/typeid_cast.h>
#include <Common/LRUCache.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Native.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnLowCardinality.h>
#include <Functions/FunctionHelpers.h>

#include <Functions/Helpers/ExecuteFunctionTransform.h>
#include <Functions/Helpers/RemoveConstantsTransform.h>
#include <Functions/Helpers/RemoveLowCardinalityTransform.h>
#include <Functions/Helpers/RemoveNullableTransform.h>
#include <Functions/Helpers/WrapConstantsTransform.h>
#include <Functions/Helpers/WrapLowCardinalityTransform.h>
#include <Functions/Helpers/WrapNullableTransform.h>
#include <Functions/Helpers/CreateConstantColumnTransform.h>
#include <Processors/Executors/SequentialTransformExecutor.h>

#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/WriteHelpers.h>
#include <ext/range.h>
#include <ext/collection_cast.h>
#include <cstdlib>
#include <memory>
#include <optional>

#if USE_EMBEDDED_COMPILER
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <llvm/IR/IRBuilder.h> // Y_IGNORE
#pragma GCC diagnostic pop
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}

static DataTypePtr recursiveRemoveLowCardinality(const DataTypePtr & type)
{
    if (!type)
        return type;

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        return std::make_shared<DataTypeArray>(recursiveRemoveLowCardinality(array_type->getNestedType()));

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        DataTypes elements = tuple_type->getElements();
        for (auto & element : elements)
            element = recursiveRemoveLowCardinality(element);

        if (tuple_type->haveExplicitNames())
            return std::make_shared<DataTypeTuple>(elements, tuple_type->getElementNames());
        else
            return std::make_shared<DataTypeTuple>(elements);
    }

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return low_cardinality_type->getDictionaryType();

    return type;
}

static ColumnPtr recursiveRemoveLowCardinality(const ColumnPtr & column)
{
    if (!column)
        return column;

    if (const auto * column_array = typeid_cast<const ColumnArray *>(column.get()))
        return ColumnArray::create(recursiveRemoveLowCardinality(column_array->getDataPtr()), column_array->getOffsetsPtr());

    if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
        return ColumnConst::create(recursiveRemoveLowCardinality(column_const->getDataColumnPtr()), column_const->size());

    if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(column.get()))
    {
        Columns columns = column_tuple->getColumns();
        for (auto & element : columns)
            element = recursiveRemoveLowCardinality(element);
        return ColumnTuple::create(columns);
    }

    if (const auto * column_low_cardinality = typeid_cast<const ColumnLowCardinality *>(column.get()))
        return column_low_cardinality->convertToFullColumn();

    return column;
}

namespace
{

struct NullPresence
{
    bool has_nullable = false;
    bool has_null_constant = false;
};

NullPresence getNullPresense(const Block & block, const ColumnNumbers & args)
{
    NullPresence res;

    for (const auto & arg : args)
    {
        const auto & elem = block.getByPosition(arg);

        if (!res.has_nullable)
            res.has_nullable = elem.type->isNullable();
        if (!res.has_null_constant)
            res.has_null_constant = elem.type->onlyNull();
    }

    return res;
}

NullPresence getNullPresense(const ColumnsWithTypeAndName & args)
{
    NullPresence res;

    for (const auto & elem : args)
    {
        if (!res.has_nullable)
            res.has_nullable = elem.type->isNullable();
        if (!res.has_null_constant)
            res.has_null_constant = elem.type->onlyNull();
    }

    return res;
}

bool allArgumentsAreConstants(const Block & block, const ColumnNumbers & args)
{
    for (auto arg : args)
        if (!block.getByPosition(arg).column->isColumnConst())
            return false;
    return true;
}

void checkArgumentsToRemainConstantsAreConstants(
    const Block & header,
    const ColumnNumbers & arguments,
    const ColumnNumbers & arguments_to_remain_constants,
    const String & function_name)
{
    for (auto arg_num : arguments_to_remain_constants)
        if (arg_num < arguments.size() && !header.getByPosition(arguments[arg_num]).column->isColumnConst())
            throw Exception("Argument at index " + toString(arg_num) + " for function " + function_name
                            + " must be constant", ErrorCodes::ILLEGAL_COLUMN);
}

}


SequentialTransformExecutorPtr IFunctionBase::execute(Block & block, const ColumnNumbers & arguments, size_t result)
{
    Processors processors;

    std::function<ProcessorPtr(const Block & header)> executeWithoutLowCardinality;

    auto executePreparedFunction = [&](const Block & header)
    {
        auto function = prepare(header, arguments, result);
        auto processor = std::make_shared<ExecuteFunctionTransform>(function, header, arguments, result);
        processors.emplace_back(processor);
    };

    auto executeRemoveNullable = [&](const Block & header) -> ProcessorPtr
    {
        if (arguments.empty() || !useDefaultImplementationForNulls())
            return nullptr;

        NullPresence null_presence = getNullPresense(block, arguments);

        if (null_presence.has_null_constant)
        {
            processors.emplace_back(std::make_shared<CreateConstantColumnTransform>(header, result, Null()));
            return processors.back();
        }

        if (null_presence.has_nullable)
        {
            auto remove_nullable = std::make_shared<RemoveNullableTransform>(header, arguments, result);
            auto & out_remove_nullable = remove_nullable->getOutputs().at(0);
            auto & out_null_maps = remove_nullable->getOutputs().at(1);

            processors.emplace_back(remove_nullable);

            auto exec_function_head = executeWithoutLowCardinality(out_remove_nullable.getHeader());
            auto & exec_function_tail = processors.back();
            auto & in_func_result = exec_function_head->getInputs().at(0);
            auto & out_func_result = exec_function_tail->getOutputs().at(0);

            Blocks wrap_nullable_headers = {output_port_func_result.getHeader(), out_null_maps.getHeader()};
            auto wrap_nullable = std::make_shared<WrapNullableTransform>(wrap_nullable_headers, arguments, result);
            auto & in_wrap_nullable = wrap_nullable->getInputs().at(0);
            auto & in_null_maps = wrap_nullable->getInputs().at(1);

            processors.emplace_back(wrap_nullable);

            connect(out_remove_nullable, in_func_result);
            connect(out_func_result, in_wrap_nullable);
            connect(out_null_maps, in_null_maps);

            return remove_nullable;
        }

        return nullptr;
    };

    auto executeRemoveConstants = [&](const Block & header) -> ProcessorPtr
    {
        ColumnNumbers arguments_to_remain_constants = getArgumentsThatAreAlwaysConstant();
        checkArgumentsToRemainConstantsAreConstants(header, arguments, arguments_to_remain_constants, getName());

        if (arguments.empty() || !useDefaultImplementationForConstants() || !allArgumentsAreConstants(block, args))
            return nullptr;

        auto remove_constants = std::make_shared<RemoveConstantsTransform>(
                header, arguments_to_remain_constants, arguments, result);
        auto & out_remove_constants = remove_constants->getOutputs().at(0);

        processors.emplace_back(remove_constants);

        auto exec_function_head = executeWithoutLowCardinality(out_remove_constants.getHeader());
        auto & exec_function_tail = processors.back();
        auto & in_func_result = exec_function_head->getInputs().at(0);
        auto & out_func_result = exec_function_tail->getOutputs().at(0);

        auto wrap_constants = std::make_shared<WrapConstantsTransform>(out_func_result.getHeader(), arguments, result);
        auto & in_wrap_constants = wrap_constants->getInputs().at(0);

        processors.emplace_back(wrap_constants);

        connect(out_remove_constants, in_func_result);
        connect(out_func_result, in_wrap_constants);

        return remove_constants;
    };

    executeWithoutLowCardinality = [&](const Block & header) -> ProcessorPtr
    {
        if (auto remove_constants = executeRemoveConstants(block))
            return remove_constants;

        if (auto remove_nullable = executeRemoveNullable(block))
            return remove_nullable;

        executePreparedFunction(block);
    };

    auto executeRemoveLowCardinality = [&](const Block & header) -> ProcessorPtr
    {
        if (!useDefaultImplementationForLowCardinalityColumns())
            return executeWithoutLowCardinality(block);

        auto remove_low_cardinality = std::make_shared<RemoveLowCardinalityTransform>(
                header, arguments, result, canBeExecutedOnDefaultArguments());
        auto & out_remove_low_cardinality = remove_low_cardinality->getOutputs().at(0);
        auto & out_positions = remove_low_cardinality->getOutputs().at(1);

        processors.emplace_back(remove_low_cardinality);

        auto exec_function_head = executeWithoutLowCardinality(out_remove_low_cardinality.getHeader());
        auto & exec_function_tail = processors.back();
        auto & in_func_result = exec_function_head->getInputs().at(0);
        auto & out_func_result = exec_function_tail->getOutputs().at(0);

        Blocks wrap_lc_headers = {out_func_result.ghetHeader(), out_positions.getHeader()};
        auto wrap_low_cardinality = std::make_shared<WrapLowCardinalityTransform>(wrap_lc_headers, arguments, result);
        auto & in_wrap_low_cardinality = wrap_low_cardinality->getInputs().at(0);
        auto & in_positions = wrap_low_cardinality->getInputs().at(1);

        processors.emplace_back(wrap_low_cardinality);

        connect(out_remove_low_cardinality, in_func_result);
        connect(out_func_result, in_wrap_low_cardinality);
        connect(out_positions, in_positions);

        return remove_low_cardinality;
    };

    auto head = executeRemoveLowCardinality(block);
    auto & tail = processors.back();
    auto & input = head->getInputs().at(0);
    auto & output = tail->getOutputs().at(0);

    return std::make_shared<SequentialTransformExecutor>(processors, input, output);
}

void FunctionBuilderImpl::checkNumberOfArguments(size_t number_of_arguments) const
{
    if (isVariadic())
        return;

    size_t expected_number_of_arguments = getNumberOfArguments();

    if (number_of_arguments != expected_number_of_arguments)
        throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                        + toString(number_of_arguments) + ", should be " + toString(expected_number_of_arguments),
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

DataTypePtr FunctionBuilderImpl::getReturnTypeWithoutLowCardinality(const ColumnsWithTypeAndName & arguments) const
{
    checkNumberOfArguments(arguments.size());

    if (!arguments.empty() && useDefaultImplementationForNulls())
    {
        NullPresence null_presence = getNullPresense(arguments);

        if (null_presence.has_null_constant)
        {
            return makeNullable(std::make_shared<DataTypeNothing>());
        }
        if (null_presence.has_nullable)
        {
            Block nested_block = createBlockWithNestedColumns(Block(arguments), ext::collection_cast<ColumnNumbers>(ext::range(0, arguments.size())));
            auto return_type = getReturnTypeImpl(ColumnsWithTypeAndName(nested_block.begin(), nested_block.end()));
            return makeNullable(return_type);

        }
    }

    return getReturnTypeImpl(arguments);
}

#if USE_EMBEDDED_COMPILER

static std::optional<DataTypes> removeNullables(const DataTypes & types)
{
    for (const auto & type : types)
    {
        if (!typeid_cast<const DataTypeNullable *>(type.get()))
            continue;
        DataTypes filtered;
        for (const auto & type : types)
            filtered.emplace_back(removeNullable(type));
        return filtered;
    }
    return {};
}

bool IFunction::isCompilable(const DataTypes & arguments) const
{
    if (useDefaultImplementationForNulls())
        if (auto denulled = removeNullables(arguments))
            return isCompilableImpl(*denulled);
    return isCompilableImpl(arguments);
}

llvm::Value * IFunction::compile(llvm::IRBuilderBase & builder, const DataTypes & arguments, ValuePlaceholders values) const
{
    if (useDefaultImplementationForNulls())
    {
        if (auto denulled = removeNullables(arguments))
        {
            /// FIXME: when only one column is nullable, this can actually be slower than the non-jitted version
            ///        because this involves copying the null map while `wrapInNullable` reuses it.
            auto & b = static_cast<llvm::IRBuilder<> &>(builder);
            auto * fail = llvm::BasicBlock::Create(b.GetInsertBlock()->getContext(), "", b.GetInsertBlock()->getParent());
            auto * join = llvm::BasicBlock::Create(b.GetInsertBlock()->getContext(), "", b.GetInsertBlock()->getParent());
            auto * zero = llvm::Constant::getNullValue(toNativeType(b, makeNullable(getReturnTypeImpl(*denulled))));
            for (size_t i = 0; i < arguments.size(); i++)
            {
                if (!arguments[i]->isNullable())
                    continue;
                /// Would be nice to evaluate all this lazily, but that'd change semantics: if only unevaluated
                /// arguments happen to contain NULLs, the return value would not be NULL, though it should be.
                auto * value = values[i]();
                auto * ok = llvm::BasicBlock::Create(b.GetInsertBlock()->getContext(), "", b.GetInsertBlock()->getParent());
                b.CreateCondBr(b.CreateExtractValue(value, {1}), fail, ok);
                b.SetInsertPoint(ok);
                values[i] = [value = b.CreateExtractValue(value, {0})]() { return value; };
            }
            auto * result = b.CreateInsertValue(zero, compileImpl(builder, *denulled, std::move(values)), {0});
            auto * result_block = b.GetInsertBlock();
            b.CreateBr(join);
            b.SetInsertPoint(fail);
            auto * null = b.CreateInsertValue(zero, b.getTrue(), {1});
            b.CreateBr(join);
            b.SetInsertPoint(join);
            auto * phi = b.CreatePHI(result->getType(), 2);
            phi->addIncoming(result, result_block);
            phi->addIncoming(null, fail);
            return phi;
        }
    }
    return compileImpl(builder, arguments, std::move(values));
}

#endif

DataTypePtr FunctionBuilderImpl::getReturnType(const ColumnsWithTypeAndName & arguments) const
{
    if (useDefaultImplementationForLowCardinalityColumns())
    {
        bool has_low_cardinality = false;
        size_t num_full_low_cardinality_columns = 0;
        size_t num_full_ordinary_columns = 0;

        ColumnsWithTypeAndName args_without_low_cardinality(arguments);

        for (ColumnWithTypeAndName & arg : args_without_low_cardinality)
        {
            bool is_const = arg.column && arg.column->isColumnConst();
            if (is_const)
                arg.column = static_cast<const ColumnConst &>(*arg.column).removeLowCardinality();

            if (auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(arg.type.get()))
            {
                arg.type = low_cardinality_type->getDictionaryType();
                has_low_cardinality = true;

                if (!is_const)
                    ++num_full_low_cardinality_columns;
            }
            else if (!is_const)
                ++num_full_ordinary_columns;
        }

        for (auto & arg : args_without_low_cardinality)
        {
            arg.column = recursiveRemoveLowCardinality(arg.column);
            arg.type = recursiveRemoveLowCardinality(arg.type);
        }

        if (canBeExecutedOnLowCardinalityDictionary() && has_low_cardinality
            && num_full_low_cardinality_columns <= 1 && num_full_ordinary_columns == 0)
            return std::make_shared<DataTypeLowCardinality>(getReturnTypeWithoutLowCardinality(args_without_low_cardinality));
        else
            return getReturnTypeWithoutLowCardinality(args_without_low_cardinality);
    }

    return getReturnTypeWithoutLowCardinality(arguments);
}
}
