#include "compileFunction.h"

#if USE_EMBEDDED_COMPILER

#    include <AggregateFunctions/IAggregateFunction.h>
#    include <Columns/ColumnNullable.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/Native.h>
#    include <Interpreters/JIT/CHJIT.h>
#    include <Common/ProfileEvents.h>
#    include <Common/Stopwatch.h>

#    include <llvm/IR/BasicBlock.h>
#    include <llvm/IR/Function.h>
#    include <llvm/IR/IRBuilder.h>

namespace
{

struct ColumnDataPlaceholder
{
    /// Pointer to column raw data
    llvm::Value * data_ptr = nullptr;
    /// Data type of column raw data element
    llvm::Type * data_element_type = nullptr;
    /// Pointer to null column raw data. Data type UInt8
    llvm::Value * null_data_ptr = nullptr;
};

}

namespace ProfileEvents
{
    extern const Event CompileFunction;
    extern const Event CompileExpressionsMicroseconds;
    extern const Event CompileExpressionsBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ColumnData getColumnData(const IColumn * column, size_t skip_rows)
{
    const bool is_const = isColumnConst(*column);

    if (is_const)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Input columns should not be constant");

    ColumnData result;

    if (const auto * nullable = typeid_cast<const ColumnNullable *>(column))
    {
        result.null_data = nullable->getNullMapColumn().getDataAt(skip_rows).data;
        column = &nullable->getNestedColumn();
    }
    /// skip null key data for one nullable key optimization
    result.data = column->getDataAt(skip_rows).data;

    return result;
}

static void compileFunction(llvm::Module & module, const IFunctionBase & function)
{
    const auto & function_argument_types = function.getArgumentTypes();

    llvm::IRBuilder<> b(module.getContext());
    auto * size_type = b.getIntNTy(sizeof(size_t) * 8);
    auto * data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy());
    auto * func_type = llvm::FunctionType::get(b.getVoidTy(), { size_type, data_type->getPointerTo() }, /*isVarArg=*/false);

    /// Create function in module

    auto * func = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, function.getName(), module);
    auto * args = func->args().begin();
    llvm::Value * rows_count_arg = args++;
    llvm::Value * columns_arg = args++;

    /// Initialize ColumnDataPlaceholder llvm representation of ColumnData

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", func);
    b.SetInsertPoint(entry);

    std::vector<ColumnDataPlaceholder> columns(function_argument_types.size() + 1);
    for (size_t i = 0; i <= function_argument_types.size(); ++i)
    {
        const auto & function_argument_type = i == function_argument_types.size() ? function.getResultType() : function_argument_types[i];
        auto * data = b.CreateLoad(data_type, b.CreateConstInBoundsGEP1_64(data_type, columns_arg, i));
        columns[i].data_ptr = b.CreateExtractValue(data, {0});
        columns[i].data_element_type = toNativeType(b, removeNullable(function_argument_type));
        columns[i].null_data_ptr = function_argument_type->isNullable() ? b.CreateExtractValue(data, {1}) : nullptr;
    }

    /// Initialize loop

    auto * end = llvm::BasicBlock::Create(b.getContext(), "end", func);
    auto * loop = llvm::BasicBlock::Create(b.getContext(), "loop", func);
    b.CreateCondBr(b.CreateICmpEQ(rows_count_arg, llvm::ConstantInt::get(size_type, 0)), end, loop);

    b.SetInsertPoint(loop);

    /// Loop

    auto * counter_phi = b.CreatePHI(rows_count_arg->getType(), 2);
    counter_phi->addIncoming(llvm::ConstantInt::get(size_type, 0), entry);

    /// Initialize column row values

    ValuesWithType arguments;
    arguments.reserve(function_argument_types.size());

    for (size_t i = 0; i < function_argument_types.size(); ++i)
    {
        auto & column = columns[i];
        const auto & type = function_argument_types[i];

        auto * column_data_ptr = column.data_ptr;
        auto * column_element_value = b.CreateLoad(column.data_element_type, b.CreateInBoundsGEP(column.data_element_type, column_data_ptr, counter_phi));

        if (!type->isNullable())
        {
            arguments.emplace_back(column_element_value, type);
            continue;
        }

        auto * column_is_null_element_value = b.CreateLoad(b.getInt8Ty(), b.CreateInBoundsGEP(b.getInt8Ty(), column.null_data_ptr, counter_phi));
        auto * is_null = b.CreateICmpNE(column_is_null_element_value, b.getInt8(0));
        auto * nullable_unitialized = llvm::Constant::getNullValue(toNullableType(b, column.data_element_type));
        auto * nullable_value = b.CreateInsertValue(b.CreateInsertValue(nullable_unitialized, column_element_value, {0}), is_null, {1});
        arguments.emplace_back(nullable_value, type);
    }

    /// Compile values for column rows and store compiled value in result column

    auto * result = function.compile(b, arguments);
    auto * result_column_element_ptr = b.CreateInBoundsGEP(columns.back().data_element_type, columns.back().data_ptr, counter_phi);

    if (columns.back().null_data_ptr)
    {
        b.CreateStore(b.CreateExtractValue(result, {0}), result_column_element_ptr);
        auto * result_column_is_null_element_ptr = b.CreateInBoundsGEP(b.getInt8Ty(), columns.back().null_data_ptr, counter_phi);
        auto * is_result_column_element_null = b.CreateSelect(b.CreateExtractValue(result, {1}), b.getInt8(1), b.getInt8(0));
        b.CreateStore(is_result_column_element_null, result_column_is_null_element_ptr);
    }
    else
    {
        b.CreateStore(result, result_column_element_ptr);
    }

    /// End of loop

    auto * current_block = b.GetInsertBlock();
    auto * incremeted_counter = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1));
    counter_phi->addIncoming(incremeted_counter, current_block);

    b.CreateCondBr(b.CreateICmpEQ(incremeted_counter, rows_count_arg), end, loop);

    b.SetInsertPoint(end);
    b.CreateRetVoid();
}

CompiledFunction compileFunction(CHJIT & jit, const IFunctionBase & function)
{
    Stopwatch watch;

    auto compiled_module = jit.compileModule([&](llvm::Module & module)
    {
        compileFunction(module, function);
    });

    ProfileEvents::increment(ProfileEvents::CompileExpressionsMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::CompileExpressionsBytes, compiled_module.size);
    ProfileEvents::increment(ProfileEvents::CompileFunction);

    auto compiled_function_ptr = reinterpret_cast<JITCompiledFunction>(compiled_module.function_name_to_symbol[function.getName()]);
    assert(compiled_function_ptr);

    CompiledFunction result_compiled_function
    {
        .compiled_function = compiled_function_ptr,
        .compiled_module = compiled_module
    };

    return result_compiled_function;
}

static void compileCreateAggregateStatesFunctions(llvm::Module & module, const std::vector<AggregateFunctionWithOffset> & functions, const std::string & name)
{
    auto & context = module.getContext();
    llvm::IRBuilder<> b(context);

    auto * aggregate_data_places_type = b.getInt8Ty()->getPointerTo();
    auto * create_aggregate_states_function_type = llvm::FunctionType::get(b.getVoidTy(), { aggregate_data_places_type }, false);
    auto * create_aggregate_states_function = llvm::Function::Create(create_aggregate_states_function_type, llvm::Function::ExternalLinkage, name, module);

    auto * arguments = create_aggregate_states_function->args().begin();
    llvm::Value * aggregate_data_place_arg = arguments++;

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", create_aggregate_states_function);
    b.SetInsertPoint(entry);

    for (const auto & function_to_compile : functions)
    {
        size_t aggregate_function_offset = function_to_compile.aggregate_data_offset;
        auto * aggregation_place_with_offset = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_place_arg, aggregate_function_offset);

        const auto * aggregate_function = function_to_compile.function;
        aggregate_function->compileCreate(b, aggregation_place_with_offset);
    }

    b.CreateRetVoid();
}

enum class AddIntoAggregateStatesPlacesArgumentType : uint8_t
{
    SinglePlace,
    MultiplePlaces,
};

static void compileAddIntoAggregateStatesFunctions(llvm::Module & module,
    const std::vector<AggregateFunctionWithOffset> & functions,
    const std::string & name,
    AddIntoAggregateStatesPlacesArgumentType places_argument_type)
{
    auto & context = module.getContext();
    llvm::IRBuilder<> b(context);

    auto * size_type = b.getIntNTy(sizeof(size_t) * 8);
    llvm::Type * places_type = nullptr;

    if (places_argument_type == AddIntoAggregateStatesPlacesArgumentType::MultiplePlaces)
        places_type = b.getInt8Ty()->getPointerTo()->getPointerTo();
    else
        places_type = b.getInt8Ty()->getPointerTo();

    auto * column_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy());

    auto * add_into_aggregate_states_func_declaration = llvm::FunctionType::get(b.getVoidTy(), { size_type, size_type, column_type->getPointerTo(), places_type }, false);
    auto * add_into_aggregate_states_func = llvm::Function::Create(add_into_aggregate_states_func_declaration, llvm::Function::ExternalLinkage, name, module);

    auto * arguments = add_into_aggregate_states_func->args().begin();
    llvm::Value * row_start_arg = arguments++;
    llvm::Value * row_end_arg = arguments++;
    llvm::Value * columns_arg = arguments++;
    llvm::Value * places_arg = arguments++;

    /// Initialize ColumnDataPlaceholder llvm representation of ColumnData

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", add_into_aggregate_states_func);
    b.SetInsertPoint(entry);

    std::vector<ColumnDataPlaceholder> columns;
    size_t previous_columns_size = 0;

    for (const auto & function : functions)
    {
        auto argument_types = function.function->getArgumentTypes();
        size_t function_arguments_size = argument_types.size();

        ColumnDataPlaceholder data_placeholder;

        for (size_t column_argument_index = 0; column_argument_index < function_arguments_size; ++column_argument_index)
        {
            const auto & argument_type = argument_types[column_argument_index];
            auto * data = b.CreateLoad(column_type, b.CreateConstInBoundsGEP1_64(column_type, columns_arg, previous_columns_size + column_argument_index));

            data_placeholder.data_ptr = b.CreateExtractValue(data, {0});
            data_placeholder.data_element_type = toNativeType(b, removeNullable(argument_type));

            if (argument_type->isNullable())
                data_placeholder.null_data_ptr = b.CreateExtractValue(data, {1});

            columns.emplace_back(data_placeholder);
        }

        previous_columns_size += function_arguments_size;
    }

    /// Initialize loop

    auto * end = llvm::BasicBlock::Create(b.getContext(), "end", add_into_aggregate_states_func);
    auto * loop = llvm::BasicBlock::Create(b.getContext(), "loop", add_into_aggregate_states_func);

    b.CreateCondBr(b.CreateICmpEQ(row_start_arg, row_end_arg), end, loop);

    b.SetInsertPoint(loop);

    /// Loop

    auto * counter_phi = b.CreatePHI(row_start_arg->getType(), 2);
    counter_phi->addIncoming(row_start_arg, entry);

    llvm::Value * aggregation_place = nullptr;

    if (places_argument_type == AddIntoAggregateStatesPlacesArgumentType::MultiplePlaces)
        aggregation_place = b.CreateLoad(b.getInt8Ty()->getPointerTo(), b.CreateGEP(b.getInt8Ty()->getPointerTo(), places_arg, counter_phi));
    else
        aggregation_place = places_arg;

    ValuesWithType function_arguments;
    previous_columns_size = 0;

    for (const auto & function : functions)
    {
        const auto & arguments_types = function.function->getArgumentTypes();
        size_t function_arguments_size = arguments_types.size();

        for (size_t column_argument_index = 0; column_argument_index < function_arguments_size; ++column_argument_index)
        {
            auto & column = columns[previous_columns_size + column_argument_index];
            const auto & argument_type = arguments_types[column_argument_index];

            auto * column_data_element = b.CreateLoad(column.data_element_type, b.CreateGEP(column.data_element_type, column.data_ptr, counter_phi));

            if (!argument_type->isNullable())
            {
                function_arguments.emplace_back(column_data_element, argument_type);
                continue;
            }

            auto * column_null_data_with_offset = b.CreateGEP(b.getInt8Ty(), column.null_data_ptr, counter_phi);
            auto * is_null = b.CreateICmpNE(b.CreateLoad(b.getInt8Ty(), column_null_data_with_offset), b.getInt8(0));
            auto * nullable_unitialized = llvm::Constant::getNullValue(toNullableType(b, column.data_element_type));
            auto * first_insert = b.CreateInsertValue(nullable_unitialized, column_data_element, {0});
            auto * nullable_value = b.CreateInsertValue(first_insert, is_null, {1});
            function_arguments.emplace_back(nullable_value, argument_type);
        }

        size_t aggregate_function_offset = function.aggregate_data_offset;
        auto * aggregation_place_with_offset = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregation_place, aggregate_function_offset);

        const auto * aggregate_function_ptr = function.function;
        aggregate_function_ptr->compileAdd(b, aggregation_place_with_offset, function_arguments);

        function_arguments.clear();

        previous_columns_size += function_arguments_size;
    }

    /// End of loop

    auto * current_block = b.GetInsertBlock();
    auto * value = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1));
    counter_phi->addIncoming(value, current_block);

    b.CreateCondBr(b.CreateICmpEQ(value, row_end_arg), end, loop);

    b.SetInsertPoint(end);
    b.CreateRetVoid();
}

static void compileMergeAggregatesStates(llvm::Module & module, const std::vector<AggregateFunctionWithOffset> & functions, const std::string & name)
{
    llvm::IRBuilder<> b(module.getContext());

    auto * aggregate_data_place_type = b.getInt8Ty()->getPointerTo();
    auto * aggregate_data_places_type = aggregate_data_place_type->getPointerTo();
    auto * size_type = b.getInt64Ty();

    auto * merge_aggregates_states_func_declaration
        = llvm::FunctionType::get(b.getVoidTy(), {aggregate_data_places_type, aggregate_data_places_type, size_type}, false);
    auto * merge_aggregates_states_func
        = llvm::Function::Create(merge_aggregates_states_func_declaration, llvm::Function::ExternalLinkage, name, module);

    auto * arguments = merge_aggregates_states_func->args().begin();
    llvm::Value * aggregate_data_places_dst_arg = arguments++;
    llvm::Value * aggregate_data_places_src_arg = arguments++;
    llvm::Value * aggregate_places_size_arg = arguments++;

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", merge_aggregates_states_func);
    b.SetInsertPoint(entry);

    /// Initialize loop

    auto * end = llvm::BasicBlock::Create(b.getContext(), "end", merge_aggregates_states_func);
    auto * loop = llvm::BasicBlock::Create(b.getContext(), "loop", merge_aggregates_states_func);
    b.CreateCondBr(b.CreateICmpEQ(aggregate_places_size_arg, llvm::ConstantInt::get(size_type, 0)), end, loop);

    b.SetInsertPoint(loop);

    /// Loop

    auto * counter_phi = b.CreatePHI(size_type, 2);
    counter_phi->addIncoming(llvm::ConstantInt::get(size_type, 0), entry);

    for (const auto & function_to_compile : functions)
    {
        auto * aggregate_data_place_dst = b.CreateLoad(aggregate_data_place_type,
            b.CreateInBoundsGEP(aggregate_data_place_type->getPointerTo(), aggregate_data_places_dst_arg, counter_phi));
        auto * aggregate_data_place_src = b.CreateLoad(aggregate_data_place_type,
            b.CreateInBoundsGEP(aggregate_data_place_type->getPointerTo(), aggregate_data_places_src_arg, counter_phi));

        size_t aggregate_function_offset = function_to_compile.aggregate_data_offset;

        auto * aggregate_data_place_merge_dst_with_offset = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_place_dst, aggregate_function_offset);
        auto * aggregate_data_place_merge_src_with_offset = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_place_src, aggregate_function_offset);

        const auto * aggregate_function_ptr = function_to_compile.function;
        aggregate_function_ptr->compileMerge(b, aggregate_data_place_merge_dst_with_offset, aggregate_data_place_merge_src_with_offset);
    }

    /// End of loop

    auto * current_block = b.GetInsertBlock();
    auto * incremeted_counter = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1));
    counter_phi->addIncoming(incremeted_counter, current_block);

    b.CreateCondBr(b.CreateICmpEQ(incremeted_counter, aggregate_places_size_arg), end, loop);

    b.SetInsertPoint(end);
    b.CreateRetVoid();
}

static void compileInsertAggregatesIntoResultColumns(llvm::Module & module, const std::vector<AggregateFunctionWithOffset> & functions, const std::string & name)
{
    auto & context = module.getContext();
    llvm::IRBuilder<> b(context);

    auto * size_type = b.getIntNTy(sizeof(size_t) * 8);

    auto * column_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy());
    auto * aggregate_data_places_type = b.getInt8Ty()->getPointerTo()->getPointerTo();
    auto * insert_aggregates_into_result_func_declaration = llvm::FunctionType::get(b.getVoidTy(), { size_type, size_type, column_type->getPointerTo(), aggregate_data_places_type }, false);
    auto * insert_aggregates_into_result_func = llvm::Function::Create(insert_aggregates_into_result_func_declaration, llvm::Function::ExternalLinkage, name, module);

    auto * arguments = insert_aggregates_into_result_func->args().begin();
    llvm::Value * row_start_arg = arguments++;
    llvm::Value * row_end_arg = arguments++;
    llvm::Value * columns_arg = arguments++;
    llvm::Value * aggregate_data_places_arg = arguments++;

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", insert_aggregates_into_result_func);
    b.SetInsertPoint(entry);

    std::vector<ColumnDataPlaceholder> columns(functions.size());
    for (size_t i = 0; i < functions.size(); ++i)
    {
        auto return_type = functions[i].function->getResultType();
        auto * data = b.CreateLoad(column_type, b.CreateConstInBoundsGEP1_64(column_type, columns_arg, i));

        auto * column_data_type = toNativeType(b, removeNullable(return_type));

        columns[i].data_ptr = b.CreateExtractValue(data, {0});
        columns[i].data_element_type = column_data_type;

        if (return_type->isNullable())
            columns[i].null_data_ptr = b.CreateExtractValue(data, {1});
    }

    auto * end = llvm::BasicBlock::Create(b.getContext(), "end", insert_aggregates_into_result_func);
    auto * loop = llvm::BasicBlock::Create(b.getContext(), "loop", insert_aggregates_into_result_func);

    b.CreateCondBr(b.CreateICmpEQ(row_start_arg, row_end_arg), end, loop);

    b.SetInsertPoint(loop);

    auto * counter_phi = b.CreatePHI(row_start_arg->getType(), 2);
    counter_phi->addIncoming(row_start_arg, entry);

    auto * aggregate_data_place = b.CreateLoad(b.getInt8Ty()->getPointerTo(), b.CreateGEP(b.getInt8Ty()->getPointerTo(), aggregate_data_places_arg, counter_phi));

    for (size_t i = 0; i < functions.size(); ++i)
    {
        size_t aggregate_function_offset = functions[i].aggregate_data_offset;
        auto * aggregation_place_with_offset = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_place, aggregate_function_offset);

        const auto * aggregate_function_ptr = functions[i].function;
        auto * final_value = aggregate_function_ptr->compileGetResult(b, aggregation_place_with_offset);

        auto * result_column_data_element = b.CreateGEP(columns[i].data_element_type, columns[i].data_ptr, counter_phi);
        if (columns[i].null_data_ptr)
        {
            b.CreateStore(b.CreateExtractValue(final_value, {0}), result_column_data_element);
            auto * result_column_is_null_element = b.CreateGEP(b.getInt8Ty(), columns[i].null_data_ptr, counter_phi);
            b.CreateStore(b.CreateSelect(b.CreateExtractValue(final_value, {1}), b.getInt8(1), b.getInt8(0)), result_column_is_null_element);
        }
        else
        {
            b.CreateStore(final_value, result_column_data_element);
        }
    }

    /// End of loop

    auto * current_block = b.GetInsertBlock();
    auto * incremented_counter = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1));
    counter_phi->addIncoming(incremented_counter, current_block);

    b.CreateCondBr(b.CreateICmpEQ(incremented_counter, row_end_arg), end, loop);

    b.SetInsertPoint(end);
    b.CreateRetVoid();
}

CompiledAggregateFunctions compileAggregateFunctions(CHJIT & jit, const std::vector<AggregateFunctionWithOffset> & functions, std::string functions_dump_name)
{
    Stopwatch watch;

    std::string create_aggregate_states_functions_name = functions_dump_name + "_create";
    std::string add_aggregate_states_functions_name = functions_dump_name + "_add";
    std::string add_aggregate_states_functions_name_single_place = functions_dump_name + "_add_single_place";
    std::string merge_aggregate_states_functions_name = functions_dump_name + "_merge";
    std::string insert_aggregate_states_functions_name = functions_dump_name + "_insert";

    auto compiled_module = jit.compileModule([&](llvm::Module & module)
    {
        compileCreateAggregateStatesFunctions(module, functions, create_aggregate_states_functions_name);
        compileAddIntoAggregateStatesFunctions(module, functions, add_aggregate_states_functions_name, AddIntoAggregateStatesPlacesArgumentType::MultiplePlaces);
        compileAddIntoAggregateStatesFunctions(module, functions, add_aggregate_states_functions_name_single_place, AddIntoAggregateStatesPlacesArgumentType::SinglePlace);
        compileMergeAggregatesStates(module, functions, merge_aggregate_states_functions_name);
        compileInsertAggregatesIntoResultColumns(module, functions, insert_aggregate_states_functions_name);
    });

    auto create_aggregate_states_function = reinterpret_cast<JITCreateAggregateStatesFunction>(compiled_module.function_name_to_symbol[create_aggregate_states_functions_name]);
    auto add_into_aggregate_states_function = reinterpret_cast<JITAddIntoAggregateStatesFunction>(compiled_module.function_name_to_symbol[add_aggregate_states_functions_name]);
    auto add_into_aggregate_states_function_single_place = reinterpret_cast<JITAddIntoAggregateStatesFunctionSinglePlace>(compiled_module.function_name_to_symbol[add_aggregate_states_functions_name_single_place]);
    auto merge_aggregate_states_function = reinterpret_cast<JITMergeAggregateStatesFunction>(compiled_module.function_name_to_symbol[merge_aggregate_states_functions_name]);
    auto insert_aggregate_states_function = reinterpret_cast<JITInsertAggregateStatesIntoColumnsFunction>(compiled_module.function_name_to_symbol[insert_aggregate_states_functions_name]);

    assert(create_aggregate_states_function);
    assert(add_into_aggregate_states_function);
    assert(add_into_aggregate_states_function_single_place);
    assert(merge_aggregate_states_function);
    assert(insert_aggregate_states_function);

    ProfileEvents::increment(ProfileEvents::CompileExpressionsMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::CompileExpressionsBytes, compiled_module.size);
    ProfileEvents::increment(ProfileEvents::CompileFunction);

    CompiledAggregateFunctions compiled_aggregate_functions
    {
        .create_aggregate_states_function = create_aggregate_states_function,
        .add_into_aggregate_states_function = add_into_aggregate_states_function,
        .add_into_aggregate_states_function_single_place = add_into_aggregate_states_function_single_place,
        .merge_aggregate_states_function = merge_aggregate_states_function,
        .insert_aggregates_into_columns_function = insert_aggregate_states_function,

        .functions_count = functions.size(),
        .compiled_module = std::move(compiled_module)
    };

    return compiled_aggregate_functions;
}

static void compileSortDescription(llvm::Module & module,
    SortDescription & description,
    const DataTypes & sort_description_types,
    const std::string & sort_description_dump)
{
    llvm::IRBuilder<> b(module.getContext());

    auto * size_type = b.getIntNTy(sizeof(size_t) * 8);

    auto * column_data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy());

    std::vector<llvm::Type *> function_argument_types = {size_type, size_type, column_data_type->getPointerTo(), column_data_type->getPointerTo()};
    auto * comparator_func_declaration = llvm::FunctionType::get(b.getInt8Ty(), function_argument_types, false);
    auto * comparator_func = llvm::Function::Create(comparator_func_declaration, llvm::Function::ExternalLinkage, sort_description_dump, module);

    auto * arguments = comparator_func->args().begin();
    llvm::Value * lhs_index_arg = arguments++;
    llvm::Value * rhs_index_arg = arguments++;
    llvm::Value * columns_lhs_arg = arguments++;
    llvm::Value * columns_rhs_arg = arguments++;

    size_t columns_size = description.size();

    std::vector<std::pair<llvm::BasicBlock *, llvm::Value *>> comparator_steps_and_results;
    for (size_t i = 0; i < columns_size; ++i)
    {
        auto * step = llvm::BasicBlock::Create(b.getContext(), "step_" + std::to_string(i), comparator_func);
        comparator_steps_and_results.emplace_back(step, nullptr);
    }

    auto * lhs_equals_rhs_result = llvm::ConstantInt::getSigned(b.getInt8Ty(), 0);

    auto * comparator_join = llvm::BasicBlock::Create(b.getContext(), "comparator_join", comparator_func);

    for (size_t i = 0; i < columns_size; ++i)
    {
        b.SetInsertPoint(comparator_steps_and_results[i].first);

        const auto & sort_description = description[i];
        const auto & column_type = sort_description_types[i];

        auto dummy_column = column_type->createColumn();

        auto * column_native_type = toNativeType(b, removeNullable(column_type));
        if (!column_native_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No native type for column type {}", column_type->getName());

        bool column_type_is_nullable = column_type->isNullable();

        auto * nullable_unitialized = llvm::Constant::getNullValue(toNullableType(b, column_native_type));

        auto * lhs_column = b.CreateLoad(column_data_type, b.CreateConstInBoundsGEP1_64(column_data_type, columns_lhs_arg, i));
        auto * lhs_column_data = b.CreateExtractValue(lhs_column, {0});
        auto * lhs_column_null_data = column_type_is_nullable ? b.CreateExtractValue(lhs_column, {1}) : nullptr;

        llvm::Value * lhs_column_element_offset = b.CreateInBoundsGEP(column_native_type, lhs_column_data, lhs_index_arg);
        llvm::Value * lhs_value = b.CreateLoad(column_native_type, lhs_column_element_offset);

        if (lhs_column_null_data)
        {
            auto * is_null_value_pointer = b.CreateInBoundsGEP(b.getInt8Ty(), lhs_column_null_data, lhs_index_arg);
            auto * is_null = b.CreateICmpNE(b.CreateLoad(b.getInt8Ty(), is_null_value_pointer), b.getInt8(0));
            auto * lhs_nullable_value = b.CreateInsertValue(b.CreateInsertValue(nullable_unitialized, lhs_value, {0}), is_null, {1});
            lhs_value = lhs_nullable_value;
        }

        auto * rhs_column = b.CreateLoad(column_data_type, b.CreateConstInBoundsGEP1_64(column_data_type, columns_rhs_arg, i));
        auto * rhs_column_data = b.CreateExtractValue(rhs_column, {0});
        auto * rhs_column_null_data = column_type_is_nullable ? b.CreateExtractValue(rhs_column, {1}) : nullptr;

        llvm::Value * rhs_column_element_offset = b.CreateInBoundsGEP(column_native_type, rhs_column_data, rhs_index_arg);
        llvm::Value * rhs_value = b.CreateLoad(column_native_type, rhs_column_element_offset);

        if (rhs_column_null_data)
        {
            auto * is_null_value_pointer = b.CreateInBoundsGEP(b.getInt8Ty(), rhs_column_null_data, rhs_index_arg);
            auto * is_null = b.CreateICmpNE(b.CreateLoad(b.getInt8Ty(), is_null_value_pointer), b.getInt8(0));
            auto * rhs_nullable_value = b.CreateInsertValue(b.CreateInsertValue(nullable_unitialized, rhs_value, {0}), is_null, {1});
            rhs_value = rhs_nullable_value;
        }

        llvm::Value * direction = llvm::ConstantInt::getSigned(b.getInt8Ty(), sort_description.direction);
        llvm::Value * nan_direction_hint = llvm::ConstantInt::getSigned(b.getInt8Ty(), sort_description.nulls_direction);
        llvm::Value * compare_result = dummy_column->compileComparator(b, lhs_value, rhs_value, nan_direction_hint);
        llvm::Value * result = b.CreateMul(direction, compare_result);

        comparator_steps_and_results[i].first = b.GetInsertBlock();
        comparator_steps_and_results[i].second = result;

        /** 1. If it is last condition block move to join block.
          * 2. If column elements are not equal move to join block.
          * 3. If column elements are equal move to next column condition.
          */
        if (i == columns_size - 1)
            b.CreateBr(comparator_join);
        else
            b.CreateCondBr(b.CreateICmpEQ(result, lhs_equals_rhs_result), comparator_steps_and_results[i + 1].first, comparator_join);
    }

    b.SetInsertPoint(comparator_join);

    /** Join results from all comparator steps.
      * Result of columns comparison equals to first compare block where lhs is not equal to lhs or last compare block.
      */
    auto * compare_result_phi = b.CreatePHI(b.getInt8Ty(), static_cast<unsigned>(comparator_steps_and_results.size()));

    for (const auto & [block, result_value] : comparator_steps_and_results)
        compare_result_phi->addIncoming(result_value, block);

    b.CreateRet(compare_result_phi);
}

CompiledSortDescriptionFunction compileSortDescription(
    CHJIT & jit,
    SortDescription & description,
    const DataTypes & sort_description_types,
    const std::string & sort_description_dump)
{
    Stopwatch watch;

    auto compiled_module = jit.compileModule([&](llvm::Module & module)
    {
        compileSortDescription(module, description, sort_description_types, sort_description_dump);
    });

    ProfileEvents::increment(ProfileEvents::CompileExpressionsMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::CompileExpressionsBytes, compiled_module.size);
    ProfileEvents::increment(ProfileEvents::CompileFunction);

    auto comparator_function = reinterpret_cast<JITSortDescriptionFunc>(compiled_module.function_name_to_symbol[sort_description_dump]);
    assert(comparator_function);

    CompiledSortDescriptionFunction compiled_sort_descriptor_function
    {
        .comparator_function = comparator_function,

        .compiled_module = std::move(compiled_module)
    };

    return compiled_sort_descriptor_function;
}

}

#endif
