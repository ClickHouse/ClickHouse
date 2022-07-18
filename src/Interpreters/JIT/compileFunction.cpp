#include "compileFunction.h"

#if USE_EMBEDDED_COMPILER

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>

#include <Common/Stopwatch.h>
#include <Common/ProfileEvents.h>
#include <DataTypes/Native.h>
#include <Interpreters/JIT/CHJIT.h>

namespace
{
    struct ColumnDataPlaceholder
    {
        llvm::Value * data_init = nullptr; /// first row
        llvm::Value * null_init = nullptr;
        llvm::PHINode * data = nullptr; /// current row
        llvm::PHINode * null = nullptr;
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

ColumnData getColumnData(const IColumn * column)
{
    ColumnData result;
    const bool is_const = isColumnConst(*column);

    if (is_const)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Input columns should not be constant");

    if (const auto * nullable = typeid_cast<const ColumnNullable *>(column))
    {
        result.null_data = nullable->getNullMapColumn().getRawData().data;
        column = & nullable->getNestedColumn();
    }

    result.data = column->getRawData().data;

    return result;
}

static void compileFunction(llvm::Module & module, const IFunctionBase & function)
{
    /** Algorithm is to create a loop that iterate over ColumnDataRowsSize size_t argument and
     * over ColumnData data and null_data. On each step compiled expression from function
     * will be executed over column data and null_data row.
     *
     * Example of preudocode of generated instructions of function with 1 input column.
     * In case of multiple columns more column_i_data, column_i_null_data is created.
     *
     * void compiled_function(size_t rows_count, ColumnData * columns)
     * {
     *     /// Initialize column values
     *
     *     Column0Type * column_0_data = static_cast<Column0Type *>(columns[0].data);
     *     UInt8 * column_0_null_data = static_cast<UInt8>(columns[0].null_data);
     *
     *     /// Initialize other input columns data with indexes < input_columns_count
     *
     *     ResultType * result_column_data = static_cast<ResultType *>(columns[input_columns_count].data);
     *     UInt8 * result_column_null_data = static_cast<UInt8 *>(columns[input_columns_count].data);
     *
     *     if (rows_count == 0)
     *         goto end;
     *
     *     /// Loop
     *
     *     size_t counter = 0;
     *
     *     loop:
     *
     *     /// Create column values tuple in case of non nullable type it is just column value
     *     /// In case of nullable type it is tuple of column value and is column row nullable
     *
     *     Column0Tuple column_0_value;
     *     if (Column0Type is nullable)
     *     {
     *         value[0] = column_0_data;
     *         value[1] = static_cast<bool>(column_1_null_data);
     *     }
     *     else
     *     {
     *         value[0] = column_0_data
     *     }
     *
     *     /// Initialize other input column values tuple with indexes < input_columns_count
     *     /// execute_compiled_expressions function takes input columns values and must return single result value
     *
     *     if (ResultType is nullable)
     *     {
     *         (ResultType, bool) result_column_value = execute_compiled_expressions(column_0_value, ...);
     *         *result_column_data = result_column_value[0];
     *         *result_column_null_data = static_cast<UInt8>(result_column_value[1]);
     *     }
     *     else
     *     {
     *         ResultType result_column_value = execute_compiled_expressions(column_0_value, ...);
     *         *result_column_data = result_column_value;
     *     }
     *
     *     /// Increment input and result column current row pointer
     *
     *     ++column_0_data;
     *     if (Column 0 type is nullable)
     *     {
     *         ++column_0_null_data;
     *     }
     *
     *     ++result_column_data;
     *     if  (ResultType  is nullable)
     *     {
     *         ++result_column_null_data;
     *     }
     *
     *     /// Increment loop counter and check if we should exit.
     *
     *     ++counter;
     *     if (counter == rows_count)
     *         goto end;
     *     else
     *         goto loop;
     *
     *   /// End
     *   end:
     *       return;
     * }
     */

    const auto & arg_types = function.getArgumentTypes();

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

    std::vector<ColumnDataPlaceholder> columns(arg_types.size() + 1);
    for (size_t i = 0; i <= arg_types.size(); ++i)
    {
        const auto & type = i == arg_types.size() ? function.getResultType() : arg_types[i];
        auto * data = b.CreateLoad(data_type, b.CreateConstInBoundsGEP1_64(data_type, columns_arg, i));
        columns[i].data_init = b.CreatePointerCast(b.CreateExtractValue(data, {0}), toNativeType(b, removeNullable(type))->getPointerTo());
        columns[i].null_init = type->isNullable() ? b.CreateExtractValue(data, {1}) : nullptr;
    }

    /// Initialize loop

    auto * end = llvm::BasicBlock::Create(b.getContext(), "end", func);
    auto * loop = llvm::BasicBlock::Create(b.getContext(), "loop", func);
    b.CreateCondBr(b.CreateICmpEQ(rows_count_arg, llvm::ConstantInt::get(size_type, 0)), end, loop);

    b.SetInsertPoint(loop);

    auto * counter_phi = b.CreatePHI(rows_count_arg->getType(), 2);
    counter_phi->addIncoming(llvm::ConstantInt::get(size_type, 0), entry);

    for (auto & col : columns)
    {
        col.data = b.CreatePHI(col.data_init->getType(), 2);
        col.data->addIncoming(col.data_init, entry);
        if (col.null_init)
        {
            col.null = b.CreatePHI(col.null_init->getType(), 2);
            col.null->addIncoming(col.null_init, entry);
        }
    }

    /// Initialize column row values

    Values arguments;
    arguments.reserve(arg_types.size());

    for (size_t i = 0; i < arg_types.size(); ++i)
    {
        auto & column = columns[i];
        const auto & type = arg_types[i];

        auto * value = b.CreateLoad(toNativeType(b, removeNullable(type)), column.data);
        if (!type->isNullable())
        {
            arguments.emplace_back(value);
            continue;
        }

        auto * is_null = b.CreateICmpNE(b.CreateLoad(b.getInt8Ty(), column.null), b.getInt8(0));
        auto * nullable_unitilized = llvm::Constant::getNullValue(toNativeType(b, type));
        auto * nullable_value = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, value, {0}), is_null, {1});
        arguments.emplace_back(nullable_value);
    }

    /// Compile values for column rows and store compiled value in result column

    auto * result = function.compile(b, std::move(arguments));
    if (columns.back().null)
    {
        b.CreateStore(b.CreateExtractValue(result, {0}), columns.back().data);
        b.CreateStore(b.CreateSelect(b.CreateExtractValue(result, {1}), b.getInt8(1), b.getInt8(0)), columns.back().null);
    }
    else
    {
        b.CreateStore(result, columns.back().data);
    }

    /// End of loop

    auto * cur_block = b.GetInsertBlock();
    for (auto & col : columns)
    {
        col.data->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, col.data, 1), cur_block);
        if (col.null)
            col.null->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, col.null, 1), cur_block);
    }

    auto * value = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1));
    counter_phi->addIncoming(value, cur_block);

    b.CreateCondBr(b.CreateICmpEQ(value, rows_count_arg), end, loop);

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

    std::vector<ColumnDataPlaceholder> columns(functions.size());
    for (const auto & function_to_compile : functions)
    {
        size_t aggregate_function_offset = function_to_compile.aggregate_data_offset;
        const auto * aggregate_function = function_to_compile.function;
        auto * aggregation_place_with_offset = b.CreateConstInBoundsGEP1_64(nullptr, aggregate_data_place_arg, aggregate_function_offset);
        aggregate_function->compileCreate(b, aggregation_place_with_offset);
    }

    b.CreateRetVoid();
}

static void compileAddIntoAggregateStatesFunctions(llvm::Module & module, const std::vector<AggregateFunctionWithOffset> & functions, const std::string & name)
{
    auto & context = module.getContext();
    llvm::IRBuilder<> b(context);

    auto * size_type = b.getIntNTy(sizeof(size_t) * 8);
    auto * places_type = b.getInt8Ty()->getPointerTo()->getPointerTo();
    auto * column_data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy());

    auto * aggregate_loop_func_declaration = llvm::FunctionType::get(b.getVoidTy(), { size_type, column_data_type->getPointerTo(), places_type }, false);
    auto * aggregate_loop_func_definition = llvm::Function::Create(aggregate_loop_func_declaration, llvm::Function::ExternalLinkage, name, module);

    auto * arguments = aggregate_loop_func_definition->args().begin();
    llvm::Value * rows_count_arg = arguments++;
    llvm::Value * columns_arg = arguments++;
    llvm::Value * places_arg = arguments++;

    /// Initialize ColumnDataPlaceholder llvm representation of ColumnData

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", aggregate_loop_func_definition);
    b.SetInsertPoint(entry);

    std::vector<ColumnDataPlaceholder> columns;
    size_t previous_columns_size = 0;

    for (const auto & function : functions)
    {
        auto argument_types = function.function->getArgumentTypes();

        ColumnDataPlaceholder data_placeholder;

        size_t function_arguments_size = argument_types.size();

        for (size_t column_argument_index = 0; column_argument_index < function_arguments_size; ++column_argument_index)
        {
            const auto & argument_type = argument_types[column_argument_index];
            auto * data = b.CreateLoad(column_data_type, b.CreateConstInBoundsGEP1_64(column_data_type, columns_arg, previous_columns_size + column_argument_index));
            data_placeholder.data_init = b.CreatePointerCast(b.CreateExtractValue(data, {0}), toNativeType(b, removeNullable(argument_type))->getPointerTo());
            data_placeholder.null_init = argument_type->isNullable() ? b.CreateExtractValue(data, {1}) : nullptr;
            columns.emplace_back(data_placeholder);
        }

        previous_columns_size += function_arguments_size;
    }

    /// Initialize loop

    auto * end = llvm::BasicBlock::Create(b.getContext(), "end", aggregate_loop_func_definition);
    auto * loop = llvm::BasicBlock::Create(b.getContext(), "loop", aggregate_loop_func_definition);

    b.CreateCondBr(b.CreateICmpEQ(rows_count_arg, llvm::ConstantInt::get(size_type, 0)), end, loop);

    b.SetInsertPoint(loop);

    auto * counter_phi = b.CreatePHI(rows_count_arg->getType(), 2);
    counter_phi->addIncoming(llvm::ConstantInt::get(size_type, 0), entry);

    auto * places_phi = b.CreatePHI(places_arg->getType(), 2);
    places_phi->addIncoming(places_arg, entry);

    for (auto & col : columns)
    {
        col.data = b.CreatePHI(col.data_init->getType(), 2);
        col.data->addIncoming(col.data_init, entry);

        if (col.null_init)
        {
            col.null = b.CreatePHI(col.null_init->getType(), 2);
            col.null->addIncoming(col.null_init, entry);
        }
    }

    auto * aggregation_place = b.CreateLoad(b.getInt8Ty()->getPointerTo(), places_phi);

    previous_columns_size = 0;
    for (const auto & function : functions)
    {
        size_t aggregate_function_offset = function.aggregate_data_offset;
        const auto * aggregate_function_ptr = function.function;

        auto arguments_types = function.function->getArgumentTypes();
        std::vector<llvm::Value *> arguments_values;

        size_t function_arguments_size = arguments_types.size();
        arguments_values.resize(function_arguments_size);

        for (size_t column_argument_index = 0; column_argument_index < function_arguments_size; ++column_argument_index)
        {
            auto * column_argument_data = columns[previous_columns_size + column_argument_index].data;
            auto * column_argument_null_data = columns[previous_columns_size + column_argument_index].null;

            auto & argument_type = arguments_types[column_argument_index];

            auto * value = b.CreateLoad(toNativeType(b, removeNullable(argument_type)), column_argument_data);
            if (!argument_type->isNullable())
            {
                arguments_values[column_argument_index] = value;
                continue;
            }

            auto * is_null = b.CreateICmpNE(b.CreateLoad(b.getInt8Ty(), column_argument_null_data), b.getInt8(0));
            auto * nullable_unitilized = llvm::Constant::getNullValue(toNativeType(b, argument_type));
            auto * nullable_value = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, value, {0}), is_null, {1});
            arguments_values[column_argument_index] = nullable_value;
        }

        auto * aggregation_place_with_offset = b.CreateConstInBoundsGEP1_64(nullptr, aggregation_place, aggregate_function_offset);
        aggregate_function_ptr->compileAdd(b, aggregation_place_with_offset, arguments_types, arguments_values);

        previous_columns_size += function_arguments_size;
    }

    /// End of loop

    auto * cur_block = b.GetInsertBlock();
    for (auto & col : columns)
    {
        col.data->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, col.data, 1), cur_block);

        if (col.null)
            col.null->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, col.null, 1), cur_block);
    }

    places_phi->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, places_phi, 1), cur_block);

    auto * value = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1));
    counter_phi->addIncoming(value, cur_block);

    b.CreateCondBr(b.CreateICmpEQ(value, rows_count_arg), end, loop);

    b.SetInsertPoint(end);
    b.CreateRetVoid();
}

static void compileAddIntoAggregateStatesFunctionsSinglePlace(llvm::Module & module, const std::vector<AggregateFunctionWithOffset> & functions, const std::string & name)
{
    auto & context = module.getContext();
    llvm::IRBuilder<> b(context);

    auto * size_type = b.getIntNTy(sizeof(size_t) * 8);
    auto * places_type = b.getInt8Ty()->getPointerTo();
    auto * column_data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy());

    auto * aggregate_loop_func_declaration = llvm::FunctionType::get(b.getVoidTy(), { size_type, column_data_type->getPointerTo(), places_type }, false);
    auto * aggregate_loop_func_definition = llvm::Function::Create(aggregate_loop_func_declaration, llvm::Function::ExternalLinkage, name, module);

    auto * arguments = aggregate_loop_func_definition->args().begin();
    llvm::Value * rows_count_arg = arguments++;
    llvm::Value * columns_arg = arguments++;
    llvm::Value * place_arg = arguments++;

    /// Initialize ColumnDataPlaceholder llvm representation of ColumnData

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", aggregate_loop_func_definition);
    b.SetInsertPoint(entry);

    std::vector<ColumnDataPlaceholder> columns;
    size_t previous_columns_size = 0;

    for (const auto & function : functions)
    {
        auto argument_types = function.function->getArgumentTypes();

        ColumnDataPlaceholder data_placeholder;

        size_t function_arguments_size = argument_types.size();

        for (size_t column_argument_index = 0; column_argument_index < function_arguments_size; ++column_argument_index)
        {
            const auto & argument_type = argument_types[column_argument_index];
            auto * data = b.CreateLoad(column_data_type, b.CreateConstInBoundsGEP1_64(column_data_type, columns_arg, previous_columns_size + column_argument_index));
            data_placeholder.data_init = b.CreatePointerCast(b.CreateExtractValue(data, {0}), toNativeType(b, removeNullable(argument_type))->getPointerTo());
            data_placeholder.null_init = argument_type->isNullable() ? b.CreateExtractValue(data, {1}) : nullptr;
            columns.emplace_back(data_placeholder);
        }

        previous_columns_size += function_arguments_size;
    }

    /// Initialize loop

    auto * end = llvm::BasicBlock::Create(b.getContext(), "end", aggregate_loop_func_definition);
    auto * loop = llvm::BasicBlock::Create(b.getContext(), "loop", aggregate_loop_func_definition);

    b.CreateCondBr(b.CreateICmpEQ(rows_count_arg, llvm::ConstantInt::get(size_type, 0)), end, loop);

    b.SetInsertPoint(loop);

    auto * counter_phi = b.CreatePHI(rows_count_arg->getType(), 2);
    counter_phi->addIncoming(llvm::ConstantInt::get(size_type, 0), entry);

    for (auto & col : columns)
    {
        col.data = b.CreatePHI(col.data_init->getType(), 2);
        col.data->addIncoming(col.data_init, entry);

        if (col.null_init)
        {
            col.null = b.CreatePHI(col.null_init->getType(), 2);
            col.null->addIncoming(col.null_init, entry);
        }
    }

    previous_columns_size = 0;
    for (const auto & function : functions)
    {
        size_t aggregate_function_offset = function.aggregate_data_offset;
        const auto * aggregate_function_ptr = function.function;

        auto arguments_types = function.function->getArgumentTypes();
        std::vector<llvm::Value *> arguments_values;

        size_t function_arguments_size = arguments_types.size();
        arguments_values.resize(function_arguments_size);

        for (size_t column_argument_index = 0; column_argument_index < function_arguments_size; ++column_argument_index)
        {
            auto * column_argument_data = columns[previous_columns_size + column_argument_index].data;
            auto * column_argument_null_data = columns[previous_columns_size + column_argument_index].null;

            auto & argument_type = arguments_types[column_argument_index];

            auto * value = b.CreateLoad(toNativeType(b, removeNullable(argument_type)), column_argument_data);
            if (!argument_type->isNullable())
            {
                arguments_values[column_argument_index] = value;
                continue;
            }

            auto * is_null = b.CreateICmpNE(b.CreateLoad(b.getInt8Ty(), column_argument_null_data), b.getInt8(0));
            auto * nullable_unitilized = llvm::Constant::getNullValue(toNativeType(b, argument_type));
            auto * nullable_value = b.CreateInsertValue(b.CreateInsertValue(nullable_unitilized, value, {0}), is_null, {1});
            arguments_values[column_argument_index] = nullable_value;
        }

        auto * aggregation_place_with_offset = b.CreateConstInBoundsGEP1_64(nullptr, place_arg, aggregate_function_offset);
        aggregate_function_ptr->compileAdd(b, aggregation_place_with_offset, arguments_types, arguments_values);

        previous_columns_size += function_arguments_size;
    }

    /// End of loop

    auto * cur_block = b.GetInsertBlock();
    for (auto & col : columns)
    {
        col.data->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, col.data, 1), cur_block);

        if (col.null)
            col.null->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, col.null, 1), cur_block);
    }

    auto * value = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1));
    counter_phi->addIncoming(value, cur_block);

    b.CreateCondBr(b.CreateICmpEQ(value, rows_count_arg), end, loop);

    b.SetInsertPoint(end);
    b.CreateRetVoid();
}

static void compileMergeAggregatesStates(llvm::Module & module, const std::vector<AggregateFunctionWithOffset> & functions, const std::string & name)
{
    auto & context = module.getContext();
    llvm::IRBuilder<> b(context);

    auto * aggregate_data_places_type = b.getInt8Ty()->getPointerTo();
    auto * aggregate_loop_func_declaration = llvm::FunctionType::get(b.getVoidTy(), { aggregate_data_places_type, aggregate_data_places_type }, false);
    auto * aggregate_loop_func = llvm::Function::Create(aggregate_loop_func_declaration, llvm::Function::ExternalLinkage, name, module);

    auto * arguments = aggregate_loop_func->args().begin();
    llvm::Value * aggregate_data_place_dst_arg = arguments++;
    llvm::Value * aggregate_data_place_src_arg = arguments++;

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", aggregate_loop_func);
    b.SetInsertPoint(entry);

    for (const auto & function_to_compile : functions)
    {
        size_t aggregate_function_offset = function_to_compile.aggregate_data_offset;
        const auto * aggregate_function_ptr = function_to_compile.function;

        auto * aggregate_data_place_merge_dst_with_offset = b.CreateConstInBoundsGEP1_64(nullptr, aggregate_data_place_dst_arg, aggregate_function_offset);
        auto * aggregate_data_place_merge_src_with_offset = b.CreateConstInBoundsGEP1_64(nullptr, aggregate_data_place_src_arg, aggregate_function_offset);

        aggregate_function_ptr->compileMerge(b, aggregate_data_place_merge_dst_with_offset, aggregate_data_place_merge_src_with_offset);
    }

    b.CreateRetVoid();
}

static void compileInsertAggregatesIntoResultColumns(llvm::Module & module, const std::vector<AggregateFunctionWithOffset> & functions, const std::string & name)
{
    auto & context = module.getContext();
    llvm::IRBuilder<> b(context);

    auto * size_type = b.getIntNTy(sizeof(size_t) * 8);

    auto * column_data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy());
    auto * aggregate_data_places_type = b.getInt8Ty()->getPointerTo()->getPointerTo();
    auto * aggregate_loop_func_declaration = llvm::FunctionType::get(b.getVoidTy(), { size_type, column_data_type->getPointerTo(), aggregate_data_places_type }, false);
    auto * aggregate_loop_func = llvm::Function::Create(aggregate_loop_func_declaration, llvm::Function::ExternalLinkage, name, module);

    auto * arguments = aggregate_loop_func->args().begin();
    llvm::Value * rows_count_arg = &*arguments++;
    llvm::Value * columns_arg = &*arguments++;
    llvm::Value * aggregate_data_places_arg = &*arguments++;

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", aggregate_loop_func);
    b.SetInsertPoint(entry);

    std::vector<ColumnDataPlaceholder> columns(functions.size());
    for (size_t i = 0; i < functions.size(); ++i)
    {
        auto return_type = functions[i].function->getReturnType();
        auto * data = b.CreateLoad(column_data_type, b.CreateConstInBoundsGEP1_64(column_data_type, columns_arg, i));
        columns[i].data_init = b.CreatePointerCast(b.CreateExtractValue(data, {0}), toNativeType(b, removeNullable(return_type))->getPointerTo());
        columns[i].null_init = return_type->isNullable() ? b.CreateExtractValue(data, {1}) : nullptr;
    }

    auto * end = llvm::BasicBlock::Create(b.getContext(), "end", aggregate_loop_func);
    auto * loop = llvm::BasicBlock::Create(b.getContext(), "loop", aggregate_loop_func);

    b.CreateCondBr(b.CreateICmpEQ(rows_count_arg, llvm::ConstantInt::get(size_type, 0)), end, loop);

    b.SetInsertPoint(loop);

    auto * counter_phi = b.CreatePHI(rows_count_arg->getType(), 2);
    counter_phi->addIncoming(llvm::ConstantInt::get(size_type, 0), entry);

    auto * aggregate_data_place_phi = b.CreatePHI(aggregate_data_places_type, 2);
    aggregate_data_place_phi->addIncoming(aggregate_data_places_arg, entry);

    for (auto & col : columns)
    {
        col.data = b.CreatePHI(col.data_init->getType(), 2);
        col.data->addIncoming(col.data_init, entry);

        if (col.null_init)
        {
            col.null = b.CreatePHI(col.null_init->getType(), 2);
            col.null->addIncoming(col.null_init, entry);
        }
    }

    for (size_t i = 0; i < functions.size(); ++i)
    {
        size_t aggregate_function_offset = functions[i].aggregate_data_offset;
        const auto * aggregate_function_ptr = functions[i].function;

        auto * aggregate_data_place = b.CreateLoad(b.getInt8Ty()->getPointerTo(), aggregate_data_place_phi);
        auto * aggregation_place_with_offset = b.CreateConstInBoundsGEP1_64(nullptr, aggregate_data_place, aggregate_function_offset);

        auto * final_value = aggregate_function_ptr->compileGetResult(b, aggregation_place_with_offset);

        if (columns[i].null_init)
        {
            b.CreateStore(b.CreateExtractValue(final_value, {0}), columns[i].data);
            b.CreateStore(b.CreateSelect(b.CreateExtractValue(final_value, {1}), b.getInt8(1), b.getInt8(0)), columns[i].null);
        }
        else
        {
            b.CreateStore(final_value, columns[i].data);
        }
    }

    /// End of loop

    auto * cur_block = b.GetInsertBlock();
    for (auto & col : columns)
    {
        col.data->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, col.data, 1), cur_block);

        if (col.null)
            col.null->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, col.null, 1), cur_block);
    }

    auto * value = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1), "", true, true);
    counter_phi->addIncoming(value, cur_block);

    aggregate_data_place_phi->addIncoming(b.CreateConstInBoundsGEP1_64(nullptr, aggregate_data_place_phi, 1), cur_block);

    b.CreateCondBr(b.CreateICmpEQ(value, rows_count_arg), end, loop);

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
        compileAddIntoAggregateStatesFunctions(module, functions, add_aggregate_states_functions_name);
        compileAddIntoAggregateStatesFunctionsSinglePlace(module, functions, add_aggregate_states_functions_name_single_place);
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

}

#endif
