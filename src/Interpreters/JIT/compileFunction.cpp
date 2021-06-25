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

    ProfileEvents::increment(ProfileEvents::CompileFunction);

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
        auto * data = b.CreateLoad(data_type, b.CreateConstInBoundsGEP1_32(data_type, columns_arg, i));
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
        auto type = arg_types[i];

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
        col.data->addIncoming(b.CreateConstInBoundsGEP1_32(nullptr, col.data, 1), cur_block);
        if (col.null)
            col.null->addIncoming(b.CreateConstInBoundsGEP1_32(nullptr, col.null, 1), cur_block);
    }

    auto * value = b.CreateAdd(counter_phi, llvm::ConstantInt::get(size_type, 1));
    counter_phi->addIncoming(value, cur_block);

    b.CreateCondBr(b.CreateICmpEQ(value, rows_count_arg), end, loop);

    b.SetInsertPoint(end);
    b.CreateRetVoid();
}

CHJIT::CompiledModuleInfo compileFunction(CHJIT & jit, const IFunctionBase & function)
{
    Stopwatch watch;

    auto compiled_module_info = jit.compileModule([&](llvm::Module & module)
    {
        compileFunction(module, function);
    });

    ProfileEvents::increment(ProfileEvents::CompileExpressionsMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::CompileExpressionsBytes, compiled_module_info.size);
    ProfileEvents::increment(ProfileEvents::CompileFunction);

    return compiled_module_info;
}

}

#endif
