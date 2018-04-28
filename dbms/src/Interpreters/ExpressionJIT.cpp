#include <Interpreters/ExpressionJIT.h>

#if USE_EMBEDDED_COMPILER

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Native.h>

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Mangler.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/ExecutionEngine/Orc/CompileUtils.h>
#include <llvm/ExecutionEngine/Orc/IRCompileLayer.h>
#include <llvm/ExecutionEngine/Orc/NullResolver.h>
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct LLVMContext::Data
{
    llvm::LLVMContext context;
    std::shared_ptr<llvm::Module> module;
    std::unique_ptr<llvm::TargetMachine> machine;
    llvm::orc::RTDyldObjectLinkingLayer objectLayer;
    llvm::orc::IRCompileLayer<decltype(objectLayer), llvm::orc::SimpleCompiler> compileLayer;
    llvm::DataLayout layout;
    llvm::IRBuilder<> builder;

    Data()
        : module(std::make_shared<llvm::Module>("jit", context))
        , machine(llvm::EngineBuilder().selectTarget())
        , objectLayer([]() { return std::make_shared<llvm::SectionMemoryManager>(); })
        , compileLayer(objectLayer, llvm::orc::SimpleCompiler(*machine))
        , layout(machine->createDataLayout())
        , builder(context)
    {
        module->setDataLayout(layout);
        module->setTargetTriple(machine->getTargetTriple().getTriple());
    }
};

LLVMContext::LLVMContext()
    : shared(std::make_shared<LLVMContext::Data>())
{}

void LLVMContext::finalize()
{
    if (!shared->module->size())
        return;
    llvm::PassManagerBuilder builder;
    llvm::legacy::FunctionPassManager fpm(shared->module.get());
    builder.OptLevel = 3;
    builder.SLPVectorize = true;
    builder.LoopVectorize = true;
    builder.RerollLoops = true;
    builder.VerifyInput = true;
    builder.VerifyOutput = true;
    builder.populateFunctionPassManager(fpm);
    for (auto & function : *shared->module)
        fpm.run(function);
    llvm::cantFail(shared->compileLayer.addModule(shared->module, std::make_shared<llvm::orc::NullResolver>()));
}

bool LLVMContext::isCompilable(const IFunctionBase& function) const
{
    if (!toNativeType(shared->builder, function.getReturnType()))
        return false;
    for (const auto & type : function.getArgumentTypes())
        if (!toNativeType(shared->builder, type))
            return false;
    return function.isCompilable();
}

LLVMPreparedFunction::LLVMPreparedFunction(LLVMContext context, std::shared_ptr<const IFunctionBase> parent)
    : parent(parent), context(context)
{
    std::string mangledName;
    llvm::raw_string_ostream mangledNameStream(mangledName);
    llvm::Mangler::getNameWithPrefix(mangledNameStream, parent->getName(), context->layout);
    function = reinterpret_cast<const void *>(context->compileLayer.findSymbol(mangledNameStream.str(), false).getAddress().get());
}

namespace
{
    struct ColumnData
    {
        const char * data = nullptr;
        const char * null = nullptr;
        size_t stride;
    };

    struct ColumnDataPlaceholders
    {
        llvm::PHINode * data;
        llvm::PHINode * null;
        llvm::Value * data_init;
        llvm::Value * null_init;
        llvm::Value * stride;
        llvm::Value * is_const;
    };
}

static ColumnData getColumnData(const IColumn * column)
{
    ColumnData result;
    const bool is_const = column->isColumnConst();
    if (is_const)
        column = &reinterpret_cast<const ColumnConst *>(column)->getDataColumn();
    if (auto * nullable = typeid_cast<const ColumnNullable *>(column))
    {
        result.null = nullable->getNullMapColumn().getRawData().data;
        column = &nullable->getNestedColumn();
    }
    result.data = column->getRawData().data;
    result.stride = is_const ? 0 : column->sizeOfValueIfFixed();
    return result;
}

void LLVMPreparedFunction::execute(Block & block, const ColumnNumbers & arguments, size_t result)
{
    size_t block_size = block.rows();
    auto col_res = parent->getReturnType()->createColumn()->cloneResized(block_size);
    if (block_size)
    {
        std::vector<ColumnData> columns(arguments.size() + 1);
        for (size_t i = 0; i < arguments.size(); i++)
        {
            auto * column = block.getByPosition(arguments[i]).column.get();
            if (!column)
                throw Exception("column " + block.getByPosition(arguments[i]).name + " is missing", ErrorCodes::LOGICAL_ERROR);
            columns[i] = getColumnData(column);
        }
        columns[arguments.size()] = getColumnData(col_res.get());
        reinterpret_cast<void (*) (size_t, ColumnData *)>(function)(block_size, columns.data());
    }
    block.getByPosition(result).column = std::move(col_res);
};

LLVMFunction::LLVMFunction(ExpressionActions::Actions actions_, LLVMContext context, const Block & sample_block)
    : actions(std::move(actions_)), context(context)
{
    auto & b = context->builder;
    auto * size_type = b.getIntNTy(sizeof(size_t) * 8);
    auto * data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy(), size_type);
    auto * func_type = llvm::FunctionType::get(b.getVoidTy(), { size_type, llvm::PointerType::get(data_type, 0) }, /*isVarArg=*/false);
    auto * func = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, actions.back().result_name, context->module.get());
    auto args = func->args().begin();
    llvm::Value * counter = &*args++;
    llvm::Value * columns = &*args++;

    auto * entry = llvm::BasicBlock::Create(context->context, "entry", func);
    b.SetInsertPoint(entry);

    std::unordered_map<std::string, std::function<llvm::Value * ()>> by_name;
    for (const auto & c : sample_block)
        if (auto * value = getNativeValue(toNativeType(b, c.type), c.column.get(), 0))
            by_name[c.name] = [=]() { return value; };

    std::unordered_set<std::string> seen;
    for (const auto & action : actions)
    {
        const auto & names = action.argument_names;
        const auto & types = action.function->getArgumentTypes();
        for (size_t i = 0; i < names.size(); i++)
        {
            if (!seen.emplace(names[i]).second || by_name.find(names[i]) != by_name.end())
                continue;
            arg_names.push_back(names[i]);
            arg_types.push_back(types[i]);
        }
        seen.insert(action.result_name);
    }

    std::vector<ColumnDataPlaceholders> columns_v(arg_types.size() + 1);
    for (size_t i = 0; i <= arg_types.size(); i++)
    {
        auto & column_type = (i == arg_types.size()) ? getReturnType() : arg_types[i];
        auto * type = llvm::PointerType::get(toNativeType(b, removeNullable(column_type)), 0);
        columns_v[i].data_init = b.CreatePointerCast(b.CreateLoad(b.CreateConstInBoundsGEP2_32(data_type, columns, i, 0)), type);
        columns_v[i].stride = b.CreateLoad(b.CreateConstInBoundsGEP2_32(data_type, columns, i, 2));
        if (column_type->isNullable())
        {
            columns_v[i].null_init = b.CreateLoad(b.CreateConstInBoundsGEP2_32(data_type, columns, i, 1));
            columns_v[i].is_const = b.CreateICmpEQ(columns_v[i].stride, b.getIntN(sizeof(size_t) * 8, 0));
        }
    }

    for (size_t i = 0; i < arg_types.size(); i++)
    {
        by_name[arg_names[i]] = [&, &col = columns_v[i], i]() -> llvm::Value *
        {
            auto * value = b.CreateLoad(col.data);
            if (!col.null)
                return value;
            auto * is_null = b.CreateICmpNE(b.CreateLoad(col.null), b.getInt8(0));
            auto * nullable = getDefaultNativeValue(toNativeType(b, arg_types[i]));
            return b.CreateInsertValue(b.CreateInsertValue(nullable, value, {0}), is_null, {1});
        };
    }
    for (const auto & action : actions)
    {
        ValuePlaceholders input;
        for (const auto & name : action.argument_names)
            input.push_back(by_name.at(name));
        /// TODO: pass compile-time constant arguments to `compilePrologue`?
        auto extra = action.function->compilePrologue(b);
        for (auto * value : extra)
            input.emplace_back([=]() { return value; });
        by_name[action.result_name] = [&, input = std::move(input)]() {
            auto * result = action.function->compile(b, input);
            if (result->getType() != toNativeType(b, action.function->getReturnType()))
                throw Exception("function " + action.function->getName() + " generated an llvm::Value of invalid type",
                    ErrorCodes::LOGICAL_ERROR);
            return result;
        };
    }

    /// assume nonzero initial value in `counter`
    auto * loop = llvm::BasicBlock::Create(context->context, "loop", func);
    b.CreateBr(loop);
    b.SetInsertPoint(loop);
    auto * counter_phi = b.CreatePHI(counter->getType(), 2);
    counter_phi->addIncoming(counter, entry);
    for (auto & col : columns_v)
    {
        col.data = b.CreatePHI(col.data_init->getType(), 2);
        col.data->addIncoming(col.data_init, entry);
        if (col.null_init)
        {
            col.null = b.CreatePHI(col.null_init->getType(), 2);
            col.null->addIncoming(col.null_init, entry);
        }
    }

    auto * result = by_name.at(getName())();
    if (columns_v[arg_types.size()].null)
    {
        b.CreateStore(b.CreateExtractValue(result, {0}), columns_v[arg_types.size()].data);
        b.CreateStore(b.CreateSelect(b.CreateExtractValue(result, {1}), b.getInt8(1), b.getInt8(0)), columns_v[arg_types.size()].null);
    }
    else
    {
        b.CreateStore(result, columns_v[arg_types.size()].data);
    }

    auto * cur_block = b.GetInsertBlock();
    for (auto & col : columns_v)
    {
        auto * as_char = b.CreatePointerCast(col.data, b.getInt8PtrTy());
        auto * as_type = b.CreatePointerCast(b.CreateInBoundsGEP(as_char, col.stride), col.data->getType());
        col.data->addIncoming(as_type, cur_block);
        if (col.null)
            col.null->addIncoming(b.CreateSelect(col.is_const, col.null, b.CreateConstInBoundsGEP1_32(b.getInt8Ty(), col.null, 1)), cur_block);
    }
    counter_phi->addIncoming(b.CreateSub(counter_phi, llvm::ConstantInt::get(size_type, 1)), cur_block);

    auto * end = llvm::BasicBlock::Create(context->context, "end", func);
    b.CreateCondBr(b.CreateICmpNE(counter_phi, llvm::ConstantInt::get(size_type, 1)), loop, end);
    b.SetInsertPoint(end);
    b.CreateRetVoid();
}

static void applyFunction(IFunctionBase & function, Field & value)
{
    const auto & type = function.getArgumentTypes().at(0);
    Block block = {{ type->createColumnConst(1, value), type, "x" }, { nullptr, function.getReturnType(), "y" }};
    function.execute(block, {0}, 1);
    block.safeGetByPosition(1).column->get(0, value);
}

IFunctionBase::Monotonicity LLVMFunction::getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const
{
    const IDataType * type_ = &type;
    Field left_ = left;
    Field right_ = right;
    Monotonicity result(true, true, true);
    /// monotonicity is only defined for unary functions, so the chain must describe a sequence of nested calls
    for (size_t i = 0; i < actions.size(); i++)
    {
        Monotonicity m = actions[i].function->getMonotonicityForRange(*type_, left_, right_);
        if (!m.is_monotonic)
            return m;
        result.is_positive ^= !m.is_positive;
        result.is_always_monotonic &= m.is_always_monotonic;
        if (i + 1 < actions.size())
        {
            if (left_ != Field())
                applyFunction(*actions[i].function, left_);
            if (right_ != Field())
                applyFunction(*actions[i].function, right_);
            if (!m.is_positive)
                std::swap(left_, right_);
            type_ = actions[i].function->getReturnType().get();
        }
    }
    return result;
}

}


namespace
{
    struct LLVMTargetInitializer
    {
        LLVMTargetInitializer()
        {
            llvm::InitializeNativeTarget();
            llvm::InitializeNativeTargetAsmPrinter();
        }
    } llvmInitializer;
}

#endif
