#include <Interpreters/ExpressionJIT.h>

#if USE_EMBEDDED_COMPILER

#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Mangler.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Verifier.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/ExecutionEngine/Orc/CompileUtils.h>
#include <llvm/ExecutionEngine/Orc/IRCompileLayer.h>
#include <llvm/ExecutionEngine/Orc/NullResolver.h>
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>

#include <stdexcept>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename T>
static bool typeIsA(const DataTypePtr & type)
{
    return typeid_cast<const T *>(removeNullable(type).get());;
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

    llvm::Type * toNativeType(const DataTypePtr & type)
    {
        /// LLVM doesn't have unsigned types, it has unsigned instructions.
        if (typeIsA<DataTypeInt8>(type) || typeIsA<DataTypeUInt8>(type))
            return builder.getInt8Ty();
        if (typeIsA<DataTypeInt16>(type) || typeIsA<DataTypeUInt16>(type))
            return builder.getInt16Ty();
        if (typeIsA<DataTypeInt32>(type) || typeIsA<DataTypeUInt32>(type))
            return builder.getInt32Ty();
        if (typeIsA<DataTypeInt64>(type) || typeIsA<DataTypeUInt64>(type))
            return builder.getInt64Ty();
        if (typeIsA<DataTypeFloat32>(type))
            return builder.getFloatTy();
        if (typeIsA<DataTypeFloat64>(type))
            return builder.getDoubleTy();
        return nullptr;
    }

    const void * lookup(const std::string& name)
    {
        std::string mangledName;
        llvm::raw_string_ostream mangledNameStream(mangledName);
        llvm::Mangler::getNameWithPrefix(mangledNameStream, name, layout);
        /// why is `findSymbol` not const? we may never know.
        return reinterpret_cast<const void *>(compileLayer.findSymbol(mangledNameStream.str(), false).getAddress().get());
    }
};

LLVMContext::LLVMContext()
    : shared(std::make_shared<LLVMContext::Data>())
{}

void LLVMContext::finalize()
{
    if (!shared->module->size())
        return;
    shared->module->print(llvm::errs(), nullptr, false, true);
    llvm::PassManagerBuilder builder;
    llvm::legacy::FunctionPassManager fpm(shared->module.get());
    builder.OptLevel = 2;
    builder.populateFunctionPassManager(fpm);
    for (auto & function : *shared->module)
        fpm.run(function);
    llvm::cantFail(shared->compileLayer.addModule(shared->module, std::make_shared<llvm::orc::NullResolver>()));
    shared->module->print(llvm::errs(), nullptr, false, true);
}

bool LLVMContext::isCompilable(const IFunctionBase& function) const
{
    if (!function.isCompilable() || !shared->toNativeType(function.getReturnType()))
        return false;
    for (const auto & type : function.getArgumentTypes())
        if (!shared->toNativeType(type))
            return false;
    return true;
}

LLVMPreparedFunction::LLVMPreparedFunction(LLVMContext context, std::shared_ptr<const IFunctionBase> parent)
    : parent(parent), context(context), function(context->lookup(parent->getName()))
{}

namespace
{
    struct ColumnData
    {
        const char * data;
        size_t stride;
    };
}

static ColumnData getColumnData(const IColumn * column)
{
    if (!column->isFixedAndContiguous())
        throw Exception("column type " + column->getName() + " is not a contiguous array; its data type "
                        "should've had no native equivalent in LLVMContext::Data::toNativeType", ErrorCodes::LOGICAL_ERROR);
    /// TODO: handle ColumnNullable
    return {column->getRawData().data, !column->isColumnConst() ? column->sizeOfValueIfFixed() : 0};
}

void LLVMPreparedFunction::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    size_t block_size = block.rows();
    /// assuming that the function has default behavior on NULL, the column will be wrapped by `PreparedFunctionImpl::execute`.
    auto col_res = removeNullable(parent->getReturnType())->createColumn()->cloneResized(block_size);
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
    std::unordered_map<std::string, std::function<llvm::Value * ()>> by_name;
    for (const auto & c : sample_block)
    {
        auto generator = [&]() -> llvm::Value *
        {
            auto * type = context->toNativeType(c.type);
            if (typeIsA<DataTypeFloat32>(c.type))
                return llvm::ConstantFP::get(type, typeid_cast<const ColumnVector<Float32> *>(c.column.get())->getElement(0));
            if (typeIsA<DataTypeFloat64>(c.type))
                return llvm::ConstantFP::get(type, typeid_cast<const ColumnVector<Float64> *>(c.column.get())->getElement(0));
            if (type && type->isIntegerTy())
                return llvm::ConstantInt::get(type, c.column->getUInt(0));
            return nullptr;
        };
        if (c.column && generator() && !by_name.emplace(c.name, std::move(generator)).second)
            throw Exception("duplicate constant column " + c.name, ErrorCodes::LOGICAL_ERROR);
    }

    std::unordered_set<std::string> seen;
    for (const auto & action : actions)
    {
        const auto & names = action.argument_names;
        const auto & types = action.function->getArgumentTypes();
        for (size_t i = 0; i < names.size(); i++)
        {
            if (seen.emplace(names[i]).second && by_name.find(names[i]) == by_name.end())
            {
                arg_names.push_back(names[i]);
                arg_types.push_back(types[i]);
            }
        }
        seen.insert(action.result_name);
    }

    auto * char_type = context->builder.getInt8Ty();
    auto * size_type = context->builder.getIntNTy(sizeof(size_t) * 8);
    auto * data_type = llvm::StructType::get(llvm::PointerType::get(char_type, 0), size_type);
    auto * func_type = llvm::FunctionType::get(context->builder.getVoidTy(), { size_type, llvm::PointerType::get(data_type, 0) }, /*isVarArg=*/false);
    auto * func = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, actions.back().result_name, context->module.get());
    auto args = func->args().begin();
    llvm::Value * counter = &*args++;
    llvm::Value * columns = &*args++;

    auto * entry = llvm::BasicBlock::Create(context->context, "entry", func);
    context->builder.SetInsertPoint(entry);

    struct CastedColumnData
    {
        llvm::PHINode * data;
        llvm::Value * data_init;
        llvm::Value * stride;
    };
    std::vector<CastedColumnData> columns_v(arg_types.size() + 1);
    for (size_t i = 0; i <= arg_types.size(); i++)
    {
        auto * type = llvm::PointerType::getUnqual(context->toNativeType(i == arg_types.size() ? getReturnType() : arg_types[i]));
        auto * data = context->builder.CreateConstInBoundsGEP2_32(data_type, columns, i, 0);
        auto * stride = context->builder.CreateConstInBoundsGEP2_32(data_type, columns, i, 1);
        columns_v[i] = { nullptr, context->builder.CreatePointerCast(context->builder.CreateLoad(data), type), context->builder.CreateLoad(stride) };
    }

    /// assume nonzero initial value in `counter`
    auto * loop = llvm::BasicBlock::Create(context->context, "loop", func);
    context->builder.CreateBr(loop);
    context->builder.SetInsertPoint(loop);
    auto * counter_phi = context->builder.CreatePHI(counter->getType(), 2);
    counter_phi->addIncoming(counter, entry);
    for (auto & col : columns_v)
    {
        col.data = context->builder.CreatePHI(col.data_init->getType(), 2);
        col.data->addIncoming(col.data_init, entry);
    }

    for (size_t i = 0; i < arg_types.size(); i++)
        if (!by_name.emplace(arg_names[i], [&, i]() { return context->builder.CreateLoad(columns_v[i].data); }).second)
            throw Exception("duplicate input column name " + arg_names[i], ErrorCodes::LOGICAL_ERROR);
    for (const auto & action : actions)
    {
        ValuePlaceholders action_input;
        action_input.reserve(action.argument_names.size());
        for (const auto & name : action.argument_names)
            action_input.push_back(by_name.at(name));
        auto generator = [&action, &context, action_input{std::move(action_input)}]()
        {
            return action.function->compile(context->builder, action_input);
        };
        if (!by_name.emplace(action.result_name, std::move(generator)).second)
            throw Exception("duplicate action result name " + action.result_name, ErrorCodes::LOGICAL_ERROR);
    }
    context->builder.CreateStore(by_name.at(actions.back().result_name)(), columns_v[arg_types.size()].data);

    auto * cur_block = context->builder.GetInsertBlock();
    for (auto & col : columns_v)
    {
        auto * as_char = context->builder.CreatePointerCast(col.data, llvm::PointerType::get(char_type, 0));
        auto * as_type = context->builder.CreatePointerCast(context->builder.CreateGEP(as_char, col.stride), col.data->getType());
        col.data->addIncoming(as_type, cur_block);
    }
    counter_phi->addIncoming(context->builder.CreateSub(counter_phi, llvm::ConstantInt::get(counter_phi->getType(), 1)), cur_block);

    auto * end = llvm::BasicBlock::Create(context->context, "end", func);
    context->builder.CreateCondBr(context->builder.CreateICmpNE(counter_phi, llvm::ConstantInt::get(counter_phi->getType(), 1)), loop, end);
    context->builder.SetInsertPoint(end);
    context->builder.CreateRetVoid();
}

static Field evaluateFunction(IFunctionBase & function, const IDataType & type, const Field & arg)
{
    const auto & arg_types = function.getArgumentTypes();
    if (arg_types.size() != 1 || !arg_types[0]->equals(type))
        return {};
    auto column = arg_types[0]->createColumn();
    column->insert(arg);
    Block block = {{ ColumnConst::create(std::move(column), 1), arg_types[0], "_arg" }, { nullptr, function.getReturnType(), "_result" }};
    function.execute(block, {0}, 1);
    auto result = block.getByPosition(1).column;
    return result && result->size() == 1 ? (*result)[0] : Field();
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
        Monotonicity m = actions[i].function->getMonotonicityForRange(type, left_, right_);
        if (!m.is_monotonic)
            return m;
        result.is_positive ^= !m.is_positive;
        result.is_always_monotonic &= m.is_always_monotonic;
        if (i + 1 < actions.size())
        {
            if (left_ != Field())
                left_ = evaluateFunction(*actions[i].function, *type_, left_);
            if (right_ != Field())
                right_ = evaluateFunction(*actions[i].function, *type_, right_);
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
