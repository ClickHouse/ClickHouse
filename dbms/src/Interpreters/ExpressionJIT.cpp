#include <Interpreters/ExpressionJIT.h>

#if USE_EMBEDDED_COMPILER

#include <optional>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Common/LRUCache.h>
#include <Common/MemoryTracker.h>
#include <Common/typeid_cast.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Native.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"

#include <llvm/Analysis/TargetTransformInfo.h> // Y_IGNORE
#include <llvm/Config/llvm-config.h> // Y_IGNORE
#include <llvm/IR/BasicBlock.h> // Y_IGNORE
#include <llvm/IR/DataLayout.h> // Y_IGNORE
#include <llvm/IR/DerivedTypes.h> // Y_IGNORE
#include <llvm/IR/Function.h> // Y_IGNORE
#include <llvm/IR/IRBuilder.h> // Y_IGNORE
#include <llvm/IR/LLVMContext.h> // Y_IGNORE
#include <llvm/IR/Mangler.h> // Y_IGNORE
#include <llvm/IR/Module.h> // Y_IGNORE
#include <llvm/IR/Type.h> // Y_IGNORE
#include <llvm/ExecutionEngine/ExecutionEngine.h> // Y_IGNORE
#include <llvm/ExecutionEngine/JITSymbol.h> // Y_IGNORE
#include <llvm/ExecutionEngine/SectionMemoryManager.h> // Y_IGNORE
#include <llvm/ExecutionEngine/Orc/CompileUtils.h> // Y_IGNORE
#include <llvm/ExecutionEngine/Orc/IRCompileLayer.h> // Y_IGNORE
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h> // Y_IGNORE
#include <llvm/Target/TargetMachine.h> // Y_IGNORE
#include <llvm/MC/SubtargetFeature.h> // Y_IGNORE
#include <llvm/Support/DynamicLibrary.h> // Y_IGNORE
#include <llvm/Support/Host.h> // Y_IGNORE
#include <llvm/Support/TargetRegistry.h> // Y_IGNORE
#include <llvm/Support/TargetSelect.h> // Y_IGNORE
#include <llvm/Transforms/IPO/PassManagerBuilder.h> // Y_IGNORE

#pragma GCC diagnostic pop


namespace ProfileEvents
{
    extern const Event CompileFunction;
    extern const Event CompileExpressionsMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_COMPILE_CODE;
}

namespace
{
    struct ColumnData
    {
        const char * data = nullptr;
        const char * null = nullptr;
        size_t stride;
    };

    struct ColumnDataPlaceholder
    {
        llvm::Value * data_init; /// first row
        llvm::Value * null_init;
        llvm::Value * stride;
        llvm::PHINode * data; /// current row
        llvm::PHINode * null;
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

static void applyFunction(IFunctionBase & function, Field & value)
{
    const auto & type = function.getArgumentTypes().at(0);
    Block block = {{ type->createColumnConst(1, value), type, "x" }, { nullptr, function.getReturnType(), "y" }};
    function.execute(block, {0}, 1, 1);
    block.safeGetByPosition(1).column->get(0, value);
}

static llvm::TargetMachine * getNativeMachine()
{
    std::string error;
    auto cpu = llvm::sys::getHostCPUName();
    auto triple = llvm::sys::getProcessTriple();
    auto target = llvm::TargetRegistry::lookupTarget(triple, error);
    if (!target)
        throw Exception("Could not initialize native target: " + error, ErrorCodes::CANNOT_COMPILE_CODE);
    llvm::SubtargetFeatures features;
    llvm::StringMap<bool> feature_map;
    if (llvm::sys::getHostCPUFeatures(feature_map))
        for (auto & f : feature_map)
            features.AddFeature(f.first(), f.second);
    llvm::TargetOptions options;
    return target->createTargetMachine(
        triple, cpu, features.getString(), options, llvm::None,
#if LLVM_VERSION_MAJOR >= 6
        llvm::None, llvm::CodeGenOpt::Default, /*jit=*/true
#else
        llvm::CodeModel::Default, llvm::CodeGenOpt::Default
#endif
    );
}

#if LLVM_VERSION_MAJOR >= 7
auto wrapJITSymbolResolver(llvm::JITSymbolResolver & jsr)
{
    auto flags = [&](llvm::orc::SymbolFlagsMap & flags, const llvm::orc::SymbolNameSet & symbols)
    {
        llvm::orc::SymbolNameSet missing;
        for (const auto & symbol : symbols)
        {
            auto resolved = jsr.lookupFlags({*symbol});
            if (resolved && resolved->size())
                flags.emplace(symbol, resolved->begin()->second);
            else
                missing.emplace(symbol);
        }
        return missing;
    };
    auto symbols = [&](std::shared_ptr<llvm::orc::AsynchronousSymbolQuery> query, llvm::orc::SymbolNameSet symbols)
    {
        llvm::orc::SymbolNameSet missing;
        for (const auto & symbol : symbols)
        {
            auto resolved = jsr.lookup({*symbol});
            if (resolved && resolved->size())
                query->resolve(symbol, resolved->begin()->second);
            else
                missing.emplace(symbol);
        }
        return missing;
    };
    return llvm::orc::createSymbolResolver(flags, symbols);
}
#endif

#if LLVM_VERSION_MAJOR >= 6
struct CountingMMapper final : public llvm::SectionMemoryManager::MemoryMapper
{
    MemoryTracker memory_tracker{VariableContext::Global};

    llvm::sys::MemoryBlock allocateMappedMemory(llvm::SectionMemoryManager::AllocationPurpose /*purpose*/,
        size_t num_bytes,
        const llvm::sys::MemoryBlock * const near_block,
        unsigned flags,
        std::error_code & error_code) override
    {
        memory_tracker.alloc(num_bytes);
        return llvm::sys::Memory::allocateMappedMemory(num_bytes, near_block, flags, error_code);
    }

    std::error_code protectMappedMemory(const llvm::sys::MemoryBlock & block, unsigned flags) override
    {
        return llvm::sys::Memory::protectMappedMemory(block, flags);
    }

    std::error_code releaseMappedMemory(llvm::sys::MemoryBlock & block) override
    {
        memory_tracker.free(block.size());
        return llvm::sys::Memory::releaseMappedMemory(block);
    }
};
#endif

struct LLVMContext
{
    static inline std::atomic<size_t> id_counter{0};
    llvm::LLVMContext context;
#if LLVM_VERSION_MAJOR >= 7
    llvm::orc::ExecutionSession execution_session;
    std::unique_ptr<llvm::Module> module;
#else
    std::shared_ptr<llvm::Module> module;
#endif
    std::unique_ptr<llvm::TargetMachine> machine;
#if LLVM_VERSION_MAJOR >= 6
    std::unique_ptr<CountingMMapper> memory_mapper;
#endif
    std::shared_ptr<llvm::SectionMemoryManager> memory_manager;
    llvm::orc::RTDyldObjectLinkingLayer object_layer;
    llvm::orc::IRCompileLayer<decltype(object_layer), llvm::orc::SimpleCompiler> compile_layer;
    llvm::DataLayout layout;
    llvm::IRBuilder<> builder;
    std::unordered_map<std::string, void *> symbols;
    size_t id;

    LLVMContext()
#if LLVM_VERSION_MAJOR >= 7
        : module(std::make_unique<llvm::Module>("jit", context))
#else
        : module(std::make_shared<llvm::Module>("jit", context))
#endif
        , machine(getNativeMachine())

#if LLVM_VERSION_MAJOR >= 6
        , memory_mapper(std::make_unique<CountingMMapper>())
        , memory_manager(std::make_shared<llvm::SectionMemoryManager>(memory_mapper.get()))
#else
        , memory_manager(std::make_shared<llvm::SectionMemoryManager>())
#endif
#if LLVM_VERSION_MAJOR >= 7
        , object_layer(execution_session, [this](llvm::orc::VModuleKey)
        {
            return llvm::orc::RTDyldObjectLinkingLayer::Resources{memory_manager, wrapJITSymbolResolver(*memory_manager)};
        })
#else
        , object_layer([this]() { return memory_manager; })
#endif
        , compile_layer(object_layer, llvm::orc::SimpleCompiler(*machine))
        , layout(machine->createDataLayout())
        , builder(context)
        , id(id_counter++)
    {
        module->setDataLayout(layout);
        module->setTargetTriple(machine->getTargetTriple().getTriple());
    }

    void finalize()
    {
        if (!module->size())
            return;
        llvm::PassManagerBuilder builder;
        llvm::legacy::PassManager mpm;
        llvm::legacy::FunctionPassManager fpm(module.get());
        builder.OptLevel = 3;
        builder.SLPVectorize = true;
        builder.LoopVectorize = true;
        builder.RerollLoops = true;
        builder.VerifyInput = true;
        builder.VerifyOutput = true;
        machine->adjustPassManager(builder);
        fpm.add(llvm::createTargetTransformInfoWrapperPass(machine->getTargetIRAnalysis()));
        mpm.add(llvm::createTargetTransformInfoWrapperPass(machine->getTargetIRAnalysis()));
        builder.populateFunctionPassManager(fpm);
        builder.populateModulePassManager(mpm);
        fpm.doInitialization();
        for (auto & function : *module)
            fpm.run(function);
        fpm.doFinalization();
        mpm.run(*module);

        std::vector<std::string> functions;
        functions.reserve(module->size());
        for (const auto & function : *module)
            functions.emplace_back(function.getName());

#if LLVM_VERSION_MAJOR >= 7
        llvm::orc::VModuleKey module_key = execution_session.allocateVModule();
        if (compile_layer.addModule(module_key, std::move(module)))
            throw Exception("Cannot add module to compile layer", ErrorCodes::CANNOT_COMPILE_CODE);
#else
        if (!compile_layer.addModule(module, memory_manager))
            throw Exception("Cannot add module to compile layer", ErrorCodes::CANNOT_COMPILE_CODE);
#endif

        for (const auto & name : functions)
        {
            std::string mangled_name;
            llvm::raw_string_ostream mangled_name_stream(mangled_name);
            llvm::Mangler::getNameWithPrefix(mangled_name_stream, name, layout);
            mangled_name_stream.flush();
            auto symbol = compile_layer.findSymbol(mangled_name, false);
            if (!symbol)
                continue; /// external function (e.g. an intrinsic that calls into libc)
            auto address = symbol.getAddress();
            if (!address)
                throw Exception("Function " + name + " failed to link", ErrorCodes::CANNOT_COMPILE_CODE);
            symbols[name] = reinterpret_cast<void *>(*address);
        }
    }
};

class LLVMPreparedFunction : public PreparedFunctionImpl
{
    std::string name;
    std::shared_ptr<LLVMContext> context;
    void * function;

public:
    LLVMPreparedFunction(std::string name_, std::shared_ptr<LLVMContext> context)
        : name(std::move(name_)), context(context), function(context->symbols.at(name))
    {}

    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t block_size) override
    {
        auto col_res = block.getByPosition(result).type->createColumn()->cloneResized(block_size);
        if (block_size)
        {
            std::vector<ColumnData> columns(arguments.size() + 1);
            for (size_t i = 0; i < arguments.size(); ++i)
            {
                auto * column = block.getByPosition(arguments[i]).column.get();
                if (!column)
                    throw Exception("Column " + block.getByPosition(arguments[i]).name + " is missing", ErrorCodes::LOGICAL_ERROR);
                columns[i] = getColumnData(column);
            }
            columns[arguments.size()] = getColumnData(col_res.get());
            reinterpret_cast<void (*) (size_t, ColumnData *)>(function)(block_size, columns.data());
        }
        block.getByPosition(result).column = std::move(col_res);
    }
};

static void compileFunction(std::shared_ptr<LLVMContext> & context, const IFunctionBase & f)
{
    ProfileEvents::increment(ProfileEvents::CompileFunction);

    auto & arg_types = f.getArgumentTypes();
    auto & b = context->builder;
    auto * size_type = b.getIntNTy(sizeof(size_t) * 8);
    auto * data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy(), size_type);
    auto * func_type = llvm::FunctionType::get(b.getVoidTy(), { size_type, data_type->getPointerTo() }, /*isVarArg=*/false);
    auto * func = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, f.getName(), context->module.get());
    auto args = func->args().begin();
    llvm::Value * counter_arg = &*args++;
    llvm::Value * columns_arg = &*args++;

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", func);
    b.SetInsertPoint(entry);
    std::vector<ColumnDataPlaceholder> columns(arg_types.size() + 1);
    for (size_t i = 0; i <= arg_types.size(); ++i)
    {
        auto & type = i == arg_types.size() ? f.getReturnType() : arg_types[i];
        auto * data = b.CreateLoad(b.CreateConstInBoundsGEP1_32(data_type, columns_arg, i));
        columns[i].data_init = b.CreatePointerCast(b.CreateExtractValue(data, {0}), toNativeType(b, removeNullable(type))->getPointerTo());
        columns[i].null_init = type->isNullable() ? b.CreateExtractValue(data, {1}) : nullptr;
        columns[i].stride = b.CreateExtractValue(data, {2});
    }

    /// assume nonzero initial value in `counter_arg`
    auto * loop = llvm::BasicBlock::Create(b.getContext(), "loop", func);
    b.CreateBr(loop);
    b.SetInsertPoint(loop);
    auto * counter_phi = b.CreatePHI(counter_arg->getType(), 2);
    counter_phi->addIncoming(counter_arg, entry);
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
    ValuePlaceholders arguments(arg_types.size());
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        arguments[i] = [&b, &col = columns[i], &type = arg_types[i]]() -> llvm::Value *
        {
            auto * value = b.CreateLoad(col.data);
            if (!col.null)
                return value;
            auto * is_null = b.CreateICmpNE(b.CreateLoad(col.null), b.getInt8(0));
            auto * nullable = llvm::Constant::getNullValue(toNativeType(b, type));
            return b.CreateInsertValue(b.CreateInsertValue(nullable, value, {0}), is_null, {1});
        };
    }
    auto * result = f.compile(b, std::move(arguments));
    if (columns.back().null)
    {
        b.CreateStore(b.CreateExtractValue(result, {0}), columns.back().data);
        b.CreateStore(b.CreateSelect(b.CreateExtractValue(result, {1}), b.getInt8(1), b.getInt8(0)), columns.back().null);
    }
    else
    {
        b.CreateStore(result, columns.back().data);
    }
    auto * cur_block = b.GetInsertBlock();
    for (auto & col : columns)
    {
        /// stride is either 0 or size of native type; output column is never constant; neither is at least one input
        auto * is_const = &col == &columns.back() || columns.size() <= 2 ? b.getFalse() : b.CreateICmpEQ(col.stride, llvm::ConstantInt::get(size_type, 0));
        col.data->addIncoming(b.CreateSelect(is_const, col.data, b.CreateConstInBoundsGEP1_32(nullptr, col.data, 1)), cur_block);
        if (col.null)
            col.null->addIncoming(b.CreateSelect(is_const, col.null, b.CreateConstInBoundsGEP1_32(nullptr, col.null, 1)), cur_block);
    }
    counter_phi->addIncoming(b.CreateSub(counter_phi, llvm::ConstantInt::get(size_type, 1)), cur_block);

    auto * end = llvm::BasicBlock::Create(b.getContext(), "end", func);
    b.CreateCondBr(b.CreateICmpNE(counter_phi, llvm::ConstantInt::get(size_type, 1)), loop, end);
    b.SetInsertPoint(end);
    b.CreateRetVoid();
}

static llvm::Constant * getNativeValue(llvm::Type * type, const IColumn & column, size_t i)
{
    if (!type)
        return nullptr;
    if (auto * constant = typeid_cast<const ColumnConst *>(&column))
        return getNativeValue(type, constant->getDataColumn(), 0);
    if (auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        auto * value = getNativeValue(type->getContainedType(0), nullable->getNestedColumn(), i);
        auto * is_null = llvm::ConstantInt::get(type->getContainedType(1), nullable->isNullAt(i));
        return value ? llvm::ConstantStruct::get(static_cast<llvm::StructType *>(type), value, is_null) : nullptr;
    }
    if (type->isFloatTy())
        return llvm::ConstantFP::get(type, static_cast<const ColumnVector<Float32> &>(column).getElement(i));
    if (type->isDoubleTy())
        return llvm::ConstantFP::get(type, static_cast<const ColumnVector<Float64> &>(column).getElement(i));
    if (type->isIntegerTy())
        return llvm::ConstantInt::get(type, column.getUInt(i));
    /// TODO: if (type->isVectorTy())
    return nullptr;
}

/// Same as IFunctionBase::compile, but also for constants and input columns.
using CompilableExpression = std::function<llvm::Value * (llvm::IRBuilderBase &, const ValuePlaceholders &)>;

static CompilableExpression subexpression(ColumnPtr c, DataTypePtr type)
{
    return [=](llvm::IRBuilderBase & b, const ValuePlaceholders &) { return getNativeValue(toNativeType(b, type), *c, 0); };
}

static CompilableExpression subexpression(size_t i)
{
    return [=](llvm::IRBuilderBase &, const ValuePlaceholders & inputs) { return inputs[i](); };
}

static CompilableExpression subexpression(const IFunctionBase & f, std::vector<CompilableExpression> args)
{
    return [&, args = std::move(args)](llvm::IRBuilderBase & builder, const ValuePlaceholders & inputs)
    {
        ValuePlaceholders input;
        for (const auto & arg : args)
            input.push_back([&]() { return arg(builder, inputs); });
        auto * result = f.compile(builder, input);
        if (result->getType() != toNativeType(builder, f.getReturnType()))
            throw Exception("Function " + f.getName() + " generated an llvm::Value of invalid type", ErrorCodes::LOGICAL_ERROR);
        return result;
    };
}

LLVMFunction::LLVMFunction(const ExpressionActions::Actions & actions, std::shared_ptr<LLVMContext> context, const Block & sample_block)
        : name(actions.back().result_name), context(context)
{
    for (const auto & c : sample_block)
        /// TODO: implement `getNativeValue` for all types & replace the check with `c.column && toNativeType(...)`
        if (c.column && getNativeValue(toNativeType(context->builder, c.type), *c.column, 0))
            subexpressions[c.name] = subexpression(c.column, c.type);
    for (const auto & action : actions)
    {
        const auto & names = action.argument_names;
        const auto & types = action.function->getArgumentTypes();
        std::vector<CompilableExpression> args;
        for (size_t i = 0; i < names.size(); ++i)
        {
            auto inserted = subexpressions.emplace(names[i], subexpression(arg_names.size()));
            if (inserted.second)
            {
                arg_names.push_back(names[i]);
                arg_types.push_back(types[i]);
            }
            args.push_back(inserted.first->second);
        }
        subexpressions[action.result_name] = subexpression(*action.function, std::move(args));
        originals.push_back(action.function);
    }
    compileFunction(context, *this);
}

PreparedFunctionPtr LLVMFunction::prepare(const Block &) const { return std::make_shared<LLVMPreparedFunction>(name, context); }

bool LLVMFunction::isDeterministic() const
{
    for (const auto & f : originals)
        if (!f->isDeterministic())
            return false;
    return true;
}

bool LLVMFunction::isDeterministicInScopeOfQuery() const
{
    for (const auto & f : originals)
        if (!f->isDeterministicInScopeOfQuery())
            return false;
    return true;
}

bool LLVMFunction::isSuitableForConstantFolding() const
{
    for (const auto & f : originals)
        if (!f->isSuitableForConstantFolding())
            return false;
    return true;
}

bool LLVMFunction::isInjective(const Block & sample_block)
{
    for (const auto & f : originals)
        if (!f->isInjective(sample_block))
            return false;
    return true;
}

bool LLVMFunction::hasInformationAboutMonotonicity() const
{
    for (const auto & f : originals)
        if (!f->hasInformationAboutMonotonicity())
            return false;
    return true;
}

LLVMFunction::Monotonicity LLVMFunction::getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const
{
    const IDataType * type_ = &type;
    Field left_ = left;
    Field right_ = right;
    Monotonicity result(true, true, true);
    /// monotonicity is only defined for unary functions, so the chain must describe a sequence of nested calls
    for (size_t i = 0; i < originals.size(); ++i)
    {
        Monotonicity m = originals[i]->getMonotonicityForRange(*type_, left_, right_);
        if (!m.is_monotonic)
            return m;
        result.is_positive ^= !m.is_positive;
        result.is_always_monotonic &= m.is_always_monotonic;
        if (i + 1 < originals.size())
        {
            if (left_ != Field())
                applyFunction(*originals[i], left_);
            if (right_ != Field())
                applyFunction(*originals[i], right_);
            if (!m.is_positive)
                std::swap(left_, right_);
            type_ = originals[i]->getReturnType().get();
        }
    }
    return result;
}


static bool isCompilable(llvm::IRBuilderBase & builder, const IFunctionBase & function)
{
    if (!toNativeType(builder, function.getReturnType()))
        return false;
    for (const auto & type : function.getArgumentTypes())
        if (!toNativeType(builder, type))
            return false;
    return function.isCompilable();
}

size_t CompiledExpressionCache::weight() const
{

#if LLVM_VERSION_MAJOR >= 6
    std::lock_guard<std::mutex> lock(mutex);
    size_t result{0};
    std::unordered_set<size_t> seen;
    for (const auto & cell : cells)
    {
        auto function_context = cell.second.value->getContext();
        if (!seen.count(function_context->id))
        {
            result += function_context->memory_mapper->memory_tracker.get();
            seen.insert(function_context->id);
        }
    }
    return result;
#else
    return Base::weight();
#endif
}

void compileFunctions(ExpressionActions::Actions & actions, const Names & output_columns, const Block & sample_block, std::shared_ptr<CompiledExpressionCache> compilation_cache)
{
    struct LLVMTargetInitializer
    {
        LLVMTargetInitializer()
        {
            llvm::InitializeNativeTarget();
            llvm::InitializeNativeTargetAsmPrinter();
            llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
        }
    };

    static LLVMTargetInitializer initializer;

    auto context = std::make_shared<LLVMContext>();
    /// an empty optional is a poisoned value prohibiting the column's producer from being removed
    /// (which it could be, if it was inlined into every dependent function).
    std::unordered_map<std::string, std::unordered_set<std::optional<size_t>>> current_dependents;
    for (const auto & name : output_columns)
        current_dependents[name].emplace();
    /// a snapshot of each compilable function's dependents at the time of its execution.
    std::vector<std::unordered_set<std::optional<size_t>>> dependents(actions.size());
    for (size_t i = actions.size(); i--;)
    {
        switch (actions[i].type)
        {
            case ExpressionAction::REMOVE_COLUMN:
                current_dependents.erase(actions[i].source_name);
                /// poison every other column used after this point so that inlining chains do not cross it.
                for (auto & dep : current_dependents)
                    dep.second.emplace();
                break;

            case ExpressionAction::PROJECT:
                current_dependents.clear();
                for (const auto & proj : actions[i].projection)
                    current_dependents[proj.first].emplace();
                break;

            case ExpressionAction::ADD_ALIASES:
                for (const auto & proj : actions[i].projection)
                    current_dependents[proj.first].emplace();
                break;

            case ExpressionAction::ADD_COLUMN:
            case ExpressionAction::COPY_COLUMN:
            case ExpressionAction::ARRAY_JOIN:
            case ExpressionAction::JOIN:
            {
                Names columns = actions[i].getNeededColumns();
                for (const auto & column : columns)
                    current_dependents[column].emplace();
                break;
            }

            case ExpressionAction::APPLY_FUNCTION:
            {
                dependents[i] = current_dependents[actions[i].result_name];
                const bool compilable = isCompilable(context->builder, *actions[i].function);
                for (const auto & name : actions[i].argument_names)
                {
                    if (compilable)
                        current_dependents[name].emplace(i);
                    else
                        current_dependents[name].emplace();
                }
                break;
            }
        }
    }

    std::vector<ExpressionActions::Actions> fused(actions.size());
    for (size_t i = 0; i < actions.size(); ++i)
    {
        if (actions[i].type != ExpressionAction::APPLY_FUNCTION || !isCompilable(context->builder, *actions[i].function))
            continue;

        fused[i].push_back(actions[i]);
        if (dependents[i].find({}) != dependents[i].end())
        {
            /// the result of compiling one function in isolation is pretty much the same as its `execute` method.
            if (fused[i].size() == 1)
                continue;

            std::shared_ptr<LLVMFunction> fn;
            if (compilation_cache)
            {
                bool success;
                auto set_func = [&fused, i, context, &sample_block] () { return std::make_shared<LLVMFunction>(fused[i], context, sample_block); };
                Stopwatch watch;
                std::tie(fn, success) = compilation_cache->getOrSet(fused[i], set_func);
                if (success)
                    ProfileEvents::increment(ProfileEvents::CompileExpressionsMicroseconds, watch.elapsedMicroseconds());
            }
            else
            {
                Stopwatch watch;
                fn = std::make_shared<LLVMFunction>(fused[i], context, sample_block);
                ProfileEvents::increment(ProfileEvents::CompileExpressionsMicroseconds, watch.elapsedMicroseconds());
            }

            actions[i].function = fn;
            actions[i].argument_names = fn->getArgumentNames();
            actions[i].is_function_compiled = true;

            continue;
        }

        /// TODO: determine whether it's profitable to inline the function if there's more than one dependent.
        for (const auto & dep : dependents[i])
            fused[*dep].insert(fused[*dep].end(), fused[i].begin(), fused[i].end());
    }

    context->finalize();
}

}

#endif
