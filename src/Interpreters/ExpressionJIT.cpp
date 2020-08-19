#include <Interpreters/ExpressionJIT.h>

#if USE_EMBEDDED_COMPILER

#include <optional>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Common/LRUCache.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Native.h>
#include <Functions/IFunctionAdaptors.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wnon-virtual-dtor"

#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Mangler.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/ExecutionEngine/Orc/CompileUtils.h>
#include <llvm/ExecutionEngine/Orc/IRCompileLayer.h>
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <llvm/Target/TargetMachine.h>
#include <llvm/MC/SubtargetFeature.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/Host.h>
#include <llvm/Support/TargetRegistry.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>

#pragma GCC diagnostic pop

/// 'LegacyRTDyldObjectLinkingLayer' is deprecated: ORCv1 layers (layers with the 'Legacy' prefix) are deprecated. Please use ORCv2
/// 'LegacyIRCompileLayer' is deprecated: ORCv1 layers (layers with the 'Legacy' prefix) are deprecated. Please use the ORCv2 IRCompileLayer instead
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"


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
    extern const int CANNOT_COMPILE_CODE;
}

namespace
{
    struct ColumnData
    {
        const char * data = nullptr;
        const char * null = nullptr;
        size_t stride = 0;
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
    const bool is_const = isColumnConst(*column);
    if (is_const)
        column = &reinterpret_cast<const ColumnConst *>(column)->getDataColumn();
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(column))
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
    const auto * target = llvm::TargetRegistry::lookupTarget(triple, error);
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
        llvm::None, llvm::CodeGenOpt::Default, /*jit=*/true
    );
}


struct SymbolResolver : public llvm::orc::SymbolResolver
{
    llvm::LegacyJITSymbolResolver & impl;

    explicit SymbolResolver(llvm::LegacyJITSymbolResolver & impl_) : impl(impl_) {}

    llvm::orc::SymbolNameSet getResponsibilitySet(const llvm::orc::SymbolNameSet & symbols) final
    {
        return symbols;
    }

    llvm::orc::SymbolNameSet lookup(std::shared_ptr<llvm::orc::AsynchronousSymbolQuery> query, llvm::orc::SymbolNameSet symbols) final
    {
        llvm::orc::SymbolNameSet missing;
        for (const auto & symbol : symbols)
        {
            bool has_resolved = false;
            impl.lookup({*symbol}, [&](llvm::Expected<llvm::JITSymbolResolver::LookupResult> resolved)
            {
                if (resolved && !resolved->empty())
                {
                    query->notifySymbolMetRequiredState(symbol, resolved->begin()->second);
                    has_resolved = true;
                }
            });

            if (!has_resolved)
                missing.insert(symbol);
        }
        return missing;
    }
};


struct LLVMContext
{
    std::shared_ptr<llvm::LLVMContext> context {std::make_shared<llvm::LLVMContext>()};
    std::unique_ptr<llvm::Module> module {std::make_unique<llvm::Module>("jit", *context)};
    std::unique_ptr<llvm::TargetMachine> machine {getNativeMachine()};
    llvm::DataLayout layout {machine->createDataLayout()};
    llvm::IRBuilder<> builder {*context};

    llvm::orc::ExecutionSession execution_session;

    std::shared_ptr<llvm::SectionMemoryManager> memory_manager;
    llvm::orc::LegacyRTDyldObjectLinkingLayer object_layer;
    llvm::orc::LegacyIRCompileLayer<decltype(object_layer), llvm::orc::SimpleCompiler> compile_layer;

    std::unordered_map<std::string, void *> symbols;

    LLVMContext()
        : memory_manager(std::make_shared<llvm::SectionMemoryManager>())
        , object_layer(execution_session, [this](llvm::orc::VModuleKey)
        {
            return llvm::orc::LegacyRTDyldObjectLinkingLayer::Resources{memory_manager, std::make_shared<SymbolResolver>(*memory_manager)};
        })
        , compile_layer(object_layer, llvm::orc::SimpleCompiler(*machine))
    {
        module->setDataLayout(layout);
        module->setTargetTriple(machine->getTargetTriple().getTriple());
    }

    /// returns used memory
    void compileAllFunctionsToNativeCode()
    {
        if (module->empty())
            return;
        llvm::PassManagerBuilder pass_manager_builder;
        llvm::legacy::PassManager mpm;
        llvm::legacy::FunctionPassManager fpm(module.get());
        pass_manager_builder.OptLevel = 3;
        pass_manager_builder.SLPVectorize = true;
        pass_manager_builder.LoopVectorize = true;
        pass_manager_builder.RerollLoops = true;
        pass_manager_builder.VerifyInput = true;
        pass_manager_builder.VerifyOutput = true;
        machine->adjustPassManager(pass_manager_builder);
        fpm.add(llvm::createTargetTransformInfoWrapperPass(machine->getTargetIRAnalysis()));
        mpm.add(llvm::createTargetTransformInfoWrapperPass(machine->getTargetIRAnalysis()));
        pass_manager_builder.populateFunctionPassManager(fpm);
        pass_manager_builder.populateModulePassManager(mpm);
        fpm.doInitialization();
        for (auto & function : *module)
            fpm.run(function);
        fpm.doFinalization();
        mpm.run(*module);

        std::vector<std::string> functions;
        functions.reserve(module->size());
        for (const auto & function : *module)
            functions.emplace_back(function.getName());

        llvm::orc::VModuleKey module_key = execution_session.allocateVModule();
        if (compile_layer.addModule(module_key, std::move(module)))
            throw Exception("Cannot add module to compile layer", ErrorCodes::CANNOT_COMPILE_CODE);

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


template <typename... Ts>
static bool castToEitherWithNullable(IColumn * column)
{
    return ((typeid_cast<Ts *>(column)
            || (typeid_cast<ColumnNullable *>(column) && typeid_cast<Ts *>(&(typeid_cast<ColumnNullable *>(column)->getNestedColumn())))) || ...);
}

class LLVMExecutableFunction : public IExecutableFunctionImpl
{
    std::string name;
    void * function;

public:
    LLVMExecutableFunction(const std::string & name_, const std::unordered_map<std::string, void *> & symbols)
        : name(name_)
    {
        auto it = symbols.find(name);
        if (symbols.end() == it)
            throw Exception("Cannot find symbol " + name + " in LLVMContext", ErrorCodes::LOGICAL_ERROR);
        function = it->second;
    }

    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    void execute(Block & block, const ColumnNumbers & arguments, size_t result, size_t block_size) override
    {
        auto col_res = block.getByPosition(result).type->createColumn();

        if (block_size)
        {
            if (!castToEitherWithNullable<
                ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64,
                ColumnInt8, ColumnInt16, ColumnInt32, ColumnInt64,
                ColumnFloat32, ColumnFloat64>(col_res.get()))
                throw Exception("Unexpected column in LLVMExecutableFunction: " + col_res->getName(), ErrorCodes::LOGICAL_ERROR);
            col_res = col_res->cloneResized(block_size);
            std::vector<ColumnData> columns(arguments.size() + 1);
            for (size_t i = 0; i < arguments.size(); ++i)
            {
                const auto * column = block.getByPosition(arguments[i]).column.get();
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

static void compileFunctionToLLVMByteCode(LLVMContext & context, const IFunctionBaseImpl & f)
{
    ProfileEvents::increment(ProfileEvents::CompileFunction);

    const auto & arg_types = f.getArgumentTypes();
    auto & b = context.builder;
    auto * size_type = b.getIntNTy(sizeof(size_t) * 8);
    auto * data_type = llvm::StructType::get(b.getInt8PtrTy(), b.getInt8PtrTy(), size_type);
    auto * func_type = llvm::FunctionType::get(b.getVoidTy(), { size_type, data_type->getPointerTo() }, /*isVarArg=*/false);
    auto * func = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, f.getName(), context.module.get());
    auto * args = func->args().begin();
    llvm::Value * counter_arg = &*args++;
    llvm::Value * columns_arg = &*args++;

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", func);
    b.SetInsertPoint(entry);
    std::vector<ColumnDataPlaceholder> columns(arg_types.size() + 1);
    for (size_t i = 0; i <= arg_types.size(); ++i)
    {
        const auto & type = i == arg_types.size() ? f.getReturnType() : arg_types[i];
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
    for (size_t i = 0; i < arguments.size(); ++i) // NOLINT
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
    if (!type || column.size() <= i)
        return nullptr;
    if (const auto * constant = typeid_cast<const ColumnConst *>(&column))
        return getNativeValue(type, constant->getDataColumn(), 0);
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        auto * value = getNativeValue(type->getContainedType(0), nullable->getNestedColumn(), i);
        auto * is_null = llvm::ConstantInt::get(type->getContainedType(1), nullable->isNullAt(i));
        return value ? llvm::ConstantStruct::get(static_cast<llvm::StructType *>(type), value, is_null) : nullptr;
    }
    if (type->isFloatTy())
        return llvm::ConstantFP::get(type, assert_cast<const ColumnVector<Float32> &>(column).getElement(i));
    if (type->isDoubleTy())
        return llvm::ConstantFP::get(type, assert_cast<const ColumnVector<Float64> &>(column).getElement(i));
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

struct LLVMModuleState
{
    std::unordered_map<std::string, void *> symbols;
    std::shared_ptr<llvm::LLVMContext> major_context;
    std::shared_ptr<llvm::SectionMemoryManager> memory_manager;
};

LLVMFunction::LLVMFunction(const ExpressionActions::Actions & actions, const Block & sample_block)
    : name(actions.back().result_name)
    , module_state(std::make_unique<LLVMModuleState>())
{
    LLVMContext context;
    for (const auto & c : sample_block)
        /// TODO: implement `getNativeValue` for all types & replace the check with `c.column && toNativeType(...)`
        if (c.column && getNativeValue(toNativeType(context.builder, c.type), *c.column, 0))
            subexpressions[c.name] = subexpression(c.column, c.type);
    for (const auto & action : actions)
    {
        const auto & names = action.argument_names;
        const auto & types = action.function_base->getArgumentTypes();
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
        subexpressions[action.result_name] = subexpression(*action.function_base, std::move(args));
        originals.push_back(action.function_base);
    }
    compileFunctionToLLVMByteCode(context, *this);
    context.compileAllFunctionsToNativeCode();

    module_state->symbols = context.symbols;
    module_state->major_context = context.context;
    module_state->memory_manager = context.memory_manager;
}

llvm::Value * LLVMFunction::compile(llvm::IRBuilderBase & builder, ValuePlaceholders values) const
{
    auto it = subexpressions.find(name);
    if (subexpressions.end() == it)
        throw Exception("Cannot find subexpression " + name + " in LLVMFunction", ErrorCodes::LOGICAL_ERROR);
    return it->second(builder, values);
}

ExecutableFunctionImplPtr LLVMFunction::prepare(const Block &, const ColumnNumbers &, size_t) const { return std::make_unique<LLVMExecutableFunction>(name, module_state->symbols); }

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

bool LLVMFunction::isInjective(const Block & sample_block) const
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
    const IDataType * type_ptr = &type;
    Field left_mut = left;
    Field right_mut = right;
    Monotonicity result(true, true, true);
    /// monotonicity is only defined for unary functions, so the chain must describe a sequence of nested calls
    for (size_t i = 0; i < originals.size(); ++i)
    {
        Monotonicity m = originals[i]->getMonotonicityForRange(*type_ptr, left_mut, right_mut);
        if (!m.is_monotonic)
            return m;
        result.is_positive ^= !m.is_positive;
        result.is_always_monotonic &= m.is_always_monotonic;
        if (i + 1 < originals.size())
        {
            if (left_mut != Field())
                applyFunction(*originals[i], left_mut);
            if (right_mut != Field())
                applyFunction(*originals[i], right_mut);
            if (!m.is_positive)
                std::swap(left_mut, right_mut);
            type_ptr = originals[i]->getReturnType().get();
        }
    }
    return result;
}


static bool isCompilable(const IFunctionBase & function)
{
    if (!canBeNativeType(*function.getReturnType()))
        return false;
    for (const auto & type : function.getArgumentTypes())
        if (!canBeNativeType(*type))
            return false;
    return function.isCompilable();
}

static std::vector<std::unordered_set<std::optional<size_t>>> getActionsDependents(const ExpressionActions::Actions & actions, const Names & output_columns)
{
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
                const bool compilable = isCompilable(*actions[i].function_base);
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
    return dependents;
}

void compileFunctions(
    ExpressionActions::Actions & actions,
    const Names & output_columns,
    const Block & sample_block,
    std::shared_ptr<CompiledExpressionCache> compilation_cache,
    size_t min_count_to_compile_expression)
{
    static std::unordered_map<UInt128, UInt32, UInt128Hash> counter;
    static std::mutex mutex;

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

    auto dependents = getActionsDependents(actions, output_columns);
    std::vector<ExpressionActions::Actions> fused(actions.size());
    for (size_t i = 0; i < actions.size(); ++i)
    {
        if (actions[i].type != ExpressionAction::APPLY_FUNCTION || !isCompilable(*actions[i].function_base))
            continue;

        fused[i].push_back(actions[i]);
        if (dependents[i].find({}) != dependents[i].end())
        {
            /// the result of compiling one function in isolation is pretty much the same as its `execute` method.
            if (fused[i].size() == 1)
                continue;

            auto hash_key = ExpressionActions::ActionsHash{}(fused[i]);
            {
                std::lock_guard lock(mutex);
                if (counter[hash_key]++ < min_count_to_compile_expression)
                    continue;
            }

            FunctionBasePtr fn;
            if (compilation_cache)
            {
                std::tie(fn, std::ignore) = compilation_cache->getOrSet(hash_key, [&inlined_func=std::as_const(fused[i]), &sample_block] ()
                {
                    Stopwatch watch;
                    FunctionBasePtr result_fn;
                    result_fn = std::make_shared<FunctionBaseAdaptor>(std::make_unique<LLVMFunction>(inlined_func, sample_block));
                    ProfileEvents::increment(ProfileEvents::CompileExpressionsMicroseconds, watch.elapsedMicroseconds());
                    return result_fn;
                });
            }
            else
            {
                Stopwatch watch;
                fn = std::make_shared<FunctionBaseAdaptor>(std::make_unique<LLVMFunction>(fused[i], sample_block));
                ProfileEvents::increment(ProfileEvents::CompileExpressionsMicroseconds, watch.elapsedMicroseconds());
            }

            actions[i].function_base = fn;
            actions[i].argument_names = typeid_cast<const LLVMFunction *>(typeid_cast<const FunctionBaseAdaptor *>(fn.get())->getImpl())->getArgumentNames();
            actions[i].is_function_compiled = true;

            continue;
        }

        /// TODO: determine whether it's profitable to inline the function if there's more than one dependent.
        for (const auto & dep : dependents[i])
            fused[*dep].insert(fused[*dep].end(), fused[i].begin(), fused[i].end());
    }

    for (auto & action : actions)
    {
        if (action.type == ExpressionAction::APPLY_FUNCTION && action.is_function_compiled)
            action.function = action.function_base->prepare({}, {}, 0); /// Arguments are not used for LLVMFunction.
    }
}

}

#endif
