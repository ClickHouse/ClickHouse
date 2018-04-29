#include <Interpreters/ExpressionJIT.h>

#if USE_EMBEDDED_COMPILER

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Native.h>
#include <Functions/IFunction.h>

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
    function.execute(block, {0}, 1);
    block.safeGetByPosition(1).column->get(0, value);
}

struct LLVMContext
{
    llvm::LLVMContext context;
    std::shared_ptr<llvm::Module> module;
    std::unique_ptr<llvm::TargetMachine> machine;
    llvm::orc::RTDyldObjectLinkingLayer objectLayer;
    llvm::orc::IRCompileLayer<decltype(objectLayer), llvm::orc::SimpleCompiler> compileLayer;
    llvm::DataLayout layout;
    llvm::IRBuilder<> builder;

    LLVMContext()
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

    void finalize()
    {
        if (!module->size())
            return;
        llvm::PassManagerBuilder builder;
        llvm::legacy::FunctionPassManager fpm(module.get());
        builder.OptLevel = 3;
        builder.SLPVectorize = true;
        builder.LoopVectorize = true;
        builder.RerollLoops = true;
        builder.VerifyInput = true;
        builder.VerifyOutput = true;
        builder.populateFunctionPassManager(fpm);
        for (auto & function : *module)
            fpm.run(function);
        llvm::cantFail(compileLayer.addModule(module, std::make_shared<llvm::orc::NullResolver>()));
    }
};

class LLVMPreparedFunction : public IPreparedFunction
{
    std::string name;
    std::shared_ptr<LLVMContext> context;
    const void * function;

public:
    LLVMPreparedFunction(std::string name_, std::shared_ptr<LLVMContext> context)
        : name(std::move(name_)), context(context)
    {
        std::string mangledName;
        llvm::raw_string_ostream mangledNameStream(mangledName);
        llvm::Mangler::getNameWithPrefix(mangledNameStream, name, context->layout);
        function = reinterpret_cast<const void *>(context->compileLayer.findSymbol(mangledNameStream.str(), false).getAddress().get());
    }

    String getName() const override { return name; }

    void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        size_t block_size = block.rows();
        auto col_res = block.getByPosition(result).type->createColumn()->cloneResized(block_size);
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
};

class LLVMFunction : public IFunctionBase
{
    /// all actions must have type APPLY_FUNCTION
    ExpressionActions::Actions actions;
    Names arg_names;
    DataTypes arg_types;
    std::shared_ptr<LLVMContext> context;

public:
    LLVMFunction(ExpressionActions::Actions actions_, std::shared_ptr<LLVMContext> context, const Block & sample_block)
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
                columns_v[i].null_init = b.CreateLoad(b.CreateConstInBoundsGEP2_32(data_type, columns, i, 1));
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
            {
                auto * is_const = b.CreateICmpEQ(col.stride, llvm::ConstantInt::get(size_type, 0));
                col.null->addIncoming(b.CreateSelect(is_const, col.null, b.CreateConstInBoundsGEP1_32(b.getInt8Ty(), col.null, 1)), cur_block);
            }
        }
        counter_phi->addIncoming(b.CreateSub(counter_phi, llvm::ConstantInt::get(size_type, 1)), cur_block);

        auto * end = llvm::BasicBlock::Create(context->context, "end", func);
        b.CreateCondBr(b.CreateICmpNE(counter_phi, llvm::ConstantInt::get(size_type, 1)), loop, end);
        b.SetInsertPoint(end);
        b.CreateRetVoid();
    }

    String getName() const override { return actions.back().result_name; }

    const Names & getArgumentNames() const { return arg_names; }

    const DataTypes & getArgumentTypes() const override { return arg_types; }

    const DataTypePtr & getReturnType() const override { return actions.back().function->getReturnType(); }

    PreparedFunctionPtr prepare(const Block &) const override { return std::make_shared<LLVMPreparedFunction>(getName(), context); }

    bool isDeterministic() override
    {
        for (const auto & action : actions)
            if (!action.function->isDeterministic())
                return false;
        return true;
    }

    bool isDeterministicInScopeOfQuery() override
    {
        for (const auto & action : actions)
            if (!action.function->isDeterministicInScopeOfQuery())
                return false;
        return true;
    }

    bool isSuitableForConstantFolding() const override
    {
        for (const auto & action : actions)
            if (!action.function->isSuitableForConstantFolding())
                return false;
        return true;
    }

    bool isInjective(const Block & sample_block) override
    {
        for (const auto & action : actions)
            if (!action.function->isInjective(sample_block))
                return false;
        return true;
    }

    bool hasInformationAboutMonotonicity() const override
    {
        for (const auto & action : actions)
            if (!action.function->hasInformationAboutMonotonicity())
                return false;
        return true;
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
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
};

static bool isCompilable(llvm::IRBuilderBase & builder, const IFunctionBase& function)
{
    if (!toNativeType(builder, function.getReturnType()))
        return false;
    for (const auto & type : function.getArgumentTypes())
        if (!toNativeType(builder, type))
            return false;
    return function.isCompilable();
}

void compileFunctions(ExpressionActions::Actions & actions, const Names & output_columns, const Block & sample_block)
{
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
    for (size_t i = 0; i < actions.size(); i++)
    {
        if (actions[i].type != ExpressionAction::APPLY_FUNCTION || !isCompilable(context->builder, *actions[i].function))
            continue;
        if (dependents[i].find({}) != dependents[i].end())
        {
            fused[i].push_back(actions[i]);
            auto fn = std::make_shared<LLVMFunction>(std::move(fused[i]), context, sample_block);
            actions[i].function = fn;
            actions[i].argument_names = fn->getArgumentNames();
            continue;
        }
        /// TODO: determine whether it's profitable to inline the function if there's more than one dependent.
        for (const auto & dep : dependents[i])
            fused[*dep].push_back(actions[i]);
    }
    context->finalize();
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
