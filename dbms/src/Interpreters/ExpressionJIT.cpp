#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ExpressionJIT.h>

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

#include <stdexcept>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct LLVMSharedData
{
    llvm::LLVMContext context;
    std::shared_ptr<llvm::Module> module;
    std::unique_ptr<llvm::TargetMachine> machine;
    llvm::orc::RTDyldObjectLinkingLayer objectLayer;
    llvm::orc::IRCompileLayer<decltype(objectLayer), llvm::orc::SimpleCompiler> compileLayer;
    llvm::DataLayout layout;
    llvm::IRBuilder<> builder;

    LLVMSharedData()
        : module(std::make_shared<llvm::Module>("jit", context))
        , machine(llvm::EngineBuilder().selectTarget())
        , objectLayer([]() { return std::make_shared<llvm::SectionMemoryManager>(); })
        , compileLayer(objectLayer, llvm::orc::SimpleCompiler(*machine))
        , layout(machine->createDataLayout())
        , builder(context)
    {
        module->setDataLayout(layout);
        module->setTargetTriple(machine->getTargetTriple().getTriple());
        // TODO: throw in some optimization & verification layers
    }

    llvm::Type * toNativeType(const DataTypePtr & type)
    {
        // LLVM doesn't have unsigned types, it has unsigned instructions.
        if (type->equals(DataTypeInt8{}) || type->equals(DataTypeUInt8{}))
            return builder.getInt8Ty();
        if (type->equals(DataTypeInt16{}) || type->equals(DataTypeUInt16{}))
            return builder.getInt16Ty();
        if (type->equals(DataTypeInt32{}) || type->equals(DataTypeUInt32{}))
            return builder.getInt32Ty();
        if (type->equals(DataTypeInt64{}) || type->equals(DataTypeUInt64{}))
            return builder.getInt64Ty();
        if (type->equals(DataTypeFloat32{}))
            return builder.getFloatTy();
        if (type->equals(DataTypeFloat64{}))
            return builder.getDoubleTy();
        return nullptr;
    }

    void finalize()
    {
        if (module->size())
            llvm::cantFail(compileLayer.addModule(module, std::make_shared<llvm::orc::NullResolver>()));
    }

    LLVMCompiledFunction * lookup(const std::string& name)
    {
        std::string mangledName;
        llvm::raw_string_ostream mangledNameStream(mangledName);
        llvm::Mangler::getNameWithPrefix(mangledNameStream, name, layout);
        // why is `findSymbol` not const? we may never know.
        return reinterpret_cast<LLVMCompiledFunction *>(compileLayer.findSymbol(mangledNameStream.str(), false).getAddress().get());
    }
};

LLVMSharedDataPtr::LLVMSharedDataPtr()
    : std::shared_ptr<LLVMSharedData>(std::make_shared<LLVMSharedData>())
{}

void LLVMSharedDataPtr::finalize()
{
    (*this)->finalize();
}

LLVMPreparedFunction::LLVMPreparedFunction(LLVMSharedDataPtr context, std::shared_ptr<const IFunctionBase> parent)
    : parent(parent), context(context), function(context->lookup(parent->getName()))
{}

std::shared_ptr<LLVMFunction> LLVMFunction::create(ExpressionActions::Actions actions, LLVMSharedDataPtr context)
{
    Names arg_names;
    DataTypes arg_types;
    std::unordered_set<std::string> seen;
    for (const auto & action : actions)
        seen.insert(action.result_name);
    for (const auto & action : actions)
    {
        const auto & names = action.argument_names;
        const auto & types = action.function->getArgumentTypes();
        for (size_t i = 0; i < names.size(); i++)
        {
            if (seen.emplace(names[i]).second)
            {
                arg_names.push_back(names[i]);
                arg_types.push_back(types[i]);
            }
        }
    }

    std::vector<llvm::Type *> native_types(arg_types.size());
    for (size_t i = 0; i < arg_types.size(); i++)
        if (!(native_types[i] = context->toNativeType(arg_types[i])))
            return nullptr;
    llvm::Type * return_type = context->toNativeType(actions.back().function->getReturnType());
    if (!return_type)
        return nullptr;

    llvm::FunctionType * func_type = llvm::FunctionType::get(context->builder.getVoidTy(), {
        llvm::PointerType::getUnqual(llvm::PointerType::getUnqual(context->builder.getVoidTy())),
        llvm::PointerType::getUnqual(context->builder.getInt8Ty()),
        llvm::PointerType::getUnqual(return_type),
    }, /*isVarArg=*/false);
    std::unique_ptr<llvm::Function> func{llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, actions.back().result_name)};
    context->builder.SetInsertPoint(llvm::BasicBlock::Create(context->context, "entry", func.get()));

    // prologue: cast each input column to appropriate type
    auto args = func->args().begin();
    llvm::Value * in_arg = &*args++;
    llvm::Value * is_const_arg = &*args++;
    llvm::Value * out_arg = &*args++;
    std::unordered_map<std::string, llvm::Value *> by_name;
    for (size_t i = 0; i < native_types.size(); i++)
    {
        // not sure if this is the correct ir instruction
        llvm::Value * ptr = i ? context->builder.CreateConstGEP1_32(in_arg, i) : in_arg;
        ptr = context->builder.CreateLoad(ptr);
        ptr = context->builder.CreatePointerCast(ptr, llvm::PointerType::getUnqual(native_types[i]));
        if (!by_name.emplace(arg_names[i], context->builder.CreateLoad(ptr)).second)
            throw Exception("duplicate input column name", ErrorCodes::LOGICAL_ERROR);
    }

    // main loop over the columns
    (void)is_const_arg;
    for (const auto & action : actions)
    {
        ValuePlaceholders inputs;
        inputs.reserve(action.argument_names.size());
        for (const auto & name : action.argument_names)
            inputs.push_back(by_name.at(name));
        llvm::Value * val = action.function->compile(context->builder, inputs);
        if (!val)
            // TODO: separate checks from compilation
            return nullptr;
        if (!by_name.emplace(action.result_name, val).second)
            throw Exception("duplicate action result name", ErrorCodes::LOGICAL_ERROR);
    }
    context->builder.CreateStore(by_name.at(actions.back().result_name), out_arg);
    context->builder.CreateRetVoid();
    // TODO: increment each pointer if column is not constant then loop

    func->print(llvm::errs());
    context->module->getFunctionList().push_back(func.release());
    return std::make_shared<LLVMFunction>(std::move(actions), std::move(arg_names), std::move(arg_types), context);
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
};

}

static LLVMTargetInitializer llvmInitializer;
