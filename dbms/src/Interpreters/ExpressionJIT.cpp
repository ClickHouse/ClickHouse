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
#if 0
template <typename It>
static void unpack(It it, It end)
{
    if (it != end)
        throw std::invalid_argument("unpacked range contains excess elements");
}

template <typename It, typename H, typename... T>
static void unpack(It it, It end, H& h, T&... t)
{
    if (it == end)
        throw std::invalid_argument("unpacked range does not contain enough elements");
    h = *it;
    unpack(++it, t...);
}
#endif
std::shared_ptr<LLVMFunction> LLVMFunction::create(ExpressionActions::Actions actions, LLVMSharedDataPtr context)
{
    Names arg_names;
    DataTypes arg_types;
    std::unordered_map<std::string, size_t> arg_index;
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
                arg_index[names[i]] = arg_names.size();
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

    auto & name = actions.back().result_name;
    auto char_ptr = llvm::PointerType::getUnqual(context->builder.getInt8Ty());
    auto void_ptr = llvm::PointerType::getUnqual(context->builder.getVoidTy());
    auto void_ptr_ptr = llvm::PointerType::getUnqual(void_ptr);
    auto func_type = llvm::FunctionType::get(context->builder.getDoubleTy(), {void_ptr_ptr, char_ptr, void_ptr}, /*isVarArg=*/false);
    auto func = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, name);
//    llvm::Argument * in_arg, is_const_arg, out_arg;
//    unpack(func->args().begin(), func->args().end(), in_arg, is_const_arg, out_arg);
    context->builder.SetInsertPoint(llvm::BasicBlock::Create(context->context, name, func));
    // TODO: cast each element of void** to corresponding native type
    for (const auto & action : actions)
    {
        // TODO: generate code to fill the next entry
        if (auto * val = action.function->compile(context->builder, {}))
            context->builder.CreateRet(val);
        else
            return nullptr;
    }
    // TODO: increment each pointer if column is not constant then loop
    func->print(llvm::errs());
    // context->module->add(func); or something like this, don't know the api
    // return std::make_shared<LLVMFunction>(std::move(actions), std::move(arg_names), std::move(arg_types), context);
    return nullptr;
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
