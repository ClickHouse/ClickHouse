#include <iostream>

#include <unordered_map>

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

static llvm::TargetMachine * getNativeMachine()
{
    std::string error;
    auto cpu = llvm::sys::getHostCPUName();
    auto triple = llvm::sys::getProcessTriple();
    const auto * target = llvm::TargetRegistry::lookupTarget(triple, error);
    if (!target)
    {
        std::cerr << "No target " << error << std::endl;
        std::terminate();
    }

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

void test_function()
{
    std::cerr << "TestFunction" << std::endl;
}

namespace {
// Trivial implementation of SectionMemoryManager::MemoryMapper that just calls
// into sys::Memory.
class CustomMMapper final : public llvm::SectionMemoryManager::MemoryMapper {
public:
  llvm::sys::MemoryBlock
  allocateMappedMemory(llvm::SectionMemoryManager::AllocationPurpose Purpose,
                       size_t NumBytes, const llvm::sys::MemoryBlock *const NearBlock,
                       unsigned Flags, std::error_code &EC) override {
    (void)(Purpose);
    auto result_block = llvm::sys::Memory::allocateMappedMemory(NumBytes, NearBlock, Flags, EC);

    // std::cerr << "CustomMMapper::allocateMappedMemory " << NumBytes << " result block " << result_block.base();
    // std::cerr << " allocated size " << result_block.allocatedSize() << std::endl;

    return result_block;
  }

  std::error_code protectMappedMemory(const llvm::sys::MemoryBlock &Block,
                                      unsigned Flags) override {
    // std::cerr << "CustomMMapper::protectMappedMemory " << Block.base() << " " << Block.allocatedSize() << std::endl;
    return llvm::sys::Memory::protectMappedMemory(Block, Flags);
  }

  std::error_code releaseMappedMemory(llvm::sys::MemoryBlock &M) override {
    // std::cerr << "CustomMMapper::releaseMappedMemory " << M.base() << " size " << M.allocatedSize() << std::endl;
    return llvm::sys::Memory::releaseMappedMemory(M);
  }
};

}

CustomMMapper DefaultMMapperInstance;

class CustomMemoryManager : public llvm::SectionMemoryManager
{
public:
    CustomMemoryManager() : llvm::SectionMemoryManager(&DefaultMMapperInstance)
    {
        // std::cerr << "CustomMemoryManager::constructor" << std::endl;
    }

    uint8_t *allocateCodeSection(uintptr_t Size, unsigned Alignment,
                               unsigned SectionID,
                               llvm::StringRef SectionName) override
    {
        // std::cerr << "CustomMemoryManager::allocateCodeSection " << Size << " " << Alignment << std::endl;
        return llvm::SectionMemoryManager::allocateCodeSection(Size, Alignment, SectionID, SectionName);
    }

    uint8_t *allocateDataSection(uintptr_t Size, unsigned Alignment,
                                unsigned SectionID, llvm::StringRef SectionName,
                                bool isReadOnly) override
    {
        // std::cerr << "CustomMemoryManager::allocateDataSection " << Size << " " << Alignment << std::endl;
        return llvm::SectionMemoryManager::allocateDataSection(Size, Alignment, SectionID, SectionName, isReadOnly);
    }

    bool finalizeMemory(std::string *ErrMsg = nullptr) override
    {
        // std::cerr << "CustomMemoryManager::finalizeMemory" << std::endl;
        return llvm::SectionMemoryManager::finalizeMemory(ErrMsg);
    }

    ~CustomMemoryManager() override
    {
        // std::cerr << "CustomMemoryManager::destructor" << std::endl;
    }
};

class CHCompiler: public llvm::orc::SimpleCompiler
{
public:
    using Base = llvm::orc::SimpleCompiler;
    using Base::Base;

    typename Base::CompileResult operator()(llvm::Module &M)
    {
        // std::cerr << "CHCompiler::operator() module " << std::string(M.getName()) << std::endl;
        auto compile_result = Base::operator()(M);
        // auto buffer = compile_result->getBuffer();
        // std::cerr << "Compile result " << static_cast<const void*>(buffer.data()) << " compile size " << buffer.size() << std::endl;
        return compile_result;
    }

};

class CHObjectMaterializationUnit : public llvm::orc::BasicObjectLayerMaterializationUnit
{
public:

    using Base = llvm::orc::BasicObjectLayerMaterializationUnit;
    using Base::Base;

    static llvm::Expected<std::unique_ptr<BasicObjectLayerMaterializationUnit>>
    Create(llvm::orc::ObjectLayer &L, llvm::orc::VModuleKey Key, std::unique_ptr<llvm::MemoryBuffer> O) {
        std::cerr << "CHObjectMaterializationUnit::constructor" << std::endl;

        auto symbol_flags =
            getObjectSymbolFlags(L.getExecutionSession(), O->getMemBufferRef());

        if (!symbol_flags)
            return symbol_flags.takeError();

        return std::unique_ptr<BasicObjectLayerMaterializationUnit>(
            new CHObjectMaterializationUnit(L, Key, std::move(O),
                                                std::move(*symbol_flags)));
    }

    void discard(const llvm::orc::JITDylib &JD, const llvm::orc::SymbolStringPtr &Name) override
    {
        std::cerr << "CHObjectMaterializationUnit::discard jd " << JD.getName() << " name " << std::string(*Name) << std::endl;
    }

    ~CHObjectMaterializationUnit() override
    {
        std::cerr << "CHObjectMaterializationUnit::destructor" << std::endl;
    }
};

class CHRTDyldObjectLinkingLayer: public llvm::orc::RTDyldObjectLinkingLayer
{
public:
    using Base = llvm::orc::RTDyldObjectLinkingLayer;

    using Base::Base;

    void emit(llvm::orc::MaterializationResponsibility R, std::unique_ptr<llvm::MemoryBuffer> O) override
    {
        std::cerr << "CHRTDyldObjectLinkingLayer::emit jitdylib " << R.getTargetJITDylib().getName() << std::endl;
        Base::emit(std::move(R), std::move(O));
    }

    llvm::Error add(llvm::orc::JITDylib &JD, std::unique_ptr<llvm::MemoryBuffer> O, llvm::orc::VModuleKey K = llvm::orc::VModuleKey()) override
    {
        std::cerr << "CHRTDyldObjectLinkingLayer::add " << JD.getName() << std::endl;

        auto object_mu = CHObjectMaterializationUnit::Create(*this, std::move(K),
                                                                std::move(O));
        if (!object_mu)
            return object_mu.takeError();

        return JD.define(std::move(*object_mu));
    }
};

class CHIRLayerMaterializationUnit : public llvm::orc::BasicIRLayerMaterializationUnit
{
public:
    using Base = llvm::orc::BasicIRLayerMaterializationUnit;
    using Base::Base;

    void discard(const llvm::orc::JITDylib &JD, const llvm::orc::SymbolStringPtr &Name) override
    {
        std::cerr << "CHIRLayerMaterializationUnit::discard " << JD.getName() << " symbol " << std::string(*Name) << std::endl;
    }

    ~CHIRLayerMaterializationUnit() override
    {
        std::cerr << "CHIRLayerMaterializationUnit::~CHIRLayerMaterializationUnit" << std::endl;
    }
};

class CHIRCompileLayer: public llvm::orc::IRCompileLayer
{
public:
    using Base = llvm::orc::IRCompileLayer;
    using Base::Base;

    llvm::Error add(llvm::orc::JITDylib &JD, llvm::orc::ThreadSafeModule TSM, llvm::orc::VModuleKey K) override {
        std::cerr << "CHIRCompileLayer::add " << JD.getName() << std::endl;
        auto materialization_unit = llvm::make_unique<CHIRLayerMaterializationUnit>(*this, std::move(K), std::move(TSM));
        auto symbols = materialization_unit->getSymbols();
        std::cerr << "CHIRCompileLayer:: symbols in materialization unit " << symbols.size() << std::endl;
        for (const auto & symbol : symbols)
        {
            std::cerr << std::string(*symbol.getFirst()) << std::endl;
        }

        return JD.define(materialization_unit);
    }

    void emit(llvm::orc::MaterializationResponsibility R, llvm::orc::ThreadSafeModule TSM) override
    {
        std::cerr << "CHIRCompileLayer::emit " << R.getTargetJITDylib().getName() << std::endl;
        Base::emit(std::move(R), std::move(TSM));
    }
};

struct LLVMContext
{
    llvm::orc::ThreadSafeContext context { std::make_unique<llvm::LLVMContext>() };
    std::unique_ptr<llvm::Module> module {std::make_unique<llvm::Module>("jit", *context.getContext())};
    std::unique_ptr<llvm::TargetMachine> machine {getNativeMachine()};
    llvm::DataLayout layout {machine->createDataLayout()};
    llvm::IRBuilder<> builder {*context.getContext()};

    llvm::orc::ExecutionSession execution_session;

    CHRTDyldObjectLinkingLayer object_layer;
    std::unique_ptr<CHCompiler> compiler;
    CHIRCompileLayer compile_layer;
    llvm::orc::MangleAndInterner mangler;

    std::unordered_map<std::string, void *> symbols;

    std::vector<llvm::orc::VModuleKey> modules;

    LLVMContext()
        : object_layer(execution_session, []() {
            std::cerr << "CHRTDyldObjectLinkingLayer get SectionMemoryManager" << std::endl;
            return std::make_unique<CustomMemoryManager>();
        })
        , compiler(std::make_unique<CHCompiler>(*machine))
        , compile_layer(execution_session, object_layer, *compiler)
        , mangler(execution_session, layout)
    {
        module->setDataLayout(layout);
        module->setTargetTriple(machine->getTargetTriple().getTriple());

        auto pointer = llvm::pointerToJITTargetAddress(&test_function);
        auto symbol = mangler("test_function");

        llvm::orc::SymbolMap map;
        map[symbol] = llvm::JITEvaluatedSymbol(pointer, llvm::JITSymbolFlags::Exported | llvm::JITSymbolFlags::Absolute);

        auto error = execution_session.getMainJITDylib().define(llvm::orc::absoluteSymbols(map));
        bool is_error = static_cast<bool>(error);
        std::cerr << "Error " << is_error << std::endl;

        // if (error)
        // {
        //     std::cerr << "Could not define symbols " << error-> << std::endl;
        //     std::terminate();
        // }
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
        llvm::orc::ThreadSafeModule thread_safe_module(std::move(module), context);
        modules.emplace_back(module_key);

        if (compile_layer.add(execution_session.getMainJITDylib(), std::move(thread_safe_module), module_key))
        {
            std::cerr << "Terminate because cannot add module" << std::endl;
            std::terminate();
        }

        std::cerr << "Module key " << module_key << std::endl;

        // for (const auto & name : functions)
        // {
        //     auto symbol = execution_session.lookup({&execution_session.getMainJITDylib()}, mangler(name));
        //     if (!symbol)
        //         continue; /// external function (e.g. an intrinsic that calls into libc)

        //     auto address = symbol->getAddress();
        //     if (!address)
        //     {
        //         std::cerr << "Terminate because cannot add module" << std::endl;
        //         std::terminate();
        //     }

        //     std::cerr << "Name " << name << " address " << reinterpret_cast<void *>(address) << std::endl;

        //     symbols[name] = reinterpret_cast<void *>(address);
        // }
    }
};


int main(int argc, char **argv)
{
    (void)(argc);
    (void)(argv);

    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);

    LLVMContext context;
    auto & b = context.builder;
    auto * integer_type = b.getInt64Ty();
    auto * func_type = llvm::FunctionType::get(integer_type, { integer_type }, /*isVarArg=*/false);

    std::cerr << "Context module " << context.module.get() << std::endl;

    auto * standard_function_type = llvm::FunctionType::get(b.getVoidTy(), {}, false);
    auto * standard_function = llvm::Function::Create(
        standard_function_type,
        llvm::Function::LinkageTypes::ExternalLinkage,
        "test_function",
        *context.module);
    standard_function->setCallingConv(llvm::CallingConv::C);

    auto * func = llvm::Function::Create(func_type, llvm::Function::LinkageTypes::ExternalLinkage, "test1", context.module.get());

    auto * entry = llvm::BasicBlock::Create(b.getContext(), "entry", func);
    b.SetInsertPoint(entry);

    auto * argument = func->args().begin();

    auto * value = llvm::ConstantInt::get(b.getInt64Ty(), 1);
    auto * loop_block = llvm::BasicBlock::Create(b.getContext(), "loop", func);
    b.CreateBr(loop_block);

    b.SetInsertPoint(loop_block);

    auto * phi_value = b.CreatePHI(b.getInt64Ty(), 2);
    phi_value->addIncoming(value, entry);

    b.CreateCall(standard_function);

    auto * add_value = b.CreateAdd(phi_value, llvm::ConstantInt::get(b.getInt64Ty(), 1));
    phi_value->addIncoming(add_value, loop_block);

    auto * end = llvm::BasicBlock::Create(b.getContext(), "end", func);
    b.CreateCondBr(b.CreateICmpNE(phi_value, llvm::ConstantInt::get(b.getInt64Ty(), 10)), loop_block, end);

    b.SetInsertPoint(end);

    auto * result = b.CreateAdd(phi_value, argument);
    b.CreateRet(result);

    // std::cerr << "Context module " << context.module.get() << std::endl;
    // if (context.module)
    //     context.module->print(llvm::errs(), nullptr);

    context.compileAllFunctionsToNativeCode();

    // context.module->print(llvm::errs(), nullptr);

    std::cerr << "ExecutionSession before module release dump " << std::endl;
    // context.execution_session.dump(llvm::errs());

    // for (auto module_key : context.modules)
        // context.execution_session.releaseVModule(module_key);

    llvm::orc::SymbolNameSet set;

    auto ptr = context.execution_session.intern("test1");
    set.insert(ptr);

    auto error = context.execution_session.getMainJITDylib().remove(set);

    if (error)
        llvm::logAllUnhandledErrors(std::move(error), llvm::errs(), "Error logging ");

    std::cerr << "ExecutionSession after module release dump " << std::endl;
    context.execution_session.dump(llvm::errs());

    // auto * symbol = context.symbols.at("test1");
    // auto compiled_func = reinterpret_cast<int64_t (*)(int64_t)>(symbol);
    // std::cerr << "Function " << compiled_func(5) << std::endl;

    return 0;
}
