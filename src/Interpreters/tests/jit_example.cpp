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

struct LLVMContext
{
    llvm::orc::ThreadSafeContext context { std::make_unique<llvm::LLVMContext>() };
    std::unique_ptr<llvm::Module> module {std::make_unique<llvm::Module>("jit", *context.getContext())};
    std::unique_ptr<llvm::TargetMachine> machine {getNativeMachine()};
    llvm::DataLayout layout {machine->createDataLayout()};
    llvm::IRBuilder<> builder {*context.getContext()};

    llvm::orc::ExecutionSession execution_session;

    llvm::orc::RTDyldObjectLinkingLayer object_layer;
    std::unique_ptr<llvm::orc::SimpleCompiler> compiler;
    llvm::orc::IRCompileLayer compile_layer;
    llvm::orc::MangleAndInterner mangler;

    std::unordered_map<std::string, void *> symbols;

    std::vector<llvm::orc::VModuleKey> modules;

    LLVMContext()
        : object_layer(execution_session, []() { return std::make_unique<llvm::SectionMemoryManager>(); })
        , compiler(std::make_unique<llvm::orc::SimpleCompiler>(*machine))
        , compile_layer(execution_session, object_layer, *compiler)
        , mangler(execution_session, layout)
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
        llvm::orc::ThreadSafeModule thread_safe_module(std::move(module), context);
        modules.emplace_back(module_key);

        if (compile_layer.add(execution_session.getMainJITDylib(), std::move(thread_safe_module), module_key))
        {
            std::cerr << "Terminate because cannot add module" << std::endl;
            std::terminate();
        }

        std::cerr << "Module key " << module_key << std::endl;

        for (const auto & name : functions)
        {
            auto symbol = execution_session.lookup({&execution_session.getMainJITDylib()}, mangler(name));
            if (!symbol)
                continue; /// external function (e.g. an intrinsic that calls into libc)

            auto address = symbol->getAddress();
            if (!address)
            {
                std::cerr << "Terminate because cannot add module" << std::endl;
                std::terminate();
            }

            std::cerr << "Name " << name << " address " << reinterpret_cast<void *>(address) << std::endl;

            symbols[name] = reinterpret_cast<void *>(address);
        }
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

    auto * func = llvm::Function::Create(
        func_type, llvm::Function::LinkageTypes::ExternalLinkage, "test1",
        context.module.get());

    auto *basic_block = llvm::BasicBlock::Create(b.getContext(), "entry", func);
    b.SetInsertPoint(basic_block);

    auto *argument = func->args().begin();

    // auto *value = llvm::ConstantInt::get(b.getInt64Ty(), 0);
    // auto *phi_value = b.CreatePHI(b.getInt64Ty(), 2);
    // phi_value->addIncoming(value, basic_block);

    // auto *loop_block = llvm::BasicBlock::Create(b.getContext(), "loop", func);
    // b.CreateBr(loop_block);
    // b.SetInsertPoint(loop_block);
    // auto *add_value =
    //     b.CreateAdd(phi_value, llvm::ConstantInt::get(b.getInt64Ty(), 1));
    // phi_value->addIncoming(add_value, loop_block);

    // auto *end = llvm::BasicBlock::Create(b.getContext(), "end", func);
    // b.CreateCondBr(
    //     b.CreateICmpNE(phi_value, llvm::ConstantInt::get(b.getInt64Ty(), 10)),
    //     loop_block, end);

    // b.SetInsertPoint(end);

    auto * result = b.CreateAdd(argument, argument);
    b.CreateRet(result);

    std::cerr << "Context module " << context.module.get() << std::endl;
    if (context.module)
        context.module->print(llvm::errs(), nullptr);

    context.compileAllFunctionsToNativeCode();
    // context.module->print(llvm::errs(), nullptr);

    std::cerr << "ExecutionSession before module release dump " << std::endl;
    context.execution_session.dump(llvm::errs());

    for (auto module_key : context.modules)
        context.execution_session.releaseVModule(module_key);
    std::cerr << "ExecutionSession after module release dump " << std::endl;
    context.execution_session.dump(llvm::errs());

    auto * symbol = context.symbols.at("test1");
    auto compiled_func = reinterpret_cast<int64_t (*)(int64_t)>(symbol);
    std::cerr << "Function " << compiled_func(5) << std::endl;

    return 0;
}
