#include "CHJIT.h"

#if USE_EMBEDDED_COMPILER

#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Mangler.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/ExecutionEngine/JITEventListener.h>
#include <llvm/MC/SubtargetFeature.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/Host.h>
#include <llvm/Support/TargetRegistry.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Support/SmallVectorMemoryBuffer.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_CODE;
    extern const int LOGICAL_ERROR;
}

/** Simple module to object file compiler.
  * Result object cannot be used as machine code directly, it should be passed to linker.
  */
class JITCompiler
{
public:

    explicit JITCompiler(llvm::TargetMachine &target_machine_)
    : target_machine(target_machine_)
    {
    }

    std::unique_ptr<llvm::MemoryBuffer> compile(llvm::Module & module)
    {
        auto materialize_error = module.materializeAll();
        if (materialize_error)
        {
            std::string error_message;
            handleAllErrors(std::move(materialize_error),
                [&](const llvm::ErrorInfoBase & error_info) { error_message = error_info.message(); });

            throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "Cannot materialize module: {}", error_message);
        }

        llvm::SmallVector<char, 4096> object_buffer;

        llvm::raw_svector_ostream object_stream(object_buffer);
        llvm::legacy::PassManager pass_manager;
        llvm::MCContext * machine_code_context = nullptr;

        if (target_machine.addPassesToEmitMC(pass_manager, machine_code_context, object_stream))
            throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "MachineCode is not supported for the platform");

        pass_manager.run(module);

        std::unique_ptr<llvm::MemoryBuffer> compiled_object_buffer = std::make_unique<llvm::SmallVectorMemoryBuffer>(
            std::move(object_buffer), "<in memory object compiled from " + module.getModuleIdentifier() + ">");

        return compiled_object_buffer;
    }

    ~JITCompiler() = default;

private:
    llvm::TargetMachine & target_machine;
};

/** MemoryManager for module.
  * Keep total allocated size during RuntimeDyld linker execution.
  * Actual compiled code memory is stored in llvm::SectionMemoryManager member, we cannot use ZeroBase optimization here
  * because it is required for llvm::SectionMemoryManager::MemoryMapper to live longer than llvm::SectionMemoryManager.
  */
class JITModuleMemoryManager
{
    class DefaultMMapper final : public llvm::SectionMemoryManager::MemoryMapper
    {
    public:
        llvm::sys::MemoryBlock allocateMappedMemory(
            llvm::SectionMemoryManager::AllocationPurpose Purpose [[maybe_unused]],
            size_t NumBytes,
            const llvm::sys::MemoryBlock * const NearBlock,
            unsigned Flags,
            std::error_code & EC) override
        {
            auto allocated_memory_block = llvm::sys::Memory::allocateMappedMemory(NumBytes, NearBlock, Flags, EC);
            allocated_size += allocated_memory_block.allocatedSize();
            return allocated_memory_block;
        }

        std::error_code protectMappedMemory(const llvm::sys::MemoryBlock & Block, unsigned Flags) override
        {
            return llvm::sys::Memory::protectMappedMemory(Block, Flags);
        }

        std::error_code releaseMappedMemory(llvm::sys::MemoryBlock & M) override { return llvm::sys::Memory::releaseMappedMemory(M); }

        size_t allocated_size = 0;
    };

public:
    JITModuleMemoryManager() : manager(&mmaper) { }

    inline size_t getAllocatedSize() const { return mmaper.allocated_size; }

    inline llvm::SectionMemoryManager & getManager() { return manager; }

private:
    DefaultMMapper mmaper;
    llvm::SectionMemoryManager manager;
};

class JITSymbolResolver : public llvm::LegacyJITSymbolResolver
{
public:
    llvm::JITSymbol findSymbolInLogicalDylib(const std::string &) override { return nullptr; }

    llvm::JITSymbol findSymbol(const std::string & Name) override
    {
        auto address_it = symbol_name_to_symbol_address.find(Name);
        if (address_it == symbol_name_to_symbol_address.end())
            throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "Could not find symbol {}", Name);

        uint64_t symbol_address = reinterpret_cast<uint64_t>(address_it->second);
        auto jit_symbol = llvm::JITSymbol(symbol_address, llvm::JITSymbolFlags::None);

        return jit_symbol;
    }

    void registerSymbol(const std::string & symbol_name, void * symbol) { symbol_name_to_symbol_address[symbol_name] = symbol; }

    ~JITSymbolResolver() override = default;

private:
    std::unordered_map<std::string, void *> symbol_name_to_symbol_address;
};

/// GDB JITEventListener. Can be used if result machine code need to be debugged.
// class JITEventListener
// {
// public:
//     JITEventListener()
//         : gdb_listener(llvm::JITEventListener::createGDBRegistrationListener())
//     {}

//     void notifyObjectLoaded(
//         llvm::JITEventListener::ObjectKey object_key,
//         const llvm::object::ObjectFile & object_file,
//         const llvm::RuntimeDyld::LoadedObjectInfo & loaded_object_Info)
//     {
//         gdb_listener->notifyObjectLoaded(object_key, object_file, loaded_object_Info);
//     }

//     void notifyFreeingObject(llvm::JITEventListener::ObjectKey object_key)
//     {
//         gdb_listener->notifyFreeingObject(object_key);
//     }

// private:
//     llvm::JITEventListener * gdb_listener = nullptr;
// };

CHJIT::CHJIT()
    : machine(getTargetMachine())
    , layout(machine->createDataLayout())
    , compiler(std::make_unique<JITCompiler>(*machine))
    , symbol_resolver(std::make_unique<JITSymbolResolver>())
{
    /// Define common symbols that can be generated during compilation
    /// Necessary for valid linker symbol resolution
    symbol_resolver->registerSymbol("memset", reinterpret_cast<void *>(&memset));
    symbol_resolver->registerSymbol("memcpy", reinterpret_cast<void *>(&memcpy));
    symbol_resolver->registerSymbol("memcmp", reinterpret_cast<void *>(&memcmp));
}

CHJIT::~CHJIT() = default;

CHJIT::CompiledModuleInfo CHJIT::compileModule(std::function<void (llvm::Module &)> compile_function)
{
    std::lock_guard<std::mutex> lock(jit_lock);

    auto module = createModuleForCompilation();
    compile_function(*module);
    auto module_info = compileModule(std::move(module));

    ++current_module_key;
    return module_info;
}

std::unique_ptr<llvm::Module> CHJIT::createModuleForCompilation()
{
    std::unique_ptr<llvm::Module> module = std::make_unique<llvm::Module>("jit" + std::to_string(current_module_key), context);
    module->setDataLayout(layout);
    module->setTargetTriple(machine->getTargetTriple().getTriple());

    return module;
}

CHJIT::CompiledModuleInfo CHJIT::compileModule(std::unique_ptr<llvm::Module> module)
{
    runOptimizationPassesOnModule(*module);

    auto buffer = compiler->compile(*module);

    llvm::Expected<std::unique_ptr<llvm::object::ObjectFile>> object = llvm::object::ObjectFile::createObjectFile(*buffer);

    if (!object)
    {
        std::string error_message;
        handleAllErrors(object.takeError(), [&](const llvm::ErrorInfoBase & error_info) { error_message = error_info.message(); });

        throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "Cannot create object file from compiled buffer: {}", error_message);
    }

    std::unique_ptr<JITModuleMemoryManager> module_memory_manager = std::make_unique<JITModuleMemoryManager>();
    llvm::RuntimeDyld dynamic_linker = {module_memory_manager->getManager(), *symbol_resolver};

    std::unique_ptr<llvm::RuntimeDyld::LoadedObjectInfo> linked_object = dynamic_linker.loadObject(*object.get());

    dynamic_linker.resolveRelocations();
    module_memory_manager->getManager().finalizeMemory();

    CompiledModuleInfo module_info;

    for (const auto & function : *module)
    {
        if (function.isDeclaration())
            continue;

        auto function_name = std::string(function.getName());

        auto mangled_name = getMangledName(function_name);
        auto jit_symbol = dynamic_linker.getSymbol(mangled_name);

        if (!jit_symbol)
            throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "DynamicLinker could not found symbol {} after compilation", function_name);

        auto * jit_symbol_address = reinterpret_cast<void *>(jit_symbol.getAddress());

        std::string symbol_name = std::to_string(current_module_key) + '_' + function_name;
        name_to_symbol[symbol_name] = jit_symbol_address;
        module_info.compiled_functions.emplace_back(std::move(function_name));
    }

    module_info.size = module_memory_manager->getAllocatedSize();
    module_info.identifier = current_module_key;

    module_identifier_to_memory_manager[current_module_key] = std::move(module_memory_manager);

    compiled_code_size.fetch_add(module_info.size, std::memory_order_relaxed);

    return module_info;
}

void CHJIT::deleteCompiledModule(const CHJIT::CompiledModuleInfo & module_info)
{
    std::lock_guard<std::mutex> lock(jit_lock);

    auto module_it = module_identifier_to_memory_manager.find(module_info.identifier);
    if (module_it == module_identifier_to_memory_manager.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no compiled module with identifier {}", module_info.identifier);

    for (const auto & function : module_info.compiled_functions)
        name_to_symbol.erase(function);

    module_identifier_to_memory_manager.erase(module_it);
    compiled_code_size.fetch_sub(module_info.size, std::memory_order_relaxed);
}

void * CHJIT::findCompiledFunction(const CompiledModuleInfo & module_info, const std::string & function_name) const
{
    std::lock_guard<std::mutex> lock(jit_lock);

    std::string symbol_name = std::to_string(module_info.identifier) + '_' + function_name;
    auto it = name_to_symbol.find(symbol_name);
    if (it != name_to_symbol.end())
        return it->second;

    return nullptr;
}

void CHJIT::registerExternalSymbol(const std::string & symbol_name, void * address)
{
    std::lock_guard<std::mutex> lock(jit_lock);
    symbol_resolver->registerSymbol(symbol_name, address);
}

std::string CHJIT::getMangledName(const std::string & name_to_mangle) const
{
    std::string mangled_name;
    llvm::raw_string_ostream mangled_name_stream(mangled_name);
    llvm::Mangler::getNameWithPrefix(mangled_name_stream, name_to_mangle, layout);
    mangled_name_stream.flush();

    return mangled_name;
}

void CHJIT::runOptimizationPassesOnModule(llvm::Module & module) const
{
    llvm::PassManagerBuilder pass_manager_builder;
    llvm::legacy::PassManager mpm;
    llvm::legacy::FunctionPassManager fpm(&module);
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
    for (auto & function : module)
        fpm.run(function);
    fpm.doFinalization();

    mpm.run(module);
}

std::unique_ptr<llvm::TargetMachine> CHJIT::getTargetMachine()
{
    static std::once_flag llvm_target_initialized;
    std::call_once(llvm_target_initialized, []()
    {
        llvm::InitializeNativeTarget();
        llvm::InitializeNativeTargetAsmPrinter();
        llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
    });

    std::string error;
    auto cpu = llvm::sys::getHostCPUName();
    auto triple = llvm::sys::getProcessTriple();
    const auto * target = llvm::TargetRegistry::lookupTarget(triple, error);
    if (!target)
        throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "Cannot find target triple {} error: {}", triple, error);

    llvm::SubtargetFeatures features;
    llvm::StringMap<bool> feature_map;
    if (llvm::sys::getHostCPUFeatures(feature_map))
        for (auto & f : feature_map)
            features.AddFeature(f.first(), f.second);

    llvm::TargetOptions options;

    bool jit = true;
    auto * target_machine = target->createTargetMachine(triple,
        cpu,
        features.getString(),
        options,
        llvm::None,
        llvm::None,
        llvm::CodeGenOpt::Aggressive,
        jit);

    if (!target_machine)
        throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "Cannot create target machine");

    return std::unique_ptr<llvm::TargetMachine>(target_machine);
}

}

#endif
