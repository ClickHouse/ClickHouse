#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_EMBEDDED_COMPILER

#include <unordered_map>
#include <atomic>

#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/Target/TargetMachine.h>

namespace DB
{

class JITModuleMemoryManager;
class JITSymbolResolver;
class JITCompiler;

/// TODO: Add documentation
class CHJIT
{
public:
    CHJIT();

    ~CHJIT();

    struct CompiledModuleInfo
    {
        size_t size;
        uint64_t module_identifier;
        std::vector<std::string> compiled_functions;
    };

    CompiledModuleInfo compileModule(std::function<void (llvm::Module &)> compile_function);

    void deleteCompiledModule(const CompiledModuleInfo & module_info);

    void * findCompiledFunction(const std::string & name) const;

    void registerExternalSymbol(const std::string & symbol_name, void * address);

    inline size_t getCompiledCodeSize() const { return compiled_code_size.load(std::memory_order_relaxed); }

private:

    std::unique_ptr<llvm::Module> createModuleForCompilation();

    CompiledModuleInfo compileModule(std::unique_ptr<llvm::Module> module);

    std::string getMangledName(const std::string & name_to_mangle) const;

    void runOptimizationPassesOnModule(llvm::Module & module) const;

    static std::unique_ptr<llvm::TargetMachine> getTargetMachine();

    llvm::LLVMContext context;
    std::unique_ptr<llvm::TargetMachine> machine;
    llvm::DataLayout layout;
    std::unique_ptr<JITCompiler> compiler;
    std::unique_ptr<JITSymbolResolver> symbol_resolver;

    std::unordered_map<std::string, void *> name_to_symbol;
    std::unordered_map<uint64_t, std::unique_ptr<JITModuleMemoryManager>> module_identifier_to_memory_manager;
    uint64_t current_module_key = 0;
    std::atomic<size_t> compiled_code_size = 0;
    mutable std::mutex jit_lock;

};

}

#endif
