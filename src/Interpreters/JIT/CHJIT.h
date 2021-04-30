#pragma once

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_EMBEDDED_COMPILER

#include <unordered_map>

#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/Target/TargetMachine.h>

namespace DB
{

class JITModuleMemoryManager;
class JITSymbolResolver;
class JITCompiler;

class CHJIT
{
public:
    CHJIT();

    ~CHJIT();

    struct CompiledModuleInfo
    {
        size_t size;
        std::string module_identifier;
        std::vector<std::string> compiled_functions;
    };

    std::unique_ptr<llvm::Module> createModuleForCompilation();

    CompiledModuleInfo compileModule(std::unique_ptr<llvm::Module> module);

    void deleteCompiledModule(const CompiledModuleInfo & module_info);

    void * findCompiledFunction(const std::string & name) const;

    void registerExternalSymbol(const std::string & symbol_name, void * address);

    llvm::LLVMContext & getContext()
    {
        return context;
    }

    inline size_t getCompiledCodeSize() const
    {
        return compiled_code_size;
    }

private:

    std::string getMangledName(const std::string & name_to_mangle) const;

    void runOptimizationPassesOnModule(llvm::Module & module) const;

    static std::unique_ptr<llvm::TargetMachine> getTargetMachine();

    llvm::LLVMContext context;
    std::unique_ptr<llvm::TargetMachine> machine;
    llvm::DataLayout layout;
    std::unique_ptr<JITCompiler> compiler;
    std::unique_ptr<JITSymbolResolver> symbol_resolver;

    std::unordered_map<std::string, void *> name_to_symbol;
    std::unordered_map<std::string, std::unique_ptr<JITModuleMemoryManager>> module_identifier_to_memory_manager;
    size_t current_module_key = 0;
    size_t compiled_code_size = 0;
};

}

#endif
