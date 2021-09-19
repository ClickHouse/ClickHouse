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

/** Custom jit implementation
  * Main use cases:
  * 1. Compiled functions in module.
  * 2. Release memory for compiled functions.
  *
  * In LLVM library there are 2 main JIT stacks MCJIT and ORCv2.
  *
  * Main reasons for custom implementation vs MCJIT
  * MCJIT keeps llvm::Module and compiled object code before linking process after module was compiled.
  * llvm::Module can be removed, but compiled object code cannot be removed. Memory for compiled code
  * will be release only during MCJIT instance destruction. It is too expensive to create MCJIT
  * instance for each compiled module.
  * Also important reason is that if some error occurred during compilation steps, MCJIT can just terminate
  * our program.
  *
  * Main reasons for custom implementation vs ORCv2.
  * ORC is on request compiler, we do not need support for asynchronous compilation.
  * It was possible to remove compiled code with ORCv1 but it was deprecated.
  * In ORCv2 this probably can be done only with custom layer and materialization unit.
  * But it is inconvenient, discard is only called for materialization units by JITDylib that are not yet materialized.
  *
  * CHJIT interface is thread safe, that means all functions can be called from multiple threads and state of CHJIT instance
  * will not be broken.
  * It is client responsibility to be sure and do not use compiled code after it was released.
  */
class CHJIT
{
public:
    CHJIT();

    ~CHJIT();

    struct CompiledModuleInfo
    {
        /// Size of compiled module code in bytes
        size_t size;
        /// Module identifier. Should not be changed by client
        uint64_t identifier;
        /// Vector of compiled function nameds. Should not be changed by client
        std::vector<std::string> compiled_functions;
    };

    /** Compile module. In compile function client responsibility is to fill module with necessary
      * IR code, then it will be compiled by CHJIT instance.
      * Return compiled module info.
      */
    CompiledModuleInfo compileModule(std::function<void (llvm::Module &)> compile_function);

    /** Delete compiled module. Pointers to functions from module become invalid after this call.
      * It is client responsibility to be sure that there are no pointers to compiled module code.
      */
    void deleteCompiledModule(const CompiledModuleInfo & module_info);

    /** Find compiled function using module_info, and function_name.
      * It is client responsibility to case result function to right signature.
      * After call to deleteCompiledModule compiled functions from module become invalid.
      */
    void * findCompiledFunction(const CompiledModuleInfo & module_info, const std::string & function_name) const;

    /** Register external symbol for CHJIT instance to use, during linking.
      * It can be function, or global constant.
      * It is client responsibility to be sure that address of symbol is valid during CHJIT instance lifetime.
      */
    void registerExternalSymbol(const std::string & symbol_name, void * address);

    /** Total compiled code size for module that are currently valid.
      */
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
