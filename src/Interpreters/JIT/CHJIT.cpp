#include "CHJIT.h"

#if USE_EMBEDDED_COMPILER

#include <sys/mman.h>

#include <boost/noncopyable.hpp>

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

#include <base/getPageSize.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_CODE;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_MPROTECT;
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

/** Arena that allocate all memory with system page_size.
  * All allocated pages can be protected with protection_flags using protect method.
  * During destruction all allocated pages protection_flags will be reset.
  */
class PageArena : private boost::noncopyable
{
public:
    PageArena() : page_size(::getPageSize()) {}

    char * allocate(size_t size, size_t alignment)
    {
        /** First check if in some allocated page blocks there are enough free memory to make allocation.
          * If there is no such block create it and then allocate from it.
          */

        for (size_t i = 0; i < page_blocks.size(); ++i)
        {
            char * result = tryAllocateFromPageBlockWithIndex(size, alignment, i);
            if (result)
                return result;
        }

        allocateNextPageBlock(size);
        size_t allocated_page_index = page_blocks.size() - 1;
        char * result = tryAllocateFromPageBlockWithIndex(size, alignment, allocated_page_index);
        assert(result);

        return result;
    }

    inline size_t getAllocatedSize() const { return allocated_size; }

    inline size_t getPageSize() const { return page_size; }

    ~PageArena()
    {
        protect(PROT_READ | PROT_WRITE);

        for (auto & page_block : page_blocks)
            free(page_block.base());
    }

    void protect(int protection_flags)
    {
        /** The code is partially based on the LLVM codebase
              * The LLVM Project is under the Apache License v2.0 with LLVM Exceptions.
              */

#    if defined(__NetBSD__) && defined(PROT_MPROTECT)
        protection_flags |= PROT_MPROTECT(PROT_READ | PROT_WRITE | PROT_EXEC);
#    endif

        bool invalidate_cache = (protection_flags & PROT_EXEC);

        for (const auto & block : page_blocks)
        {
#    if defined(__arm__) || defined(__aarch64__)
            /// Certain ARM implementations treat icache clear instruction as a memory read,
            /// and CPU segfaults on trying to clear cache on !PROT_READ page.
            /// Therefore we need to temporarily add PROT_READ for the sake of flushing the instruction caches.
            if (invalidate_cache && !(protection_flags & PROT_READ))
            {
                int res = mprotect(block.base(), block.blockSize(), protection_flags | PROT_READ);
                if (res != 0)
                    throwFromErrno("Cannot mprotect memory region", ErrorCodes::CANNOT_MPROTECT);

                llvm::sys::Memory::InvalidateInstructionCache(block.base(), block.blockSize());
                invalidate_cache = false;
            }
#    endif
            int res = mprotect(block.base(), block.blockSize(), protection_flags);
            if (res != 0)
                throwFromErrno("Cannot mprotect memory region", ErrorCodes::CANNOT_MPROTECT);

            if (invalidate_cache)
                llvm::sys::Memory::InvalidateInstructionCache(block.base(), block.blockSize());
        }
    }

private:
    struct PageBlock
    {
    public:
        PageBlock(void * pages_base_, size_t pages_size_, size_t page_size_)
            : pages_base(pages_base_), pages_size(pages_size_), page_size(page_size_)
        {
        }

        inline void * base() const { return pages_base; }
        inline size_t pagesSize() const { return pages_size; }
        inline size_t pageSize() const { return page_size; }
        inline size_t blockSize() const { return pages_size * page_size; }

    private:
        void * pages_base;
        size_t pages_size;
        size_t page_size;
    };

    std::vector<PageBlock> page_blocks;

    std::vector<size_t> page_blocks_allocated_size;

    size_t page_size = 0;

    size_t allocated_size = 0;

    char * tryAllocateFromPageBlockWithIndex(size_t size, size_t alignment, size_t page_block_index)
    {
        assert(page_block_index < page_blocks.size());
        auto & pages_block = page_blocks[page_block_index];

        size_t block_size = pages_block.blockSize();
        size_t & block_allocated_size = page_blocks_allocated_size[page_block_index];
        size_t block_free_size = block_size - block_allocated_size;

        uint8_t * pages_start = static_cast<uint8_t *>(pages_block.base());
        void * pages_offset = pages_start + block_allocated_size;

        auto * result = std::align(alignment, size, pages_offset, block_free_size);

        if (result)
        {
            block_allocated_size = reinterpret_cast<uint8_t *>(result) - pages_start;
            block_allocated_size += size;

            return static_cast<char *>(result);
        }
        else
        {
            return nullptr;
        }
    }

    void allocateNextPageBlock(size_t size)
    {
        size_t pages_to_allocate_size = ((size / page_size) + 1) * 2;
        size_t allocate_size = page_size * pages_to_allocate_size;

        void * buf = nullptr;
        int res = posix_memalign(&buf, page_size, allocate_size);

        if (res != 0)
            throwFromErrno(
                fmt::format("Cannot allocate memory (posix_memalign) alignment {} size {}.", page_size, ReadableSize(allocate_size)),
                ErrorCodes::CANNOT_ALLOCATE_MEMORY,
                res);

        page_blocks.emplace_back(buf, pages_to_allocate_size, page_size);
        page_blocks_allocated_size.emplace_back(0);

        allocated_size += allocate_size;
    }
};

/** MemoryManager for module.
  * Keep total allocated size during RuntimeDyld linker execution.
  */
class JITModuleMemoryManager : public llvm::RTDyldMemoryManager
{
public:

    uint8_t * allocateCodeSection(uintptr_t size, unsigned alignment, unsigned, llvm::StringRef) override
    {
        return reinterpret_cast<uint8_t *>(ex_page_arena.allocate(size, alignment));
    }

    uint8_t * allocateDataSection(uintptr_t size, unsigned alignment, unsigned, llvm::StringRef, bool is_read_only) override
    {
        if (is_read_only)
            return reinterpret_cast<uint8_t *>(ro_page_arena.allocate(size, alignment));
        else
            return reinterpret_cast<uint8_t *>(rw_page_arena.allocate(size, alignment));
    }

    bool finalizeMemory(std::string *) override
    {
        ro_page_arena.protect(PROT_READ);
        ex_page_arena.protect(PROT_READ | PROT_EXEC);
        return true;
    }

    inline size_t allocatedSize() const
    {
        size_t data_size = rw_page_arena.getAllocatedSize() + ro_page_arena.getAllocatedSize();
        size_t code_size = ex_page_arena.getAllocatedSize();

        return data_size + code_size;
    }

private:
    PageArena rw_page_arena;
    PageArena ro_page_arena;
    PageArena ex_page_arena;
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

CHJIT::CompiledModule CHJIT::compileModule(std::function<void (llvm::Module &)> compile_function)
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

CHJIT::CompiledModule CHJIT::compileModule(std::unique_ptr<llvm::Module> module)
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
    llvm::RuntimeDyld dynamic_linker = {*module_memory_manager, *symbol_resolver};

    std::unique_ptr<llvm::RuntimeDyld::LoadedObjectInfo> linked_object = dynamic_linker.loadObject(*object.get());

    dynamic_linker.resolveRelocations();
    module_memory_manager->finalizeMemory(nullptr);

    CompiledModule compiled_module;

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
        compiled_module.function_name_to_symbol.emplace(std::move(function_name), jit_symbol_address);
    }

    compiled_module.size = module_memory_manager->allocatedSize();
    compiled_module.identifier = current_module_key;

    module_identifier_to_memory_manager[current_module_key] = std::move(module_memory_manager);

    compiled_code_size.fetch_add(compiled_module.size, std::memory_order_relaxed);

    return compiled_module;
}

void CHJIT::deleteCompiledModule(const CHJIT::CompiledModule & module)
{
    std::lock_guard<std::mutex> lock(jit_lock);

    auto module_it = module_identifier_to_memory_manager.find(module.identifier);
    if (module_it == module_identifier_to_memory_manager.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no compiled module with identifier {}", module.identifier);

    module_identifier_to_memory_manager.erase(module_it);
    compiled_code_size.fetch_sub(module.size, std::memory_order_relaxed);
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
