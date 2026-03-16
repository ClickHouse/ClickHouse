#include "config.h"

#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Interpreters/Context.h>
#include <Interpreters/WebAssembly/WasmEngine.h>

#if USE_WASMTIME
#    include <Interpreters/WebAssembly/WasmTimeRuntime.h>
#endif

#if USE_WASMEDGE
#    include <Interpreters/WebAssembly/WasmEdgeRuntime.h>
#endif

#include <cstdint>
#include <cstring>
#include <memory>

using namespace DB;
using namespace DB::WebAssembly;

/// Minimal valid WebAssembly module: 1 memory page (64 KiB), exported as "memory".
///
/// Binary layout:
///   \x00asm (magic) + \x01\x00\x00\x00 (version 1)
///   Memory section (ID=5): 1 memory, limits type=0 (no max), min=1 page
///   Export section (ID=7): export "memory" = memory[0]
static const uint8_t kMinimalWasm[] = {
    /// Magic + version
    0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
    /// Memory section: [05][len=3][count=1][type=0][min=1]
    0x05, 0x03, 0x01, 0x00, 0x01,
    /// Export section: [07][len=10][count=1][name_len=6]["memory"][kind=2][index=0]
    0x07, 0x0a, 0x01, 0x06, 0x6d, 0x65, 0x6d, 0x6f, 0x72, 0x79, 0x02, 0x00,
};

ContextMutablePtr context;
static std::unique_ptr<IWasmEngine> engine;
static std::unique_ptr<WasmModule> wasm_module;

extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    if (context)
        return 0;

    static SharedContextHolder shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();

    MainThreadStatus::getInstance();

#if USE_WASMTIME
    engine = std::make_unique<WasmTimeRuntime>();
#elif USE_WASMEDGE
    engine = std::make_unique<WasmEdgeRuntime>();
#else
    return 0;
#endif

    const std::string_view wasm_bytes(
        reinterpret_cast<const char *>(kMinimalWasm), sizeof(kMinimalWasm));
    wasm_module = engine->compileModule(wasm_bytes);

    return 0;
}

/// Fuzz input layout:
///   [0..3]  WasmPtr  ptr  (uint32_t, little-endian)
///   [4..7]  WasmSizeT size (uint32_t, little-endian)
///
/// This targeted harness exercises WasmCompartment::getMemory with all possible
/// (ptr, size) combinations, concentrating on integer-overflow edge cases that
/// the original uint32_t bounds check was vulnerable to:
///   ptr=0xFFFFFFFF + size=1  → wraps to 0 in uint32_t arithmetic
///   ptr=0xFFFFFFFE + size=2  → wraps to 0
///   ptr=0 + size=0xFFFFFFFF  → wraps
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        total_memory_tracker.resetCounters();
        total_memory_tracker.setHardLimit(256 * 1024 * 1024ULL);
        CurrentThread::get().memory_tracker.resetCounters();
        CurrentThread::get().memory_tracker.setHardLimit(256 * 1024 * 1024ULL);

        if (size < 8 || !wasm_module)
            return 0;

        uint32_t ptr_val = 0;
        uint32_t size_val = 0;
        memcpy(&ptr_val, data, 4);
        memcpy(&size_val, data + 4, 4);

        WasmModule::Config cfg;
        cfg.memory_limit = 64 * 1024; /// 1 page (64 KiB) — matches kMinimalWasm
        cfg.fuel_limit = 0;           /// no instruction budget needed (no code)

        auto compartment = wasm_module->instantiate(cfg);

        /// This call must throw (or return a valid in-bounds pointer) — it must
        /// never return an out-of-bounds pointer regardless of ptr_val + size_val
        /// overflow.  ASan will catch any actual OOB access.
        try
        {
            uint8_t * host_ptr = compartment->getMemory(ptr_val, size_val);

            /// If the call succeeds, write and read one byte to trigger ASan on OOB.
            if (size_val > 0)
            {
                volatile uint8_t probe = *host_ptr;
                (void)probe;
            }
        }
        catch (...)
        {
            /// Out-of-range access correctly throws — this is expected behavior.
        }
    }
    catch (...)
    {
    }

    return 0;
}
