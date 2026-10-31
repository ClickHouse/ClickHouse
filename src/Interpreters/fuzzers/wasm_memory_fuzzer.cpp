#include "config.h"

#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/MemoryTracker.h>
#include <Common/StopToken.h>
#include <Interpreters/Context.h>
#include <Interpreters/WebAssembly/WasmEngine.h>
#include <Interpreters/WebAssembly/WasmTypes.h>

#if USE_WASMTIME
#    include <Interpreters/WebAssembly/WasmTimeRuntime.h>
#endif

#include <cstdint>
#include <cstdlib>
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

/// The minimal module compiled once at startup, together with the engine that owns it.
std::unique_ptr<IWasmEngine> engine;
std::unique_ptr<WasmModule> minimal_module;

extern "C" int LLVMFuzzerInitialize(int *, char ***);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

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
    const std::string_view wasm_bytes(reinterpret_cast<const char *>(kMinimalWasm), sizeof(kMinimalWasm));
    /// The module carries no code (only a memory section), so fuel accounting is unnecessary.
    minimal_module = engine->compileModule("wasm_memory_fuzzer", wasm_bytes, FuelMode::Disabled);
#endif

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

        if (size < 8 || !minimal_module)
            return 0;

        uint32_t ptr_val = 0;
        uint32_t size_val = 0;
        memcpy(&ptr_val, data, 4);
        memcpy(&size_val, data + 4, 4);

        WasmModule::Config cfg(FuelMode::Disabled); /// no instruction budget needed (no code)
        cfg.memory_limit = 64 * 1024; /// 1 page (64 KiB) — matches kMinimalWasm

        auto compartment = minimal_module->instantiate(cfg, StopToken{});

        /// This call must throw (or return a valid in-bounds span) — it must
        /// never return an out-of-bounds span regardless of ptr_val + size_val
        /// overflow.  ASan will catch any actual OOB access.
        try
        {
            std::span<uint8_t> mem_span = compartment->getMemory(ptr_val, size_val);

            /// A successful getMemory promises exactly `size_val` bytes fully inside the
            /// WASM memory. A span whose start is in bounds but whose tail runs past the
            /// end (e.g. a missing `ptr + size <= memory_size` check) would not be caught
            /// by only probing the first byte, so touch both ends and verify the length.
            if (mem_span.size() != size_val)
                abort();

            if (!mem_span.empty())
            {
                volatile uint8_t first = mem_span.front();
                volatile uint8_t last = mem_span.back();
                (void)first;
                (void)last;
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
