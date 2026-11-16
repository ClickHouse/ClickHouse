#include "config.h"

#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/MemoryTracker.h>
#include <Common/StopToken.h>
#include <Interpreters/Context.h>
#include <Interpreters/WebAssembly/WasmEngine.h>

#if USE_WASMTIME
#    include <Interpreters/WebAssembly/WasmTimeRuntime.h>
#endif

#include <memory>

using namespace DB;
using namespace DB::WebAssembly;

ContextMutablePtr context;

extern "C" int LLVMFuzzerInitialize(int *, char ***);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    if (context)
        return true;

    static SharedContextHolder shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();

    MainThreadStatus::getInstance();

    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        total_memory_tracker.resetCounters();
        total_memory_tracker.setHardLimit(1_GiB);
        CurrentThread::get().memory_tracker.resetCounters();
        CurrentThread::get().memory_tracker.setHardLimit(1_GiB);

        /// An empty input is not a module.
        if (size < 1)
            return 0;

        /// The whole input is the module: `wasmtime` is the only WebAssembly backend.
        const std::string_view wasm_bytes(reinterpret_cast<const char *>(data), size);

#if USE_WASMTIME
        std::unique_ptr<IWasmEngine> engine = std::make_unique<WasmTimeRuntime>();
#else
        /// No WASM backend available - nothing to fuzz.
        (void)wasm_bytes;
        return 0;
#endif

        /// Attempt to compile the fuzzer-supplied bytes as a WASM module.
        /// Fuel accounting is enabled so a fuzzer-supplied `(start)` function cannot loop forever.
        auto module = engine->compileModule("fuzzer_module", wasm_bytes, FuelMode::Enabled);

        /// Instantiate with conservative resource limits to prevent infinite loops
        /// and excessive memory use inside guest code.
        WasmModule::Config cfg(FuelMode::Enabled);
        cfg.memory_limit = 16 * 1024 * 1024; /// 16 MiB guest memory
        cfg.fuel_limit = 1'000'000;            /// limit guest instructions

        auto compartment = module->instantiate(cfg, StopToken{});

        /// Try to call common entry-point exports if they exist.
        /// Both calls are wrapped individually so one failing does not skip the other.
        try
        {
            compartment->invoke<void>("_start", {}, StopToken{});
        }
        catch (...)
        {
        }

        try
        {
            compartment->invoke<void>("main", {}, StopToken{});
        }
        catch (...)
        {
        }
    }
    catch (...)
    {
    }

    return 0;
}
