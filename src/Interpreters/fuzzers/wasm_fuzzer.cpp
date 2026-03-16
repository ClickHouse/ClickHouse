#include "config.h"

#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/StopToken.h>
#include <Interpreters/Context.h>
#include <Interpreters/WebAssembly/WasmEngine.h>

#if USE_WASMTIME
#    include <Interpreters/WebAssembly/WasmTimeRuntime.h>
#endif

#if USE_WASMEDGE
#    include <Interpreters/WebAssembly/WasmEdgeRuntime.h>
#endif

#include <memory>

using namespace DB;
using namespace DB::WebAssembly;

ContextMutablePtr context;

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

        /// Need at least 1 byte for backend selector and at least a few bytes of WASM bytecode.
        if (size < 2)
            return 0;

        /// Byte 0 selects the backend.
        const uint8_t backend_selector = data[0];
        const std::string_view wasm_bytes(reinterpret_cast<const char *>(data + 1), size - 1);

        std::unique_ptr<IWasmEngine> engine;

#if USE_WASMTIME && USE_WASMEDGE
        if (backend_selector & 1)
            engine = std::make_unique<WasmTimeRuntime>();
        else
            engine = std::make_unique<WasmEdgeRuntime>();
#elif USE_WASMTIME
        (void)backend_selector;
        engine = std::make_unique<WasmTimeRuntime>();
#elif USE_WASMEDGE
        (void)backend_selector;
        engine = std::make_unique<WasmEdgeRuntime>();
#else
        /// No WASM backend available — nothing to fuzz.
        (void)backend_selector;
        (void)wasm_bytes;
        return 0;
#endif

        /// Attempt to compile the fuzzer-supplied bytes as a WASM module.
        auto module = engine->compileModule(wasm_bytes);

        /// Instantiate with conservative resource limits to prevent infinite loops
        /// and excessive memory use inside guest code.
        WasmModule::Config cfg;
        cfg.memory_limit = 16 * 1024 * 1024; /// 16 MiB guest memory
        cfg.fuel_limit = 1'000'000;            /// limit guest instructions

        auto compartment = module->instantiate(cfg);

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
