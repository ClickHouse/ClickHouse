#pragma once

#include <Core/LogsLevel.h>
#include <Interpreters/WebAssembly/WasmEngine.h>

namespace DB::WebAssembly
{

/// WasmTimeRuntime is a specific implementation of WasmModule using the wasmtime runtime.
class WasmTimeRuntime final : public IWasmEngine
{
public:
    explicit WasmTimeRuntime();

    std::unique_ptr<WasmModule> compileModule(std::string_view wasm_code) const override;
    static void setLogLevel(LogsLevel level);

    ~WasmTimeRuntime() override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

}
