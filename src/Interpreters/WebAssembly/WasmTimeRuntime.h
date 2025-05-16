#pragma once

#include <Core/LogsLevel.h>
#include <Interpreters/WebAssembly/WasmEngine.h>

namespace DB::WebAssembly
{

/// WasmEdgeRuntime is a specific implementation of WasmModule using the WasmEdge runtime.
class WasmTimeRuntime final : public IWasmEngine
{
public:
    explicit WasmTimeRuntime();

    std::unique_ptr<WasmModule> createModule(std::string_view wasm_code) const override;
    static void setLogLevel(LogsLevel level);
};

}
