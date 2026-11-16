#pragma once

#include <Interpreters/WebAssembly/WasmEngine.h>
#include <Core/LogsLevel.h>

namespace DB::WebAssembly
{

/// WasmEdgeRuntime is a specific implementation of WasmModule using the WasmEdge runtime.
class WasmEdgeRuntime final : public IWasmEngine
{
public:
    explicit WasmEdgeRuntime();

    std::unique_ptr<WasmModule> compileModule(std::string_view module_name, std::string_view wasm_code, FuelMode fuel_mode) const override;
    bool requiresFuelSpecialization() const override { return false; }
    static void setLogLevel(LogsLevel level);
};

}
