#pragma once

#include <Interpreters/WebAssembly/WasmTypes.h>

namespace DB::WebAssembly

{

class WasmHostFunction;

/** WasmCompartment is an instantiated WebAssembly module.
  * It provides an interface to invoke WebAssembly functions and access memory within the module.
  * Each compartment is isolated, containing its own memory, and set of imported and exported functions, etc.
  * It is the core of WebAssembly execution and implementing this class and WasmModule
  * provides WebAssembly functionality for a concrete runtime backend.
  */
class WasmCompartment
{
public:
    virtual ~WasmCompartment() = default;

    virtual uint8_t * getMemory(WasmPtr ptr, WasmSizeT size) = 0;
    virtual WasmPtr growMemory(WasmSizeT num_pages) = 0;

    /// Returns the current memory size in bytes
    virtual WasmSizeT getMemorySize() = 0;

    virtual void invoke(std::string_view function_name, const std::vector<WasmVal> & params, std::vector<WasmVal> & returns) = 0;

    template <typename ResultType>
    ResultType invoke(std::string_view function_name, const std::vector<WasmVal> & params);
};

/** WasmModule represents a WebAssembly module, typically containing WebAssembly code,
  * which can be instantiated to create a WasmCompartment.
  * The specific form of the code and instantiation behavior depends on the runtime implementation.
  */
class WasmModule
{
public:
    struct Config
    {
        size_t memory_limit;
        size_t fuel_limit;
    };

    /** Creates a new instance of WasmCompartment using the code of this module.
      * During instantiation, functions from WASM_HOST_API_FUNCTIONS (see HostApi.h) must be registered as imported functions.
      */
    virtual std::unique_ptr<WasmCompartment> instantiate(Config cfg) const = 0;

    virtual std::vector<WasmFunctionDeclarationPtr> getImports() const = 0;
    virtual void addImport(std::unique_ptr<WasmHostFunction> host_function) = 0;

    virtual WasmFunctionDeclarationPtr getExport(std::string_view function_name) const = 0;

    virtual ~WasmModule() = default;
};

class IWasmEngine
{
public:
    virtual std::unique_ptr<WasmModule> createModule(std::string_view wasm_code) const = 0;
    virtual ~IWasmEngine() = default;
};

}
