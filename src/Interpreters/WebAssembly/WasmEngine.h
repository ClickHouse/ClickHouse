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
    WasmCompartment();

    virtual ~WasmCompartment() = default;

    /// Get a pointer to guest memory given a handle
    virtual uint8_t * getMemory(WasmPtr ptr, WasmSizeT size) = 0;

    /// Invoke a function expecting to return a single value of specific result type or void, if no return value expected.
    /// If function returns multiple values or different type, an exception is thrown.
    template <typename ResultType>
    ResultType invoke(std::string_view function_name, const std::vector<WasmVal> & params);

protected:
    /// Implementation provides generic invocation returning all result values of generic WasmVal type.
    virtual std::vector<WasmVal> invokeImpl(std::string_view function_name, const std::vector<WasmVal> & params) = 0;
};

/** WasmModule represents a WebAssembly module, typically containing code, imports and exports.
  * Module can be instantiated to create a WasmCompartment.
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

    virtual std::vector<WasmFunctionDeclaration> getImports() const = 0;
    virtual void linkFunction(WasmHostFunction host_function) = 0;

    virtual WasmFunctionDeclaration getExport(std::string_view function_name) const = 0;

    virtual ~WasmModule() = default;
};

/** IWasmEngine is responsible for compiling WebAssembly code into WasmModule instances for a specific runtime.
  * It contains global state for managing WebAssembly modules, including type of runtime used and runtime specific configurations.
  */
class IWasmEngine
{
public:
    virtual std::unique_ptr<WasmModule> compileModule(std::string_view wasm_code) const = 0;
    virtual ~IWasmEngine() = default;
};

}
