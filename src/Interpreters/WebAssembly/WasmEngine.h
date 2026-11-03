#pragma once

#include <Interpreters/WebAssembly/WasmTypes.h>

#include <cstdint>
#include <span>
#include <Common/StopToken.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB::WebAssembly
{

class WasmHostFunction;

/// A WebAssembly linear memory page is 64 KiB by specification.
constexpr size_t WASM_PAGE_SIZE = 65536;

/// Only `wasm32` modules are supported, and their linear memory is addressed by 32-bit offsets,
/// so it can never hold more than 4 GiB: `memory.grow` past 65536 pages fails inside the guest
/// however much the host allows. A ceiling reported above this would be unreachable.
///
/// Typed as `uint64_t` rather than `size_t` because this header is also compiled for a `wasm32`
/// target itself - the standalone SQL parser of `Build (wasm_parser)` reaches it through
/// `ASTCreateWasmFunctionQuery.h` - where a `size_t` is 32 bits and cannot hold 4 GiB at all.
constexpr uint64_t WASM_MAX_LINEAR_MEMORY_SIZE = static_cast<uint64_t>(1) << 32;

enum class FuelMode : uint8_t
{
    Enabled,
    Disabled,
};

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

    /// Get a view of guest memory given a handle and size
    virtual std::span<uint8_t> getMemory(WasmPtr ptr, WasmSizeT size) = 0;

    /// Return the current size of the WASM linear memory in bytes, empty when the module
    /// exports no linear memory at all - which is not the same as exporting one that holds
    /// no pages yet.
    virtual std::optional<size_t> getLinearMemorySize() const = 0;

    /// Return the size, in bytes, the module declares its linear memory to start with, empty
    /// when the module exports no linear memory at all. Unlike the current size, this does not
    /// move with `memory.grow`, so a caller sizing work against it gets the same answer for
    /// every instance of the module whatever earlier calls made it grow to.
    virtual std::optional<size_t> getInitialLinearMemorySize() const = 0;

    /// Return the effective ceiling, in bytes, that the WASM linear memory can actually
    /// reach in this engine - not merely the configured `memory_limit`. An engine must
    /// account for its own rounding and for a smaller maximum declared by the module
    /// itself, so that a caller sizing work against this value never proposes a batch
    /// the guest can never allocate.
    ///
    /// Empty when nothing bounds growth. The current size must not be reported in that case -
    /// it is what the guest happens to have allocated so far, not a limit, and treating it as
    /// one turns a growable memory into a hard cap. A `wasm32` engine always has a bound, since
    /// neither the host cap nor a maximum declared by the module can lift the memory past
    /// `WASM_MAX_LINEAR_MEMORY_SIZE`, so for it the empty case only means the engine cannot
    /// tell what the ceiling is.
    virtual std::optional<size_t> getMaxLinearMemorySize() const = 0;

    /// Invoke a function expecting to return a single value of specific result type or void, if no return value expected.
    /// If function returns multiple values or different type, an exception is thrown.
    template <typename ResultType>
    ResultType invoke(std::string_view function_name, const VectorWithMemoryTracking<WasmVal> & params, StopToken stop_token);

protected:
    /// Implementation provides generic invocation returning all result values of generic WasmVal type.
    virtual VectorWithMemoryTracking<WasmVal> invokeImpl(std::string_view function_name, const VectorWithMemoryTracking<WasmVal> & params, StopToken stop_token) = 0;
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
        Config() = delete;
        explicit Config(FuelMode fuel_mode_) : fuel_mode(fuel_mode_) {}

        size_t memory_limit = 0;
        size_t fuel_limit = 0;
        FuelMode fuel_mode;

        bool usesFuelAccounting() const { return fuel_mode == FuelMode::Enabled; }
        bool hasFiniteFuelLimit() const { return usesFuelAccounting() && fuel_limit != 0; }
    };

    /** Creates a new instance of WasmCompartment using the code of this module.
      * During instantiation, functions from WASM_HOST_API_FUNCTIONS (see HostApi.h) must be registered as imported functions.
      * `stop_token` is observed while the module's `(start)` function (if any) runs — letting the caller cancel a
      * hanging start function via the same path used to cancel regular calls.
      */
    virtual std::unique_ptr<WasmCompartment> instantiate(Config cfg, StopToken stop_token) const = 0;

    virtual VectorWithMemoryTracking<WasmFunctionDeclaration> getImports() const = 0;
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
    virtual std::unique_ptr<WasmModule> compileModule(
        std::string_view module_name,
        std::string_view wasm_code,
        FuelMode fuel_mode) const = 0;
    virtual ~IWasmEngine() = default;
};

}
