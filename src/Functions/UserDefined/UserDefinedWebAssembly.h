#pragma once

#include <Core/Block.h>

#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/WebAssembly/WasmEngine.h>

#include <Parsers/IAST_fwd.h>

#include <Common/SharedMutex.h>
#include <Common/StopToken.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

enum class WasmAbiVersion : uint8_t
{
    RowDirect,
    BufferedV1,
    AssemblyScript,
};

String toString(WasmAbiVersion abi_type);
WasmAbiVersion getWasmAbiFromString(const String & str);

class WebAssemblyFunctionSettings
{
public:
    void trySet(const String & name, Field value);
    Field getValue(const String & name) const;
    bool isFuelEnabled() const;
    WebAssembly::FuelMode getFuelMode() const;

private:
    UnorderedMapWithMemoryTracking<String, Field> settings;
};

class UserDefinedWebAssemblyFunction
{
public:
    virtual MutableColumnPtr executeOnBlock(WebAssembly::WasmCompartment * compartment, const Block & block, ContextPtr context, size_t num_rows, StopToken stop_token) const = 0;

    /// True when a call has to place data in, or read data from, the guest's linear memory. Such a
    /// function cannot run in a compartment whose memory can never hold a single page, while one
    /// passing its arguments as WebAssembly values is indifferent to the memory configuration.
    virtual bool requiresGuestLinearMemory() const = 0;

    /// True when a call serializes the whole input block into the guest's linear memory through
    /// `serialization_format`, so what the memory has to hold is the serialized size of a batch.
    /// An ABI that hands the guest one row at a time - `ASSEMBLYSCRIPT` builds a separate object
    /// per row and never reads `serialization_format` - writes no such block, and sizing its
    /// input by a serialization it does not perform would bound it by bytes it never places
    /// there.
    virtual bool serializesInputBlockToGuestMemory() const = 0;

    virtual ~UserDefinedWebAssemblyFunction() = default;

    static std::unique_ptr<UserDefinedWebAssemblyFunction> create(
        std::shared_ptr<WebAssembly::WasmModule> wasm_module_,
        const String & function_name_,
        const Strings & argument_names_,
        const DataTypes & arguments_,
        const DataTypePtr & result_type_,
        WasmAbiVersion abi_type,
        WebAssemblyFunctionSettings function_settings_,
        bool is_deterministic_ = false);

    const String & getInternalFunctionName() const { return function_name; }
    const DataTypes & getArguments() const { return arguments; }
    const Strings & getArgumentNames() const { return argument_names; }
    const DataTypePtr & getResultType() const { return result_type; }
    std::shared_ptr<WebAssembly::WasmModule> getModule() const { return wasm_module; }
    const WebAssemblyFunctionSettings & getSettings() const { return settings; }
    bool getIsDeterministic() const { return is_deterministic; }

protected:

    UserDefinedWebAssemblyFunction(
        std::shared_ptr<WebAssembly::WasmModule> wasm_module_,
        const String & function_name_,
        const Strings & argument_names_,
        const DataTypes & arguments_,
        const DataTypePtr & result_type_,
        WebAssemblyFunctionSettings function_settings_,
        bool is_deterministic_ = false);

    String function_name;
    Strings argument_names;
    DataTypes arguments;
    DataTypePtr result_type;

    std::shared_ptr<WebAssembly::WasmModule> wasm_module;

    WebAssemblyFunctionSettings settings;
    bool is_deterministic = false;
};

class WasmModuleManager;

class UserDefinedWebAssemblyFunctionFactory
{
public:
    struct RegisteredFunction
    {
        String sql_name;
        std::shared_ptr<UserDefinedWebAssemblyFunction> function;
        ASTPtr create_query;
    };

    RegisteredFunction prepareFunction(ASTPtr create_function_query, WasmModuleManager & module_manager) const;
    std::shared_ptr<UserDefinedWebAssemblyFunction> addOrReplace(ASTPtr create_function_query, WasmModuleManager & module_manager);
    void addOrReplace(RegisteredFunction registered_function);
    void replaceAll(VectorWithMemoryTracking<RegisteredFunction> registered_functions);

    bool has(const String & function_name) const;
    FunctionOverloadResolverPtr get(const String & function_name, ContextPtr context);

    /// Fail close before resolving a name that is stored as a WebAssembly UDF.
    /// A `CREATE FUNCTION ... LANGUAGE WASM` definition lives in SQL object storage and outlives the engine
    /// that can run it: the server may be restarted with `allow_experimental_webassembly_udf` turned off, or
    /// on a build that has no WebAssembly engine at all. The definition is then still stored while this
    /// registry is empty, and without this check the name resolves to `UNKNOWN_FUNCTION` or to an
    /// empty-registry `RESOURCE_NOT_FOUND` instead of reporting that WebAssembly support is unavailable.
    static void checkWebAssemblyIsAvailable(const ContextPtr & context);
    /// Returns nullptr if the function is not registered. Useful for non-throwing rewrite-candidate checks.
    FunctionOverloadResolverPtr tryGet(const String & function_name, ContextPtr context);

    /// Returns true if function was removed
    bool dropIfExists(const String & function_name);

    /// Returns all registered WASM functions with their metadata for introspection (e.g. system.functions).
    VectorWithMemoryTracking<RegisteredFunction> getAllFunctions() const;

    static UserDefinedWebAssemblyFunctionFactory & instance();
private:
    struct RegistryEntry
    {
        std::shared_ptr<UserDefinedWebAssemblyFunction> function;
        ASTPtr create_query;
    };

    mutable DB::SharedMutex registry_mutex;
    UnorderedMapWithMemoryTracking<String, RegistryEntry> registry;
};

}
