#pragma once

#include <Core/Block.h>

#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/WebAssembly/WasmEngine.h>

#include <Parsers/IAST_fwd.h>

#include <Common/SharedMutex.h>
#include <Common/StopToken.h>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

enum class WasmAbiVersion : uint8_t
{
    RowDirect,
    BufferedV1,
};

String toString(WasmAbiVersion abi_type);
WasmAbiVersion getWasmAbiFromString(const String & str);

class WebAssemblyFunctionSettings
{
public:
    void trySet(const String & name, Field value);
    Field getValue(const String & name) const;

private:
    std::unordered_map<String, Field> settings;
};

class UserDefinedWebAssemblyFunction
{
public:
    virtual MutableColumnPtr executeOnBlock(WebAssembly::WasmCompartment * compartment, const Block & block, ContextPtr context, size_t num_rows, StopToken stop_token) const = 0;

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
    std::shared_ptr<UserDefinedWebAssemblyFunction> addOrReplace(ASTPtr create_function_query, WasmModuleManager & module_manager);

    bool has(const String & function_name);
    FunctionOverloadResolverPtr get(const String & function_name, ContextPtr context);

    /// Returns true if function was removed
    bool dropIfExists(const String & function_name);

    static UserDefinedWebAssemblyFunctionFactory & instance();
private:
    DB::SharedMutex registry_mutex;
    std::unordered_map<String, std::shared_ptr<UserDefinedWebAssemblyFunction>> registry;
};

}
