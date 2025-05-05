#include <cstdint>
#include <ranges>
#include <span>
#include <variant>
#include <Interpreters/WebAssembly/WasmTimeRuntime.h>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <wasmtime.hh>
#include "Common/ElapsedTimeProfileEventIncrement.h"
#include "Common/logger_useful.h"
#include <Common/Exception.h>
#include "Interpreters/WebAssembly/HostApi.h"
#include "Interpreters/WebAssembly/WasmEngine.h"
#include "Interpreters/WebAssembly/WasmTypes.h"

namespace ProfileEvents
{
extern const Event WasmExecuteMicroseconds;
}

namespace DB::ErrorCodes
{
extern const int WASM_ERROR;
}

namespace DB::WebAssembly
{

namespace
{

WasmValKind fromWasmTimeValKind(wasmtime::ValKind value)
{
    switch (value)
    {
        case wasmtime::ValKind::I32:
            return WasmValKind::I32;
        case wasmtime::ValKind::I64:
            return WasmValKind::I64;
        case wasmtime::ValKind::F32:
            return WasmValKind::F32;
        case wasmtime::ValKind::F64:
            return WasmValKind::F64;
        default:
            throw Exception(ErrorCodes::WASM_ERROR, "Unsupported wasmtime type");
    }
}

wasmtime::ValKind toWasmTimeValKind(WasmValKind value)
{
    switch (value)
    {
        case WasmValKind::I32:
            return wasmtime::ValKind::I32;
        case WasmValKind::I64:
            return wasmtime::ValKind::I64;
        case WasmValKind::F32:
            return wasmtime::ValKind::F32;
        case WasmValKind::F64:
            return wasmtime::ValKind::F64;
    }
    __builtin_unreachable();
}

WasmVal fromWasmTimeValue(const wasmtime::Val & wasm_val)
{
    switch (wasm_val.kind())
    {
        case wasmtime::ValKind::I32:
            return static_cast<uint32_t>(wasm_val.i32());
        case wasmtime::ValKind::I64:
            return wasm_val.i64();
        case wasmtime::ValKind::F32:
            return wasm_val.f32();
        case wasmtime::ValKind::F64:
            return wasm_val.f64();
        default:
            throw Exception(ErrorCodes::WASM_ERROR, "Unsupported wasmtime type");
    }
}

wasmtime::Val toWasmTimeValue(WasmVal val)
{
    return std::visit(
        [](auto && arg) -> wasmtime::Val
        {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<uint32_t, T>)
            {
                return wasmtime::Val(static_cast<int32_t>(arg));
            }
            else if constexpr (std::is_same_v<int64_t, T>)
            {
                return wasmtime::Val(arg);
            }
            else if constexpr (std::is_same_v<float, T>)
            {
                return wasmtime::Val(arg);
            }
            else if constexpr (std::is_same_v<double, T>)
            {
                return wasmtime::Val(arg);
            }
            else
            {
                throw Exception(ErrorCodes::WASM_ERROR, "Failed to transform WasmVal to wasmtime::Val: unkown variant underlying type");
            }
        },
        val);
}

wasmtime::FuncType toWasmFunctionType(WasmHostFunction * host_function_ptr)
{
    auto argument_types = host_function_ptr->getArgumentTypes();
    std::vector<wasmtime::ValType> param_types;
    param_types.reserve(argument_types.size());
    for (auto & argument_type : argument_types)
    {
        param_types.emplace_back(toWasmTimeValKind(argument_type));
    }

    std::vector<wasmtime::ValType> result_type;
    result_type.reserve(1);
    if (auto return_type = host_function_ptr->getReturnType())
    {
        result_type.emplace_back(toWasmTimeValKind(return_type.value()));
    }

    return wasmtime::FuncType::from_iters(param_types, result_type);
}

}

WasmTimeRuntime::WasmTimeRuntime()
{
    setLogLevel(LogsLevel::warning);
}

void WasmTimeRuntime::setLogLevel(LogsLevel)
{
}

class WasmTimeCompartment : public WasmCompartment
{
public:
    explicit WasmTimeCompartment(wasmtime::Engine && wasm_engine, wasmtime::Store && wasm_store, wasmtime::Module && wasm_module)
        : engine(std::move(wasm_engine))
        , store(std::move(wasm_store))
        , module(std::move(wasm_module))
    {
    }

    void instantiate(wasmtime::Linker && linker_)
    {
        linker = std::move(linker_);
        auto instantination_result = linker.value().instantiate(store, module);
        if (!instantination_result)
        {
            throw Exception(ErrorCodes::WASM_ERROR, "Failed to instantiate wasm module: {}", instantination_result.err().message());
        }
        instance = std::move(instantination_result.ok());
    }

    void setLastException(Exception e) { last_exception = std::move(e); }

    uint8_t * getMemory(WasmPtr ptr, WasmSizeT size) override
    {
        auto memory_result = instance.value().get(store, "memory");
        if (!memory_result)
        {
            throw Exception(ErrorCodes::WASM_ERROR, "cannot get memory from wasm instance");
        }
        auto memory = std::get<wasmtime::Memory>(memory_result.value());

        auto memory_span = memory.data(store);
        if (ptr + size >= memory_span.size())
        {
            throw Exception(
                ErrorCodes::WASM_ERROR,
                "Cannot get memory at offset {} and size {} from wasm compartment memory with size {}",
                ptr,
                size,
                memory_span.size());
        }
        return &memory_span[ptr];
    }

    uint32_t growMemory(uint32_t num_pages) override
    {
        auto memory_result = instance.value().get(store, "memory");
        if (!memory_result)
        {
            throw Exception(ErrorCodes::WASM_ERROR, "cannot get memory from wasm instance");
        }
        auto memory = std::get<wasmtime::Memory>(memory_result.value());
        auto grow_result = memory.grow(store, num_pages);
        if (!grow_result)
        {
            throw Exception(ErrorCodes::WASM_ERROR, "cannot grow memory of wasm compartment");
        }
        return static_cast<uint32_t>(memory.size(store));
    }

    WasmSizeT getMemorySize() override
    {
        auto memory_result = instance.value().get(store, "memory");
        if (!memory_result)
        {
            throw Exception(ErrorCodes::WASM_ERROR, "cannot get memory from wasm instance");
        }
        auto memory = std::get<wasmtime::Memory>(memory_result.value());
        return static_cast<uint32_t>(memory.size(store));
    }

    void invoke(std::string_view function_name, const std::vector<WasmVal> & params, std::vector<WasmVal> & returns) override
    {
        LOG_TRACE(log, "Function {} invocation started", function_name);
        auto get_function_result = instance.value().get(store, function_name);
        if (!get_function_result.has_value())
        {
            throw Exception(ErrorCodes::WASM_ERROR, "Function '{}' is not found in compartment", function_name);
        }

        auto run = std::get<wasmtime::Func>(get_function_result.value());

        size_t params_count = run.type(store)->params().size();

        if (params_count != params.size())
            throw Exception(
                ErrorCodes::WASM_ERROR,
                "Function {} invoked with wrong number of arguments {}, "
                "expected {}",
                function_name,
                params.size(),
                params_count);

        std::vector<wasmtime::Val> params_values;
        params_values.reserve(params.size());
        for (auto param : params)
            params_values.emplace_back(toWasmTimeValue(param));

        std::vector<wasmtime::Val> returns_values;
        {
            last_exception.reset();

            ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::WasmExecuteMicroseconds);
            auto call_results = run.call(store, params_values);

            if (last_exception)
                last_exception->rethrow();

            if (!call_results)
            {
                throw Exception(ErrorCodes::WASM_ERROR, "Failed to execute {} function: {}", function_name, call_results.err().message());
            }
            returns_values = std::move(call_results.ok());
        }

        returns.clear();
        std::transform(returns_values.begin(), returns_values.end(), std::back_inserter(returns), fromWasmTimeValue);
        LOG_TRACE(log, "Function {} invocation ended", function_name);
    }

private:
    wasmtime::Engine engine;
    wasmtime::Store store;
    wasmtime::Module module;
    std::optional<wasmtime::Instance> instance;
    std::optional<wasmtime::Linker> linker;

    std::optional<Exception> last_exception;

    LoggerPtr log = getLogger("WasmTimeCompartment");
};

namespace
{
wasmtime::Result<std::monostate, wasmtime::Trap> callHostFunction(
    WasmTimeCompartment * compartment,
    WasmHostFunction * host_function_ptr,
    const wasmtime::Caller caller [[maybe_unused]],
    const wasmtime::Span<const wasmtime::Val> params,
    wasmtime::Span<wasmtime::Val> results)
{
    try
    {
        auto argument_types = host_function_ptr->getArgumentTypes();
        if (argument_types.size() != params.size())
        {
            throw Exception(
                ErrorCodes::WASM_ERROR,
                "Function {} was called from wasm with different number of arguments {} != {}",
                formatFunctionDeclaration(*host_function_ptr),
                params.size(),
                argument_types.size());
        }
        std::vector<WasmVal> args(argument_types.size());
        for (size_t i = 0; i < params.size(); ++i)
        {
            if (fromWasmTimeValKind(params[i].kind()) != argument_types[i])
                throw Exception(
                    ErrorCodes::WASM_ERROR,
                    "Function {} invoked with wrong argument types [{}]",
                    formatFunctionDeclaration(*host_function_ptr),
                    fmt::join(args | std::views::transform(getWasmValKind), ", "));
            args[i] = fromWasmTimeValue(params[i]);
        }

        auto result_val = (*host_function_ptr)(compartment, args);
        if (result_val)
        {
            if (results.size() != 1)
            {
                throw Exception(
                    ErrorCodes::WASM_ERROR,
                    "Function {} invoked with different number of return values 1 != {}",
                    formatFunctionDeclaration(*host_function_ptr),
                    results.size());
            }
            results[0] = toWasmTimeValue(result_val.value());
        }
    }
    catch (Exception & e)
    {
        /// The runtime cannot handle exceptions
        /// We catch them here and store them to rethrow after the wasm code returns.
        compartment->setLastException(std::move(e));
        return wasmtime::Trap(fmt::format("Got exception while trying to call host function from wasm"));
    }


    return std::monostate();
}
}

WasmFunctionDeclarationPtr buildFunctionDeclaration(std::string_view function_name, wasmtime::FuncType::Ref function_info)
{
    if (function_info.results().size() > 1)
        throw Exception(ErrorCodes::WASM_ERROR, "Function '{}' has more than one return value", function_name);

    std::optional<WasmValKind> return_type;
    if (function_info.results().size() == 1)
    {
        return_type = fromWasmTimeValKind(function_info.results().begin()->kind());
    }

    std::vector<WasmValKind> argument_types;
    argument_types.reserve(function_info.params().size());
    for (auto function_argument : function_info.params())
    {
        argument_types.emplace_back(fromWasmTimeValKind(function_argument.kind()));
    }

    return std::make_unique<WasmFunctionDeclaration>(function_name, std::move(argument_types), return_type);
}

class WasmTimeModule : public WasmModule
{
public:
    explicit WasmTimeModule(std::span<uint8_t> original_code_bytes, wasmtime::Engine && wasm_engine, wasmtime::Module && wasm_module)
        : code_bytes(original_code_bytes.begin(), original_code_bytes.end())
        , engine(std::move(wasm_engine))
        , store(engine)
        , module(std::move(wasm_module))
    {
        // Compiled module stored before instantiation only to inspect it
        // (i.e inspect imports/exports). During instantiation it will be compiled again,
        // because we don't know compilation settings beofre instantiation

        all_exports_list = module.exports();
        if (all_exports_list.size() >= 512)
            throw Exception(ErrorCodes::WASM_ERROR, "Module has too many exports");

        for (auto export_type : all_exports_list)
        {
            auto export_info = wasmtime::ExternType::from_export(export_type);
            if (auto * export_func = std::get_if<wasmtime::FuncType::Ref>(&export_info))
            {
                function_exports_map.insert({std::string(export_type.name()), *export_func});
            }
        }

        all_imports_list = module.imports();
        if (all_imports_list.size() >= 512)
            throw Exception(ErrorCodes::WASM_ERROR, "Module has too many imports");

        for (auto import_type : all_imports_list)
        {
            auto import_info = wasmtime::ExternType::from_import(import_type);
            if (auto * import_func = std::get_if<wasmtime::FuncType::Ref>(&import_info))
            {
                function_imports_map.insert({std::string(import_type.name()), *import_func});
            }
        }
    }

    std::unique_ptr<WasmCompartment> instantiate(Config cfg) const override
    {
        LOG_TRACE(log, "Module instantiation started");
        wasmtime::Config instance_config;
        if (cfg.fuel_limit)
        {
            instance_config.consume_fuel(true);
        }
        wasmtime::Engine engine_for_instance(std::move(instance_config));
        wasmtime::Linker linker_for_instance(engine_for_instance);

        wasmtime::Store store_for_instance(engine_for_instance);
        if (cfg.memory_limit)
        {
            store_for_instance.limiter(cfg.memory_limit, -1, -1, -1, -1);
        }
        if (cfg.fuel_limit)
        {
            const auto result = store_for_instance.context().set_fuel(cfg.fuel_limit);
            if (!result)
            {
                throw Exception(ErrorCodes::WASM_ERROR, "Failed to set fuel to wasm instance: {}", result.err().message());
            }
        }

        std::span<uint8_t> code_bytes_span(reinterpret_cast<uint8_t *>(const_cast<unsigned char *>(code_bytes.data())), code_bytes.size());
        auto compilation_result = wasmtime::Module::compile(engine_for_instance, code_bytes_span);
        if (!compilation_result)
        {
            throw Exception(ErrorCodes::WASM_ERROR, "Failed to compile wasm code: {}", compilation_result.err().message());
        }
        auto module_for_instance = compilation_result.ok();


        auto compartment = std::make_unique<WasmTimeCompartment>(
            std::move(engine_for_instance), std::move(store_for_instance), std::move(module_for_instance));

        LOG_TRACE(log, "add host function started");
        for (const auto & host_function_ptr : host_functions)
        {
            auto add_host_func_result = linker_for_instance.func_new(
                "env",
                host_function_ptr->getName(),
                toWasmFunctionType(host_function_ptr.get()),
                [compartment_ptr = compartment.get(), module_log = this->log, &host_function_ptr](
                    wasmtime::Caller caller,
                    wasmtime::Span<const wasmtime::Val> params,
                    wasmtime::Span<wasmtime::Val> results) -> wasmtime::Result<std::monostate, wasmtime::Trap>
                {
                    LOG_TRACE(module_log, "host callback invocation started");
                    return callHostFunction(compartment_ptr, host_function_ptr.get(), caller, params, results);
                }

            );
            if (!add_host_func_result)
            {
                throw Exception(
                    ErrorCodes::WASM_ERROR, "Failed to add host function to module instance: {}", add_host_func_result.err().message());
            }
        }
        LOG_TRACE(log, "add host function ended");

        compartment->instantiate(std::move(linker_for_instance));

        LOG_TRACE(log, "module instantiation ended");
        return compartment;
    }


    std::vector<WasmFunctionDeclarationPtr> getImports() const override
    {
        std::vector<WasmFunctionDeclarationPtr> result;
        result.reserve(function_imports_map.size());

        for (const auto & [function_name, function_info] : function_imports_map)
        {
            result.emplace_back(buildFunctionDeclaration(function_name, function_info));
        }

        return result;
    }

    void addImport(std::unique_ptr<WasmHostFunction> import_host_function) override
    {
        host_functions.emplace_back(std::move(import_host_function));
    }

    WasmFunctionDeclarationPtr getExport(std::string_view function_name) const override
    {
        auto export_it = function_exports_map.find(function_name);
        if (export_it == function_exports_map.end())
            return nullptr;
        return buildFunctionDeclaration(function_name, export_it->second);
    }

private:
    // Source code is explicitly stored to compile it again during instantiation,
    // with known compilation settings
    std::vector<uint8_t> code_bytes;
    // We need to store these objects to inspect imports/exports before instantiation
    wasmtime::Engine engine;
    wasmtime::Store store;
    wasmtime::Module module;

    wasmtime::ExportType::List all_exports_list;
    std::map<std::string, wasmtime::FuncType::Ref, std::less<>> function_exports_map;

    wasmtime::ImportType::List all_imports_list;
    std::map<std::string, wasmtime::FuncType::Ref, std::less<>> function_imports_map;

    std::vector<std::unique_ptr<WasmHostFunction>> host_functions;

    LoggerPtr log = getLogger("WasmTimeModule");
};

std::unique_ptr<WasmModule> WasmTimeRuntime::createModule(std::string_view wasm_code) const
{
    std::span<uint8_t> bytes(reinterpret_cast<uint8_t *>(const_cast<char *>(wasm_code.data())), wasm_code.size());
    wasmtime::Engine engine;
    auto compilation_result = wasmtime::Module::compile(engine, bytes);
    if (!compilation_result)
    {
        throw Exception(ErrorCodes::WASM_ERROR, "Failed to compile wasm code: {}", compilation_result.err().message());
    }
    auto module = compilation_result.ok();

    return std::make_unique<WasmTimeModule>(bytes, std::move(engine), std::move(module));
};

}
