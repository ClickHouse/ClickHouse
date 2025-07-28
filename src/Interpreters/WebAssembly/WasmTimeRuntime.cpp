#include <Interpreters/WebAssembly/WasmTimeRuntime.h>
#include "config.h"

#include <Common/Exception.h>

#if USE_WASMTIME

#include <cstdint>
#include <ranges>
#include <span>
#include <variant>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/logger_useful.h>
#include <Interpreters/WebAssembly/HostApi.h>
#include <Interpreters/WebAssembly/WasmEngine.h>
#include <Interpreters/WebAssembly/WasmTypes.h>

#include <wasmtime.hh>

namespace ProfileEvents
{
extern const Event WasmExecuteMicroseconds;
}

namespace DB::ErrorCodes
{
extern const int WASM_ERROR;
extern const int LOGICAL_ERROR;
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
    UNREACHABLE();
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
                throw Exception(ErrorCodes::WASM_ERROR, "Failed to transform WasmVal to wasmtime::Val: unknown variant underlying type");
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

struct WasmTimeRuntime::Impl
{
    static wasmtime::Config getConfig()
    {
        wasmtime::Config config;
        config.consume_fuel(true);
        return config;
    }

    explicit Impl()
        : engine(std::make_shared<wasmtime::Engine>(getConfig()))
    {}

    std::shared_ptr<wasmtime::Engine> engine;
};

WasmTimeRuntime::WasmTimeRuntime()
    : impl(std::make_unique<Impl>())
{
    setLogLevel(LogsLevel::warning);
}

void WasmTimeRuntime::setLogLevel(LogsLevel)
{
}

class WasmTimeCompartment : public WasmCompartment
{
public:
    explicit WasmTimeCompartment(wasmtime::Store && wasm_store, wasmtime::Instance && instance_, WasmModule::Config cfg_)
        : store(std::move(wasm_store))
        , instance(std::move(instance_))
        , cfg(std::move(cfg_))
    {
        store.context().set_data(this);
    }

    void setLastException(Exception e) { last_exception = std::move(e); }

    uint8_t * getMemory(WasmPtr ptr, WasmSizeT size) override
    {
        auto memory_span = getMemory().data(store);
        if (ptr + size >= memory_span.size())
        {
            throw Exception(
                ErrorCodes::WASM_ERROR,
                "Cannot get memory at offset {} and size {} from wasm compartment memory with size {}",
                ptr, size, memory_span.size());
        }
        return &memory_span[ptr];
    }

    uint32_t growMemory(uint32_t num_pages) override
    {
        auto memory = getMemory();
        auto grow_result = memory.grow(store, num_pages);
        if (!grow_result)
        {
            throw Exception(ErrorCodes::WASM_ERROR, "Cannot grow memory of wasm compartment");
        }
        return static_cast<uint32_t>(memory.size(store));
    }

    WasmSizeT getMemorySize() override { return static_cast<uint32_t>(getMemory().size(store)); }

    void invoke(std::string_view function_name, const std::vector<WasmVal> & params, std::vector<WasmVal> & returns) override
    {
        if (cfg.fuel_limit)
        {
            auto result = store.context().set_fuel(cfg.fuel_limit);
            if (!result)
                throw Exception(ErrorCodes::WASM_ERROR, "Failed to set fuel to wasm instance: {}", result.err().message());
        }

        auto get_function_result = instance.get(store, function_name);
        if (!get_function_result.has_value())
        {
            throw Exception(ErrorCodes::WASM_ERROR, "Function '{}' is not found in compartment", function_name);
        }

        auto wasm_func = std::get<wasmtime::Func>(get_function_result.value());

        size_t params_count = wasm_func.type(store)->params().size();

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
            auto call_results = wasm_func.call(store, params_values);

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
    }

    wasmtime::Memory getMemory()
    {
        auto memory_result = instance.get(store, "memory");
        if (!memory_result || !std::holds_alternative<wasmtime::Memory>(memory_result.value()))
        {
            throw Exception(ErrorCodes::WASM_ERROR, "cannot get memory from wasm instance");
        }
        return std::get<wasmtime::Memory>(memory_result.value());
    }

private:
    wasmtime::Store store;
    wasmtime::Instance instance;

    std::optional<Exception> last_exception;

    WasmModule::Config cfg;
    LoggerPtr log = getLogger("WasmTimeCompartment");
};

namespace
{

wasmtime::Result<std::monostate, wasmtime::Trap> callHostFunction(
    WasmTimeCompartment * compartment,
    WasmHostFunction * host_function_ptr,
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
    explicit WasmTimeModule(std::shared_ptr<wasmtime::Engine> engine_, wasmtime::Module && module_)
        : engine(engine_)
        , module(std::move(module_))
    {
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
        if (!engine)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Engine is not initialized");

        wasmtime::Store store(*engine);
        if (cfg.memory_limit)
            store.limiter(cfg.memory_limit, -1, -1, -1, -1);
        if (cfg.fuel_limit)
        {
            auto result = store.context().set_fuel(cfg.fuel_limit);
            if (!result)
                throw Exception(ErrorCodes::WASM_ERROR, "Failed to set fuel to wasm instance: {}", result.err().message());
        }

        wasmtime::Linker linker(*engine);
        for (const auto & host_function_ptr : host_functions)
        {
            auto add_host_func_result = linker.func_new(
                "env",
                host_function_ptr->getName(),
                toWasmFunctionType(host_function_ptr.get()),
                [&host_function_ptr](
                    wasmtime::Caller caller,
                    wasmtime::Span<const wasmtime::Val> params,
                    wasmtime::Span<wasmtime::Val> results) -> wasmtime::Result<std::monostate, wasmtime::Trap>
                {
                    auto * compartment_ptr = std::any_cast<WasmTimeCompartment *>(caller.context().get_data());
                    return callHostFunction(compartment_ptr, host_function_ptr.get(), params, results);
                }

            );
            if (!add_host_func_result)
            {
                throw Exception(
                    ErrorCodes::WASM_ERROR, "Failed to add host function to module instance: {}", add_host_func_result.err().message());
            }
        }

        auto instantination_result = linker.instantiate(store, module);
        if (!instantination_result)
            throw Exception(ErrorCodes::WASM_ERROR, "Failed to instantiate wasm module: {}", instantination_result.err().message());

        return std::make_unique<WasmTimeCompartment>(std::move(store), std::move(instantination_result.ok()), std::move(cfg));
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
    std::shared_ptr<wasmtime::Engine> engine;
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
    auto compilation_result = wasmtime::Module::compile(*impl->engine, bytes);
    if (!compilation_result)
    {
        throw Exception(ErrorCodes::WASM_ERROR, "Failed to compile wasm code: {}", compilation_result.err().message());
    }
    auto module = compilation_result.ok();

    return std::make_unique<WasmTimeModule>(impl->engine, std::move(module));
};

WasmTimeRuntime::~WasmTimeRuntime() = default;

}

#else

namespace DB::ErrorCodes
{
extern const int SUPPORT_IS_DISABLED;
}

namespace DB::WebAssembly
{


struct WasmTimeRuntime::Impl
{
};

WasmTimeRuntime::WasmTimeRuntime() : impl(std::make_unique<Impl>()) { }

std::unique_ptr<WasmModule> WasmTimeRuntime::createModule(std::string_view /* wasm_code */) const
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Wasmtime support is disabled");
}

void WasmTimeRuntime::setLogLevel(LogsLevel /* level */)
{
}

WasmTimeRuntime::~WasmTimeRuntime() = default;

}

#endif
