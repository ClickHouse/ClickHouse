#include <Interpreters/WebAssembly/WasmTimeRuntime.h>
#include "config.h"

#include <Common/Exception.h>

#if USE_WASMTIME

#include <cstdint>
#include <limits>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <variant>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <base/MemorySanitizer.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/logger_useful.h>
#include <Interpreters/WebAssembly/HostApi.h>
#include <Interpreters/WebAssembly/WasmEngine.h>
#include <Interpreters/WebAssembly/WasmTypes.h>

#include <wasmtime.hh>

namespace ProfileEvents
{
    extern const Event WasmGuestExecuteMicroseconds;
}

namespace DB::ErrorCodes
{
    extern const int WASM_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace DB::WebAssembly
{

namespace
{

void setStoreFuel(wasmtime::Store::Context ctx, const WasmModule::Config & cfg, std::string_view phase)
{
    if (!cfg.usesFuelAccounting())
        return;

    /// `wasmtime::Context::set_fuel` returns an error when the engine was created
    /// with `consume_fuel(false)`. The early return above keeps us off that path.
    const auto fuel = cfg.fuel_limit ? cfg.fuel_limit : std::numeric_limits<uint64_t>::max();
    auto result = ctx.set_fuel(fuel);
    if (!result)
        throw Exception(ErrorCodes::WASM_ERROR, "Failed to set fuel for {}: {}", phase, result.err().message());
}

wasmtime::Module compileModuleWithEngine(wasmtime::Engine & engine, std::string_view module_bytes, std::string_view phase)
{
    std::span<uint8_t> bytes(reinterpret_cast<uint8_t *>(const_cast<char *>(module_bytes.data())), module_bytes.size());
    auto result = wasmtime::Module::compile(engine, bytes);
    if (!result)
        throw Exception(ErrorCodes::WASM_ERROR, "Failed to compile wasm code ({}): {}", phase, result.err().message());
    return std::move(result.ok());
}

}

template <WasmValKind val_kind>
auto wasmtimeToNative(const wasmtime::Val & val)
{
    if constexpr (val_kind == WasmValKind::I32)
        return val.i32();
    else if constexpr (val_kind == WasmValKind::I64)
        return val.i64();
    else if constexpr (val_kind == WasmValKind::F32)
        return val.f32();
    else if constexpr (val_kind == WasmValKind::F64)
        return val.f64();
    else if constexpr (val_kind == WasmValKind::V128)
        return val.v128();
    else
        static_assert(false, "Unsupported WasmValKind");
}

wasmtime::ValKind toWasmTimeValKind(WasmValKind value)
{
    #define M(T) \
        if (value == WasmValKind::T) \
            return wasmtime::ValKind::T;

    APPLY_FOR_WASM_TYPES(M)
    #undef M
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported wasm implementation type");
}

WasmValKind fromWasmTimeValKind(wasmtime::ValKind val_type)
{
    #define M(T) \
        if (wasmtime::ValKind::T == val_type) \
            return WasmValKind::T;

    APPLY_FOR_WASM_TYPES(M)
    #undef M
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported wasm implementation type");
}

WasmVal fromWasmTimeValue(const wasmtime::Val & wasm_val)
{
    #define M(T) \
    { \
        constexpr auto Index = std::to_underlying(WasmValKind::T); \
        if (wasmtime::ValKind::T == wasm_val.kind()) \
        { \
            using WasmType = std::variant_alternative_t<Index, WasmVal>; \
            return std::bit_cast<WasmType>(wasmtimeToNative<WasmValKind::T>(wasm_val)); \
        } \
    }
    APPLY_FOR_WASM_TYPES(M)
    #undef M
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported wasm implementation type");
}

wasmtime::Val toWasmTimeValue(WasmVal val)
{
    #define M(T) \
    { \
        using Type = decltype(wasmtimeToNative<WasmValKind::T>(std::declval<wasmtime::Val>())); \
        constexpr auto Index = std::to_underlying(WasmValKind::T); \
        if (val.index() == Index) \
            return wasmtime::Val(std::bit_cast<Type>(std::get<Index>(val))); \
    }

    APPLY_FOR_WASM_TYPES(M)
    #undef M
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported wasm implementation type");
}

wasmtime::FuncType toWasmFunctionType(const WasmFunctionDeclaration & host_function_decl)
{
    auto argument_types = host_function_decl.getArgumentTypes();
    std::vector<wasmtime::ValType> param_types;
    param_types.reserve(argument_types.size());
    for (auto & argument_type : argument_types)
    {
        param_types.emplace_back(toWasmTimeValKind(argument_type));
    }

    std::vector<wasmtime::ValType> result_type;
    result_type.reserve(1);
    if (auto return_type = host_function_decl.getReturnType())
    {
        result_type.emplace_back(toWasmTimeValKind(return_type.value()));
    }

    return wasmtime::FuncType::from_iters(param_types, result_type);
}

struct WasmTimeRuntime::Impl
{
    static wasmtime::Config getConfig(bool consume_fuel)
    {
        wasmtime::Config config;
        config.consume_fuel(consume_fuel);
        /// Epoch interruption stays enabled even without fuel consumption because `max_execution_time`,
        /// `KILL QUERY`, and shutdown cancellation must still interrupt `(start)` and exported calls.
        /// Wasmtime epoch checks happen at safepoints, not as a per-instruction tax.
        config.epoch_interruption(true);
        config.signals_based_traps(false);
        config.wasm_exceptions(true);
        return config;
    }

    explicit Impl()
        : engine_with_fuel(getConfig(true))
        , engine_without_fuel(getConfig(false))
    {
    }

    wasmtime::Engine engine_with_fuel;
    wasmtime::Engine engine_without_fuel;
};

WasmTimeRuntime::WasmTimeRuntime()
    : impl(std::make_unique<Impl>())
{
    setLogLevel(LogsLevel::warning);
}

void WasmTimeRuntime::setLogLevel(LogsLevel)
{
}

class WasmTimeCompartment;

/// Single payload stored in `wasmtime::Store` data slot.
/// The compartment pointer is null during instantiation (before `Linker::instantiate` returns),
/// so the host-function trampoline must check it before dereferencing.
struct WasmTimeStoreData
{
    WasmTimeCompartment * compartment = nullptr;
    std::shared_ptr<std::atomic_bool> stop_requested;
};

wasmtime::Result<wasmtime::DeadlineKind> epochDeadlineCallback(
    wasmtime::Store::Context ctx, uint64_t & epoch_deadline_delta)
{
    epoch_deadline_delta += 1;
    const auto & ctx_data = ctx.get_data();
    if (const auto * data = std::any_cast<WasmTimeStoreData>(&ctx_data))
    {
        if (data->stop_requested && data->stop_requested->load())
            return wasmtime::Error("WASM execution was stopped by request");
    }
    return wasmtime::DeadlineKind::Continue;
}

class WasmTimeCompartment : public WasmCompartment
{
public:
    explicit WasmTimeCompartment(const wasmtime::Engine & engine_, wasmtime::Store && wasm_store, wasmtime::Instance && instance_, WasmModule::Config cfg_)
        : engine(engine_)
        , store(std::move(wasm_store))
        , instance(std::move(instance_))
        , cfg(std::move(cfg_))
    {
        store.context().set_data(WasmTimeStoreData{this, stop_requested});
        store.context().set_epoch_deadline(1);
        store.epoch_deadline_callback(
            [](wasmtime::Store::Context ctx, uint64_t & epoch_deadline_delta) { return epochDeadlineCallback(ctx, epoch_deadline_delta); });
    }

    void setLastException(Exception e) { last_exception = std::move(e); }

    std::span<uint8_t> getMemory(WasmPtr ptr, WasmSizeT size) override
    {
        auto memory_span = getMemory().data(store);
        if (size > memory_span.size() || ptr > memory_span.size() - size)
        {
            throw Exception(
                ErrorCodes::WASM_ERROR,
                "Cannot get memory at offset {} and size {} from wasm compartment memory with size {}",
                ptr, size, memory_span.size());
        }
        return memory_span.subspan(ptr, size);
    }

    VectorWithMemoryTracking<WasmVal> invokeImpl(std::string_view function_name, const VectorWithMemoryTracking<WasmVal> & params, StopToken stop_token) override
    {
        setStoreFuel(store.context(), cfg, "function call");

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

            stop_requested->store(false);
            StopCallback stop_callback(stop_token, [this, function_name]
            {
                LOG_DEBUG(log, "Stop requested for function '{}'", function_name);
                stop_requested->store(true);
                engine.increment_epoch();
            });

            ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::WasmGuestExecuteMicroseconds);
            auto call_results = wasm_func.call(store, params_values);

            if (last_exception)
                last_exception->rethrow();

            if (!call_results)
            {
                throw Exception(ErrorCodes::WASM_ERROR, "Failed to execute {} function: {}", function_name, call_results.err().message());
            }
            returns_values = std::move(call_results.ok());
        }

        __msan_unpoison(returns_values.data(), returns_values.size() * sizeof(wasmtime::Val));
        return std::ranges::to<VectorWithMemoryTracking<WasmVal>>(returns_values | std::views::transform(fromWasmTimeValue));
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
    const wasmtime::Engine & engine;
    wasmtime::Store store;
    wasmtime::Instance instance;

    std::shared_ptr<std::atomic_bool> stop_requested = std::make_shared<std::atomic_bool>(false);

    std::optional<Exception> last_exception;

    WasmModule::Config cfg;
    LoggerPtr log = getLogger("WasmTimeCompartment");
};

namespace
{

wasmtime::Result<std::monostate, wasmtime::Trap> callHostFunction(
    WasmTimeCompartment * compartment,
    const WasmHostFunction * host_function_ptr,
    const wasmtime::Span<const wasmtime::Val> params,
    wasmtime::Span<wasmtime::Val> results)
{
    const auto & func_decl = host_function_ptr->getFunctionDeclaration();
    try
    {
        const auto & argument_types = func_decl.getArgumentTypes();
        if (argument_types.size() != params.size())
        {
            throw Exception(
                ErrorCodes::WASM_ERROR,
                "Function {} was called from wasm with different number of arguments {} != {}",
                formatFunctionDeclaration(func_decl),
                params.size(),
                argument_types.size());
        }
        VectorWithMemoryTracking<WasmVal> args(argument_types.size());
        for (size_t i = 0; i < params.size(); ++i)
        {
            if (fromWasmTimeValKind(params[i].kind()) != argument_types[i])
                throw Exception(
                    ErrorCodes::WASM_ERROR,
                    "Function {} invoked with wrong argument types [{}]",
                    formatFunctionDeclaration(func_decl),
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
                    formatFunctionDeclaration(func_decl),
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

WasmFunctionDeclaration buildFunctionDeclaration(std::string_view module_name, std::string_view function_name, wasmtime::FuncType::Ref function_info)
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

    return WasmFunctionDeclaration(module_name, function_name, std::move(argument_types), return_type);
}

class WasmTimeModule : public WasmModule
{
public:
    explicit WasmTimeModule(
        std::string_view module_name_,
        wasmtime::Engine engine_,
        wasmtime::Module && module_,
        FuelMode fuel_mode_)
        : engine(std::move(engine_))
        , module(std::move(module_))
        , fuel_mode(fuel_mode_)
        , module_name(module_name_)
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

    }

    std::unique_ptr<WasmCompartment> instantiate(Config cfg, StopToken stop_token) const override
    {
        if (cfg.fuel_mode != fuel_mode)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "WebAssembly module fuel mode does not match instantiation config");

        wasmtime::Store store(engine);
        if (cfg.memory_limit)
            store.limiter(cfg.memory_limit, -1, -1, -1, -1);

        setStoreFuel(store.context(), cfg, "wasm module instantiation");

        /// The module's `(start)` function runs as part of `Linker::instantiate` below, so we set up
        /// epoch interruption *before* instantiate. The long-lived `WasmTimeCompartment` does not
        /// exist yet, so we install a `WasmTimeStoreData` with `compartment == nullptr`. The
        /// `WasmTimeCompartment` constructor will overwrite the data slot with its own pointer
        /// once it is constructed.
        store.context().set_epoch_deadline(1);
        auto stop_requested = std::make_shared<std::atomic_bool>(false);
        store.context().set_data(WasmTimeStoreData{nullptr, stop_requested});
        store.epoch_deadline_callback(
            [](wasmtime::Store::Context ctx, uint64_t & epoch_deadline_delta) { return epochDeadlineCallback(ctx, epoch_deadline_delta); });

        StopCallback stop_callback(stop_token, [this, stop_requested]
        {
            LOG_DEBUG(log, "Stop requested for wasm module instantiation");
            stop_requested->store(true);
            engine.increment_epoch();
        });

        wasmtime::Linker linker(engine);
        for (const auto & host_function : host_functions)
        {
            const auto & func_decl = host_function.getFunctionDeclaration();
            auto add_host_func_result = linker.func_new(
                "env",
                func_decl.getName(),
                toWasmFunctionType(func_decl),
                [host_function_raw_ptr = &host_function](
                    wasmtime::Caller caller,
                    wasmtime::Span<const wasmtime::Val> params,
                    wasmtime::Span<wasmtime::Val> results) -> wasmtime::Result<std::monostate, wasmtime::Trap>
                {
                    /// False positive (?)
                    /// FIXME: try making a small repro
                    /// https://github.com/bytecodealliance/wasmtime/issues/7935#issuecomment-1944027164
                    __msan_unpoison(params.data(), params.size_bytes());
                    const auto * store_data = std::any_cast<WasmTimeStoreData>(&caller.context().get_data());
                    if (!store_data || !store_data->compartment)
                        return wasmtime::Trap("Host function called before WASM compartment is fully initialized");
                    return callHostFunction(store_data->compartment, host_function_raw_ptr, params, results);
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

        return std::make_unique<WasmTimeCompartment>(engine, std::move(store), std::move(instantination_result.ok()), std::move(cfg));
    }


    VectorWithMemoryTracking<WasmFunctionDeclaration> getImports() const override
    {
        VectorWithMemoryTracking<WasmFunctionDeclaration> result;

        for (auto import_type : all_imports_list)
        {
            auto import_info = wasmtime::ExternType::from_import(import_type);
            if (auto * import_func = std::get_if<wasmtime::FuncType::Ref>(&import_info))
            {
                result.emplace_back(buildFunctionDeclaration(import_type.module(), import_type.name(), *import_func));
            }
        }

        return result;
    }

    void linkFunction(WasmHostFunction import_host_function) override
    {
        host_functions.emplace_back(std::move(import_host_function));
    }

    WasmFunctionDeclaration getExport(std::string_view function_name) const override
    {
        auto export_it = function_exports_map.find(function_name);
        if (export_it == function_exports_map.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function '{}' is not found in module exports", function_name);
        return buildFunctionDeclaration(module_name, function_name, export_it->second);
    }

private:
    mutable wasmtime::Engine engine;
    wasmtime::Module module;
    FuelMode fuel_mode;

    wasmtime::ExportType::List all_exports_list;
    std::map<std::string, wasmtime::FuncType::Ref, std::less<>> function_exports_map;

    wasmtime::ImportType::List all_imports_list;

    std::string module_name;

    std::vector<WasmHostFunction> host_functions;

    LoggerPtr log = getLogger("WasmTimeModule");
};

std::unique_ptr<WasmModule> WasmTimeRuntime::compileModule(
    std::string_view module_name,
    std::string_view wasm_code,
    FuelMode fuel_mode) const
{
    auto & engine = fuel_mode == FuelMode::Enabled ? impl->engine_with_fuel : impl->engine_without_fuel;
    const char * phase_label = fuel_mode == FuelMode::Enabled ? "fuel" : "no-fuel";
    auto module = compileModuleWithEngine(engine, wasm_code, phase_label);

    return std::make_unique<WasmTimeModule>(module_name, engine, std::move(module), fuel_mode);
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

std::unique_ptr<WasmModule> WasmTimeRuntime::compileModule(
    std::string_view /* module_name */,
    std::string_view /* wasm_code */,
    FuelMode /*fuel_mode*/) const
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Wasmtime support is disabled");
}

void WasmTimeRuntime::setLogLevel(LogsLevel /* level */)
{
}

WasmTimeRuntime::~WasmTimeRuntime() = default;

}

#endif
