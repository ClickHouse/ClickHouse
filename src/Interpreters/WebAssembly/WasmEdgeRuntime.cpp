#include <Interpreters/WebAssembly/WasmEdgeRuntime.h>

#include <Interpreters/WebAssembly/HostApi.h>
#include <Interpreters/WebAssembly/WasmMemory.h>

#include <Common/logger_useful.h>

#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/LoggingFormatStringHelpers.h>
#include <Common/ProfileEvents.h>

#include <absl/container/inlined_vector.h>

#include <wasmedge/wasmedge.h>

#include <filesystem>
#include <list>
#include <ranges>
#include <fmt/ranges.h>

namespace ProfileEvents
{
extern const Event WasmExecuteMicroseconds;
}

namespace DB::ErrorCodes
{
extern const int TOO_LARGE_STRING_SIZE;
extern const int LOGICAL_ERROR;
extern const int WASM_ERROR;
}

namespace DB::WebAssembly
{

/** Maps WasmEdge API types to our wrapper types.
  * This mapping is centralized here to keep the rest of the codebase decoupled from the WasmEdge API specifics.
  * To support a new type, simply add a new specialization of `WasmEdgeValueTypeTrait` for the corresponding `WasmValKind`.
  * This approach also allows statically check that all our types are covered by engine implementation.
  */
template <WasmValKind val_kind>
struct WasmEdgeValueTypeTrait;

#define WASM_EDGE_TYPE_TRAIT_SPECIALIZATION(T) \
    template <> \
    struct WasmEdgeValueTypeTrait<WasmValKind::T> \
    { \
        static WasmEdge_ValType type() \
        { \
            return WasmEdge_ValTypeGen##T(); \
        } \
        static bool is(WasmEdge_ValType val) \
        { \
            return WasmEdge_ValTypeIs##T(val); \
        } \
        static WasmEdge_Value to(auto val) \
        { \
            return WasmEdge_ValueGen##T(val); \
        } \
        static auto from(WasmEdge_Value val) \
        { \
            return WasmEdge_ValueGet##T(val); \
        } \
    };

WASM_EDGE_TYPE_TRAIT_SPECIALIZATION(I32);
WASM_EDGE_TYPE_TRAIT_SPECIALIZATION(I64);
WASM_EDGE_TYPE_TRAIT_SPECIALIZATION(F32);
WASM_EDGE_TYPE_TRAIT_SPECIALIZATION(F64);

#undef WASM_EDGE_TYPE_TRAIT_SPECIALIZATION

/// Functions below is basically compile time switch-case for WasmValKind,
/// so we don't need to maintain it in sync with enum options manually in in each function.
template <std::underlying_type_t<WasmValKind> index = 0>
inline WasmValKind fromWasmEdgeValueType(WasmEdge_ValType val_type)
{
    if constexpr (index < std::variant_size_v<WasmVal>)
    {
        constexpr auto kind = static_cast<WasmValKind>(index);
        if (WasmEdgeValueTypeTrait<kind>::is(val_type))
            return kind;
        return fromWasmEdgeValueType<index + 1>(val_type);
    }
    throw Exception(ErrorCodes::WASM_ERROR, "Unsupported wasm edge type");
}

template <std::underlying_type_t<WasmValKind> index = 0>
WasmVal fromWasmEdgeValue(WasmEdge_Value val)
{
    if constexpr (index < std::variant_size_v<WasmVal>)
    {
        constexpr auto kind = static_cast<WasmValKind>(index);
        if (WasmEdgeValueTypeTrait<kind>::is(val.Type))
        {
            using WasmType = std::variant_alternative_t<index, WasmVal>;
            return std::bit_cast<WasmType>(WasmEdgeValueTypeTrait<kind>::from(val));
        }
        return fromWasmEdgeValue<index + 1>(val);
    }
    throw Exception(ErrorCodes::WASM_ERROR, "Unsupported wasm edge type");
}

template <std::underlying_type_t<WasmValKind> index = 0>
inline WasmEdge_ValType toWasmEdgeValueType(WasmValKind k)
{
    if constexpr (index < std::variant_size_v<WasmVal>)
    {
        constexpr auto kind = static_cast<WasmValKind>(index);
        if (kind == k)
            return WasmEdgeValueTypeTrait<kind>::type();
        return toWasmEdgeValueType<index + 1>(k);
    }
    throw Exception(ErrorCodes::WASM_ERROR, "Unsupported wasm edge type");
}

WasmEdge_Value toWasmEdgeValue(WasmVal val)
{
    return std::visit(
        [](auto arg)
        {
            using T = std::decay_t<decltype(arg)>;
            return WasmEdgeValueTypeTrait<WasmValTypeToKind<T>::value>::to(arg);
        },
        val);
}

/// Mapping of WasmEdge API types to their deleter functions
template <typename T>
struct WasmEdgeDeleterTrait;

#define WASM_EDGE_DELETER_TRAIT_SPECIALIZATION(T) \
    template <> \
    struct WasmEdgeDeleterTrait<WasmEdge_##T##Context> \
    { \
        static constexpr auto name = #T; \
        void operator()(auto * ptr) const \
        { \
            WasmEdge_##T##Delete(ptr); \
        } \
    };

WASM_EDGE_DELETER_TRAIT_SPECIALIZATION(VM);
WASM_EDGE_DELETER_TRAIT_SPECIALIZATION(ModuleInstance);
WASM_EDGE_DELETER_TRAIT_SPECIALIZATION(FunctionType);
WASM_EDGE_DELETER_TRAIT_SPECIALIZATION(FunctionInstance);
WASM_EDGE_DELETER_TRAIT_SPECIALIZATION(Configure);
WASM_EDGE_DELETER_TRAIT_SPECIALIZATION(Compiler);
WASM_EDGE_DELETER_TRAIT_SPECIALIZATION(Loader);
WASM_EDGE_DELETER_TRAIT_SPECIALIZATION(ASTModule);
WASM_EDGE_DELETER_TRAIT_SPECIALIZATION(MemoryInstance);

#undef WASM_EDGE_DELETER_TRAIT_SPECIALIZATION


/// See kPageSize contrib/wasmedge/include/runtime/instance/memory.h
constexpr uint32_t WASMEDGE_PAGE_SIZE = 65536;

/// Helpers to manage WasmEdge API resources
template <typename T>
using WasmEdgeResourcePtr = std::unique_ptr<T, WasmEdgeDeleterTrait<T>>;

/// Creates a WasmEdge resource with specified constructor and checks that result is not-NULL
template <auto create_func, typename... Args>
auto WasmEdgeResourcePtrCreate(Args &&... args)
{
    auto * resource = create_func(std::forward<Args>(args)...);
    using ResourceT = std::remove_pointer_t<decltype(resource)>;
    if (!resource)
        throw Exception(ErrorCodes::WASM_ERROR, "Cannot create {}", WasmEdgeDeleterTrait<ResourceT>::name);
    return WasmEdgeResourcePtr<ResourceT>(resource);
}

WasmEdge_Bytes wasmedgeBytesWrap(std::string_view data)
{
    if (data.size() > std::numeric_limits<WasmSizeT>::max())
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Data is too large for wasm, size: {}", data.size());

    return WasmEdge_BytesWrap(reinterpret_cast<const uint8_t *>(data.data()), static_cast<uint32_t>(data.size()));
}

WasmEdge_String wasmedgeStringWrap(std::string_view data)
{
    if (data.size() > std::numeric_limits<WasmSizeT>::max())
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Data is too large for wasm, size: {}", data.size());

    return WasmEdge_StringWrap(reinterpret_cast<const char *>(data.data()), static_cast<uint32_t>(data.size()));
}


void wasmedgeCheckResult(WasmEdge_Result result, const std::string_view & msg)
{
    if (!WasmEdge_ResultOK(result))
        throw Exception(ErrorCodes::WASM_ERROR, "Wasm error: {} ({})", msg, WasmEdge_ResultGetMessage(result));
}

struct WasmEdgeFunctionProps
{
    WasmEdgeFunctionProps(std::string_view function_name_, const WasmEdge_FunctionTypeContext * ctx)
        : func_ctx(ctx)
        , function_name(function_name_)
        , params_count(WasmEdge_FunctionTypeGetParametersLength(ctx))
        , returns_count(WasmEdge_FunctionTypeGetReturnsLength(ctx))
    {
    }

    WasmEdgeFunctionProps(WasmEdge_String function_name_, const WasmEdge_FunctionTypeContext * ctx)
        : WasmEdgeFunctionProps(std::string_view(function_name_.Buf, function_name_.Length), ctx)
    {
    }

    WasmFunctionDeclarationPtr getFunctionDeclaration() const
    {
        if (returns_count > 1)
            throw Exception(ErrorCodes::WASM_ERROR, "Function '{}' has more than one return value", function_name);

        std::vector<WasmEdge_ValType> argument_types_list(params_count);
        WasmEdge_FunctionTypeGetParameters(func_ctx, argument_types_list.data(), static_cast<uint32_t>(params_count));

        std::vector<WasmValKind> argument_types(params_count);
        std::transform(argument_types_list.begin(), argument_types_list.end(), argument_types.begin(), fromWasmEdgeValueType<>);

        std::optional<WasmValKind> return_type;
        if (returns_count == 1)
        {
            WasmEdge_ValType return_type_val;
            WasmEdge_FunctionTypeGetReturns(func_ctx, &return_type_val, 1);
            return_type = fromWasmEdgeValueType(return_type_val);
        }
        return std::make_unique<WasmFunctionDeclaration>(function_name, std::move(argument_types), return_type);
    }

    const WasmEdge_FunctionTypeContext * func_ctx;

    std::string function_name;
    size_t params_count;
    size_t returns_count;
};


auto getWasmEdgeVmConfig(WasmModule::Config cfg)
{
    auto config = WasmEdgeResourcePtrCreate<WasmEdge_ConfigureCreate>();
    if (!config)
        throw Exception(ErrorCodes::WASM_ERROR, "Cannot create WasmEdge config");

    WasmEdge_ConfigureAddHostRegistration(config.get(), WasmEdge_HostRegistration_Wasi);

    if (cfg.memory_limit)
    {
        WasmEdge_ConfigureSetMaxMemoryPage(config.get(), static_cast<uint32_t>(cfg.memory_limit / WASMEDGE_PAGE_SIZE));
    }

    if (cfg.fuel_limit)
    {
        WasmEdge_ConfigureStatisticsSetCostMeasuring(config.get(), true);
        WasmEdge_ConfigureStatisticsSetTimeMeasuring(config.get(), true);
    }

    return config;
}

class WasmEdgeCompartment;

/// Transforms a host function into a WasmEdge_HostFunc_t
/// Host function accepts (WasmCompartment *) as the first argument and the rest of the arguments passed from the WASM code
class HostFunctionAdapter : private boost::noncopyable
{
public:
    HostFunctionAdapter(WasmEdgeCompartment * compartment_, const WasmHostFunction * func_)
        : compartment(compartment_)
        , host_function_ptr(func_)
    {
    }

    static WasmEdge_Result callFunction(
        void * payload [[maybe_unused]],
        const WasmEdge_CallingFrameContext * call_frame_ctx [[maybe_unused]],
        const WasmEdge_Value * in [[maybe_unused]],
        WasmEdge_Value * out [[maybe_unused]]);

    void linkTo(WasmEdge_ModuleInstanceContext * module_instance_ctx)
    {
        auto argument_types = host_function_ptr->getArgumentTypes();
        std::vector<WasmEdge_ValType> params(argument_types.size());
        std::transform(argument_types.begin(), argument_types.end(), params.begin(), toWasmEdgeValueType<>);

        std::vector<WasmEdge_ValType> returns;
        if (auto return_type = host_function_ptr->getReturnType())
            returns.push_back(toWasmEdgeValueType(*return_type));

        auto func_type = WasmEdgeResourcePtrCreate<WasmEdge_FunctionTypeCreate>(
            params.data(), static_cast<uint32_t>(params.size()), returns.data(), static_cast<uint32_t>(returns.size()));
        /// Ownership of function_instance is transferred to module_instance_ctx
        auto * function_instance
            = WasmEdge_FunctionInstanceCreate(func_type.get(), callFunction, reinterpret_cast<void *>(this), /* cost= */ 1);

        auto function_name = host_function_ptr->getName();
        WasmEdge_ModuleInstanceAddFunction(
            module_instance_ctx,
            WasmEdge_StringWrap(reinterpret_cast<const char *>(function_name.data()), static_cast<uint32_t>(function_name.size())),
            function_instance);
    }

    WasmEdgeCompartment * compartment = nullptr;
    const WasmHostFunction * host_function_ptr = nullptr;
};


class WasmEdgeCompartment : public WasmCompartment
{
public:
    explicit WasmEdgeCompartment(WasmModule::Config cfg)
        : import_module_ctx(WasmEdgeResourcePtrCreate<WasmEdge_ModuleInstanceCreate>(wasmedgeStringWrap("env")))
        , vm_cxt(WasmEdgeResourcePtrCreate<WasmEdge_VMCreate>(getWasmEdgeVmConfig(cfg).get(), nullptr))
    {
        auto * stat_ctx = WasmEdge_VMGetStatisticsContext(vm_cxt.get());
        if (cfg.fuel_limit)
        {
            WasmEdge_StatisticsSetCostLimit(stat_ctx, cfg.fuel_limit);
        }
    }

    void addHostFunction(const WasmHostFunction * host_function_ptr)
    {
        auto & host_func = host_functions.emplace_back(this, host_function_ptr);
        host_func.linkTo(import_module_ctx.get());
    }

    uint8_t * getMemory(WasmPtr ptr, WasmSizeT size) override;
    uint32_t growMemory(uint32_t num_pages) override;
    WasmSizeT getMemorySize() override;

    void invoke(std::string_view function_name, const std::vector<WasmVal> & params, std::vector<WasmVal> & returns) override;

    void loadModuleFromCode(std::string_view wasm_code);
    void loadModuleFromFile(const std::filesystem::path & file_path);
    void loadModuleFromAst(const WasmEdge_ASTModuleContext * ast_module);

    WasmEdge_ModuleInstanceContext * getHostFunctionContext() { return import_module_ctx.get(); }

    void setLastException(Exception e) { last_exception = std::move(e); }

private:
    void loadModuleImpl();

    /// Host functions are registered in this context
    WasmEdgeResourcePtr<WasmEdge_ModuleInstanceContext> import_module_ctx;
    WasmEdgeResourcePtr<WasmEdge_VMContext> vm_cxt;

    /// Owned by vm_cxt
    const WasmEdge_ModuleInstanceContext * vm_instance_cxt = nullptr;

    std::list<HostFunctionAdapter> host_functions;

    /// Note: use std::map and std::less<> to perform lookup with std::string_view
    std::map<std::string, WasmEdgeFunctionProps, std::less<>> imported_functions;

    std::optional<Exception> last_exception;

    LoggerPtr log = getLogger("WasmEdgeCompartment");
};

WasmEdge_Result HostFunctionAdapter::callFunction(
    void * payload [[maybe_unused]],
    const WasmEdge_CallingFrameContext * call_frame_ctx [[maybe_unused]],
    const WasmEdge_Value * in [[maybe_unused]],
    WasmEdge_Value * out [[maybe_unused]])
{
    auto * adapter = reinterpret_cast<HostFunctionAdapter *>(payload);
    auto * compartment = adapter->compartment;
    const auto & host_func = *adapter->host_function_ptr;

    try
    {
        auto argument_types = host_func.getArgumentTypes();
        std::vector<WasmVal> args(argument_types.size());
        for (size_t i = 0; i < argument_types.size(); ++i)
        {
            args[i] = fromWasmEdgeValue(in[i]);
            if (getWasmValKind(args[i]) != argument_types[i])
                throw Exception(
                    ErrorCodes::WASM_ERROR,
                    "Function {} invoked with wrong argument types [{}]",
                    formatFunctionDeclaration(host_func),
                    fmt::join(args | std::views::transform(getWasmValKind), ", "));
        }

        auto result_val = host_func(compartment, args);
        if (result_val)
            out[0] = toWasmEdgeValue(*result_val);
    }
    catch (Exception & e)
    {
        /// The runtime cannot handle exceptions (e.g., due to noexcept).
        /// We catch them here and store them to rethrow after the wasm code returns.
        compartment->setLastException(std::move(e));
        return WasmEdge_Result_Fail;
    }

    return WasmEdge_Result_Success;
}

void WasmEdgeCompartment::loadModuleFromFile(const std::filesystem::path & file_path)
{
    wasmedgeCheckResult(WasmEdge_VMLoadWasmFromFile(vm_cxt.get(), file_path.c_str()), "cannot load module");
    loadModuleImpl();
}

void WasmEdgeCompartment::loadModuleFromCode(std::string_view wasm_code)
{
    wasmedgeCheckResult(WasmEdge_VMLoadWasmFromBytes(vm_cxt.get(), wasmedgeBytesWrap(wasm_code)), "cannot load module");
    loadModuleImpl();
}

void WasmEdgeCompartment::loadModuleFromAst(const WasmEdge_ASTModuleContext * ast_module)
{
    wasmedgeCheckResult(WasmEdge_VMLoadWasmFromASTModule(vm_cxt.get(), ast_module), "cannot load module");
    loadModuleImpl();
}

void WasmEdgeCompartment::loadModuleImpl()
{
    wasmedgeCheckResult(WasmEdge_VMValidate(vm_cxt.get()), "cannot validate module");
    wasmedgeCheckResult(WasmEdge_VMRegisterModuleFromImport(vm_cxt.get(), import_module_ctx.get()), "cannot register host module");
    wasmedgeCheckResult(WasmEdge_VMInstantiate(vm_cxt.get()), "cannot instantiate module");
    vm_instance_cxt = WasmEdge_VMGetActiveModule(vm_cxt.get());
    if (!vm_instance_cxt)
        throw Exception(ErrorCodes::WASM_ERROR, "Cannot get active module");

    auto number_of_functions = WasmEdge_VMGetFunctionListLength(vm_cxt.get());
    std::vector<WasmEdge_String> function_names(number_of_functions);
    std::vector<const WasmEdge_FunctionTypeContext *> function_types(number_of_functions);

    WasmEdge_VMGetFunctionList(vm_cxt.get(), function_names.data(), function_types.data(), number_of_functions);

    for (size_t i = 0; i < number_of_functions; ++i)
    {
        std::string_view func_name(function_names[i].Buf, function_names[i].Length);
        auto [_, inserted] = imported_functions.emplace(func_name, WasmEdgeFunctionProps(function_names[i], function_types[i]));
        if (!inserted)
            throw Exception(ErrorCodes::WASM_ERROR, "Module has multiple '{}' functions", func_name);
    }
}

uint32_t WasmEdgeCompartment::growMemory(uint32_t num_pages)
{
    auto * memory_ctx = WasmEdge_ModuleInstanceFindMemory(vm_instance_cxt, wasmedgeStringWrap("memory"));
    uint32_t memory_end = WasmEdge_MemoryInstanceGetPageSize(memory_ctx) * WASMEDGE_PAGE_SIZE;
    wasmedgeCheckResult(WasmEdge_MemoryInstanceGrowPage(memory_ctx, num_pages), "cannot grow memory");
    return memory_end;
}

uint32_t WasmEdgeCompartment::getMemorySize()
{
    const auto * memory_ctx = WasmEdge_ModuleInstanceFindMemory(vm_instance_cxt, wasmedgeStringWrap("memory"));
    return WasmEdge_MemoryInstanceGetPageSize(memory_ctx) * WASMEDGE_PAGE_SIZE;
}

uint8_t * WasmEdgeCompartment::getMemory(WasmPtr ptr, WasmSizeT size)
{
    auto * memory_ctx = WasmEdge_ModuleInstanceFindMemory(vm_instance_cxt, wasmedgeStringWrap("memory"));
    if (memory_ctx == nullptr)
        throw Exception(ErrorCodes::WASM_ERROR, "Cannot find memory export in wasm module");
    auto * data = WasmEdge_MemoryInstanceGetPointer(memory_ctx, ptr, size);
    if (data == nullptr)
    {
        uint32_t total_memory = WasmEdge_MemoryInstanceGetPageSize(memory_ctx) * WASMEDGE_PAGE_SIZE;
        throw Exception(
            ErrorCodes::WASM_ERROR, "Cannot get memory at offset {} and size {} from wasm module with size {}", ptr, size, total_memory);
    }
    return data;
}

void WasmEdgeCompartment::invoke(std::string_view function_name, const std::vector<WasmVal> & params, std::vector<WasmVal> & returns)
{
    auto func_it = imported_functions.find(function_name);
    if (func_it == imported_functions.end())
        throw Exception(ErrorCodes::WASM_ERROR, "Function '{}' is not found in module", function_name);
    size_t params_count = func_it->second.params_count;
    size_t returns_count = func_it->second.returns_count;

    if (params_count != params.size())
        throw Exception(
            ErrorCodes::WASM_ERROR,
            "Function {} invoked with wrong number of arguments {}, "
            "expected {} for function with type '{}'",
            function_name,
            params.size(),
            params_count,
            formatFunctionDeclaration(*func_it->second.getFunctionDeclaration()));

    std::vector<WasmEdge_Value> params_values(params.size());
    for (size_t i = 0; i < params.size(); ++i)
        params_values[i] = toWasmEdgeValue(params[i]);

    std::vector<WasmEdge_Value> returns_values(returns_count);
    {
        last_exception.reset();

        ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::WasmExecuteMicroseconds);
        WasmEdge_Result result = WasmEdge_VMExecute(
            vm_cxt.get(),
            wasmedgeStringWrap(function_name),
            params_values.data(),
            static_cast<uint32_t>(params_values.size()),
            returns_values.data(),
            static_cast<uint32_t>(returns_values.size()));

        if (last_exception)
            last_exception->rethrow();
        wasmedgeCheckResult(result, fmt::format("cannot execute function {}", function_name));
    }

    returns.clear();
    std::transform(returns_values.begin(), returns_values.end(), std::back_inserter(returns), fromWasmEdgeValue<>);
    LOG_TRACE(log, "Function {} invocation ended", function_name);
}


class WasmEdgeModule : public WasmModule
{
public:
    explicit WasmEdgeModule(WasmEdge_ASTModuleContext * ast_module_ptr)
        : ast_module(ast_module_ptr)
    {
        auto exports_length = WasmEdge_ASTModuleListExportsLength(ast_module.get());
        if (exports_length >= 512)
            throw Exception(ErrorCodes::WASM_ERROR, "Module has too many exports");

        std::vector<const WasmEdge_ExportTypeContext *> exports_list(exports_length);
        WasmEdge_ASTModuleListExports(ast_module.get(), exports_list.data(), exports_length);
        for (const auto * export_ctx : exports_list)
        {
            auto export_name = WasmEdge_ExportTypeGetExternalName(export_ctx);
            if (export_name.Length == 0)
                throw Exception(ErrorCodes::WASM_ERROR, "Cannot get export name");
            auto external_type = WasmEdge_ExportTypeGetExternalType(export_ctx);
            if (external_type == WasmEdge_ExternalType_Function)
                exports.emplace(std::string(export_name.Buf, export_name.Length), export_ctx);
        }
    }

    std::unique_ptr<WasmCompartment> instantiate(Config cfg) const override
    {
        auto compartment = std::make_unique<WasmEdgeCompartment>(cfg);
        for (const auto & host_function_ptr : host_functions)
            compartment->addHostFunction(host_function_ptr.get());
        compartment->loadModuleFromAst(ast_module.get());
        return compartment;
    }

    std::vector<WasmFunctionDeclarationPtr> getImports() const override
    {
        auto imports_length = WasmEdge_ASTModuleListImportsLength(ast_module.get());
        std::vector<const WasmEdge_ImportTypeContext *> imports(imports_length);
        WasmEdge_ASTModuleListImports(ast_module.get(), imports.data(), imports_length);

        std::vector<WasmFunctionDeclarationPtr> result;

        for (const auto * import_ctx : imports)
        {
            auto import_name = WasmEdge_ImportTypeGetExternalName(import_ctx);
            if (import_name.Length == 0)
                throw Exception(ErrorCodes::WASM_ERROR, "Cannot get import name");

            const auto * function_type = WasmEdge_ImportTypeGetFunctionType(ast_module.get(), import_ctx);
            if (!function_type)
                throw Exception(
                    ErrorCodes::WASM_ERROR, "Cannot get function for import '{}'", std::string_view(import_name.Buf, import_name.Length));
            result.push_back(WasmEdgeFunctionProps(import_name, function_type).getFunctionDeclaration());
        }

        return result;
    }

    void addImport(std::unique_ptr<WasmHostFunction> import_host_function) override
    {
        host_functions.push_back(std::move(import_host_function));
    }

    WasmFunctionDeclarationPtr getExport(std::string_view function_name) const override
    {
        auto export_it = exports.find(function_name);
        if (export_it == exports.end() || export_it->second == nullptr)
            return nullptr;
        const auto * function_type = WasmEdge_ExportTypeGetFunctionType(ast_module.get(), export_it->second);
        if (!function_type)
            throw Exception(ErrorCodes::WASM_ERROR, "Cannot get function for export '{}'", function_name);
        return WasmEdgeFunctionProps(function_name, function_type).getFunctionDeclaration();
    }

private:
    WasmEdgeResourcePtr<WasmEdge_ASTModuleContext> ast_module;
    std::map<std::string, const WasmEdge_ExportTypeContext *, std::less<>> exports;
    std::vector<std::unique_ptr<WasmHostFunction>> host_functions;
};


WasmEdgeRuntime::WasmEdgeRuntime()
{
    setLogLevel(LogsLevel::warning);
}

std::unique_ptr<WasmModule> WasmEdgeRuntime::createModule(std::string_view wasm_code) const
{
    auto loader_ctx = WasmEdgeResourcePtrCreate<WasmEdge_LoaderCreate>(nullptr);
    WasmEdge_ASTModuleContext * ast_module_ptr = nullptr;
    auto res = WasmEdge_LoaderParseFromBytes(loader_ctx.get(), &ast_module_ptr, wasmedgeBytesWrap(wasm_code));
    wasmedgeCheckResult(res, "cannot parse module");
    if (!ast_module_ptr)
        throw Exception(ErrorCodes::WASM_ERROR, "Cannot parse module");
    return std::make_unique<WasmEdgeModule>(ast_module_ptr);
}


void wasmEdgeLogCallback(const WasmEdge_LogMessage * msg)
{
    std::string logger_name(msg->LoggerName.Buf, msg->LoggerName.Length);
    auto log = getLogger(logger_name);

    PreformattedMessage message{std::string(msg->Message.Buf, msg->Message.Length), "", {}};
    switch (msg->Level)
    {
        case WasmEdge_LogLevel_Critical:
            [[fallthrough]];
        case WasmEdge_LogLevel_Error:
            LOG_ERROR(log, message);
            break;
        case WasmEdge_LogLevel_Warn:
            LOG_WARNING(log, message);
            break;
        case WasmEdge_LogLevel_Info:
            LOG_INFO(log, message);
            break;
        case WasmEdge_LogLevel_Debug:
            LOG_DEBUG(log, message);
            break;
        case WasmEdge_LogLevel_Trace:
            LOG_TRACE(log, message);
            break;
    }
}

void WasmEdgeRuntime::setLogLevel(LogsLevel level)
{
    WasmEdge_LogSetCallback(wasmEdgeLogCallback);

    switch (level)
    {
        case LogsLevel::test:
            [[fallthrough]];
        case LogsLevel::trace:
            [[fallthrough]];
        case LogsLevel::debug:
            WasmEdge_LogSetDebugLevel();
            break;
        case LogsLevel::information:
            [[fallthrough]];
        case LogsLevel::warning:
            [[fallthrough]];
        case LogsLevel::error:
            WasmEdge_LogSetErrorLevel();
            break;
        case LogsLevel::fatal:
            [[fallthrough]];
        case LogsLevel::none:
            WasmEdge_LogOff();
            return;
    }
}


}
