#include <Functions/UserDefined/UserDefinedWebAssembly.h>

#include <ranges>

#include <Columns/ColumnVector.h>

#include <DataTypes/DataTypesNumber.h>

#include <Functions/IFunctionAdaptors.h>

#include <Formats/FormatFactory.h>
#include <Formats/formatBlock.h>

#include <Interpreters/Context.h>
#include <Interpreters/WasmModuleManager.h>
#include <Interpreters/WebAssembly/HostApi.h>
#include <Interpreters/WebAssembly/WasmMemory.h>

#include <Parsers/ASTCreateWasmFunctionQuery.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>

#include <Processors/Chunk.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Common/formatReadable.h>

#include <Common/NamePrompter.h>
#include <Common/PoolBase.h>
#include <fmt/ranges.h>
#include <Poco/String.h>
#include <Common/transformEndianness.h>


#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>


namespace ProfileEvents
{
extern const Event WasmMemoryAllocated;
}

namespace DB
{


namespace
{

/// Check if the string doesn't mix upper and lower case.
/// String may start with upper case letter, but all other letters should have the same case.
/// Examples:
/// Allowed: Foobar, FOOBAR, foobar
/// Not allowed: fOOBAR, FooBAR, FOObar
bool isCaseConstrained(std::string_view str)
{
    if (str.size() < 2)
        return true;
    bool is_lower = std::islower(str[0]) || std::islower(str[1]);
    for (size_t i = 1; i < str.size(); ++i)
    {
        if (is_lower != std::islower(str[i]))
            return false;
    }
    return true;
}

}

using namespace WebAssembly;

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int RESOURCE_NOT_FOUND;
extern const int TOO_LARGE_STRING_SIZE;
extern const int WASM_ERROR;
}

UserDefinedWebAssemblyFunction::UserDefinedWebAssemblyFunction(
    std::shared_ptr<WebAssembly::WasmModule> wasm_module_,
    const String & function_name_,
    const Strings & argument_names_,
    const DataTypes & arguments_,
    const DataTypePtr & result_type_,
    WebAssemblyFunctionSettings function_settings_)
    : function_name(function_name_)
    , argument_names(argument_names_)
    , arguments(arguments_)
    , result_type(result_type_)
    , wasm_module(wasm_module_)
    , settings(std::move(function_settings_))
{
}

class UserDefinedWebAssemblyFunctionSimple : public UserDefinedWebAssemblyFunction
{
public:
    template <typename... Args>
    explicit UserDefinedWebAssemblyFunctionSimple(Args &&... args) : UserDefinedWebAssemblyFunction(std::forward<Args>(args)...)
    {
        checkSignature();
    }

    void checkSignature() const
    {
        auto function_declaration = wasm_module->getExport(function_name);
        if (!function_declaration)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "WebAssembly function '{}' doesn't exist", function_name);

        const auto & wasm_argument_types = function_declaration->getArgumentTypes();
        if (wasm_argument_types.size() != arguments.size())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "WebAssembly function '{}' expects {} arguments, but it's declared with {} arguments",
                function_name,
                wasm_argument_types.size(),
                arguments.size());
        }

        for (size_t i = 0; i < arguments.size(); ++i)
            checkDataTypeWithWasmValKind(arguments[i].get(), wasm_argument_types[i]);

        auto wasm_return_type = function_declaration->getReturnType();
        if (bool(result_type) != wasm_return_type.has_value())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "WebAssembly function '{}' expects return type {}, but it's declared with {} return type",
                function_name,
                result_type ? result_type->getName() : "void",
                wasm_return_type ? toString(wasm_return_type.value()) : "void");
        }

        if (wasm_return_type)
            checkDataTypeWithWasmValKind(result_type.get(), wasm_return_type.value());
    }


    template <typename Callable, typename... Args>
    static bool tryExecuteForColumnTypes(Callable && callable, Args &&... args)
    {
        return (
            callable.template operator()<Int32>(std::forward<Args>(args)...)
            || callable.template operator()<UInt32>(std::forward<Args>(args)...)
            || callable.template operator()<Int64>(std::forward<Args>(args)...)
            || callable.template operator()<UInt64>(std::forward<Args>(args)...)
            || callable.template operator()<Float32>(std::forward<Args>(args)...)
            || callable.template operator()<Float64>(std::forward<Args>(args)...));
    }

    static void checkDataTypeWithWasmValKind(const IDataType * type, WasmValKind kind)
    {
        bool is_data_type_compatible = tryExecuteForColumnTypes(
            [type, kind]<typename T>() { return typeid_cast<const DataTypeNumber<T> *>(type) && WasmValTypeToKind<T>::value == kind; });
        if (!is_data_type_compatible)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "WebAssembly function expects type compatible with {}, but got {}",
                toString(kind),
                type->getName());
    }

    MutableColumnPtr
    executeOnBlock(WebAssembly::WasmCompartment * compartment, const Block & block, ContextPtr, size_t num_rows) const override
    {
        auto get_column_element = []<typename T>(const IColumn * column, size_t row_idx, WasmVal & val)
        {
            if (auto * column_typed = checkAndGetColumn<ColumnVector<T>>(column))
            {
                val = std::bit_cast<typename NativeToWasmType<T>::Type>(column_typed->getElement(row_idx));
                return true;
            }
            return false;
        };

        MutableColumnPtr result_column = result_type->createColumn();
        auto invoke_and_set_column = [&]<typename T>(const std::vector<WasmVal> & args)
        {
            if (auto * column_typed = typeid_cast<ColumnVector<T> *>(result_column.get()))
            {
                auto value = compartment->invoke<typename NativeToWasmType<T>::Type>(function_name, args);
                column_typed->insertValue(std::bit_cast<T>(value));
                return true;
            }
            return false;
        };

        size_t num_columns = block.columns();
        std::vector<WasmVal> wasm_args(num_columns);
        for (size_t row_idx = 0; row_idx < num_rows; ++row_idx)
        {
            for (size_t col_idx = 0; col_idx < num_columns; ++col_idx)
            {
                const auto & column = block.getByPosition(col_idx);
                if (!tryExecuteForColumnTypes(get_column_element, column.column.get(), row_idx, wasm_args[col_idx]))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot convert {} to WebAssembly type", column.type->getName());
            }

            if (!tryExecuteForColumnTypes(invoke_and_set_column, wasm_args))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot get value of type {} from result of WebAssembly function {}",
                    result_column->getName(),
                    function_name);
        }

        return result_column;
    }
};


struct WasmBuffer
{
    WasmPtr ptr;
    WasmSizeT size;
};

static_assert(sizeof(WasmBuffer) == 8, "WasmBuffer size must be 8 bytes");
static_assert(alignof(WasmBuffer) == 4, "WasmBuffer alignment must be 4 bytes");

class WasmMemoryManagerV1 : public WasmMemoryManager
{
public:
    constexpr static std::string_view allocate_function_name = "clickhouse_create_buffer";
    constexpr static std::string_view deallocate_function_name = "clickhouse_destroy_buffer";

    static WasmFunctionDeclaration allocateFunctionDeclaration() { return {allocate_function_name, {WasmValKind::I32}, WasmValKind::I32}; }
    static WasmFunctionDeclaration deallocateFunctionDeclaration() { return {deallocate_function_name, {WasmValKind::I32}, std::nullopt}; }

    explicit WasmMemoryManagerV1(WasmCompartment * compartment_) : compartment(compartment_) { }

    WasmPtr createBuffer(WasmSizeT size) const override { return compartment->invoke<WasmPtr>(allocate_function_name, {size}); }

    void destroyBuffer(WasmPtr ptr) const override { compartment->invoke<void>(deallocate_function_name, {ptr}); }

    std::span<uint8_t> getMemoryView(WasmPtr ptr, WasmSizeT size) const override { return {compartment->getMemory(ptr, size), size}; }

private:
    WasmCompartment * compartment;
};

class UserDefinedWebAssemblyFunctionV1 : public UserDefinedWebAssemblyFunction
{
public:
    template <typename... Args>
    explicit UserDefinedWebAssemblyFunctionV1(Args &&... args) : UserDefinedWebAssemblyFunction(std::forward<Args>(args)...)
    {
        checkSignature();
    }

    void checkFunction(const WasmFunctionDeclaration & expected) const
    {
        auto function_declaration = wasm_module->getExport(expected.getName());
        if (!function_declaration)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "WebAssembly function '{}' not found", expected.getName());

        checkFunctionDeclarationMatches(*function_declaration, expected);
    }

    void checkSignature() const
    {
        checkFunction(WasmFunctionDeclaration(function_name, {WasmValKind::I32, WasmValKind::I32}, WasmValKind::I32));
        checkFunction(WasmMemoryManagerV1::allocateFunctionDeclaration());
        checkFunction(WasmMemoryManagerV1::deallocateFunctionDeclaration());
    }

    static void readSingleBlock(std::unique_ptr<PullingPipelineExecutor> pipeline_executor, Block & result_block)
    {
        Chunk result_chunk;
        while (true)
        {
            Chunk chunk;
            bool has_data = pipeline_executor->pull(chunk);

            if (chunk && chunk.getNumColumns() != result_block.columns())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Different number of columns in result chunks, expected {}, got {}",
                    result_block.dumpStructure(),
                    chunk.dumpStructure());

            if (!result_chunk)
                result_chunk = std::move(chunk);
            else if (chunk)
                result_chunk.append(chunk);

            if (!has_data)
                break;
        }
        result_block.setColumns(result_chunk.detachColumns());
    }

    MutableColumnPtr
    executeOnBlock(WebAssembly::WasmCompartment * compartment, const Block & block, ContextPtr context, size_t num_rows) const override
    {
        String format_name = settings.getValue("serialization_format").safeGet<String>();

        if (num_rows == 0)
            return result_type->createColumn();
        if (num_rows >= std::numeric_limits<WasmSizeT>::max())
            throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large number of rows: {}", num_rows);

        auto wmm = std::make_unique<WasmMemoryManagerV1>(compartment);

        WasmTypedMemoryHolder<WasmBuffer> wasm_data = nullptr;
        if (block)
        {
            WriteBufferFromOwnString buf;
            auto out = context->getOutputFormat(format_name, buf, block.cloneEmpty());
            formatBlock(out, block);
            buf.finalize();

            auto input_data = buf.stringView();
            wasm_data = allocateInWasmMemory<WasmBuffer>(wmm.get(), input_data.size());
            wasm_data.ref()->size = static_cast<WasmSizeT>(input_data.size());
            if (!wasm_data || wasm_data.ref()->size != input_data.size())
                throw Exception(
                    ErrorCodes::WASM_ERROR,
                    "Cannot allocate buffer of size {}, got buffer of size {}. "
                    "Maybe '{}' function implementation in WebAssembly module is incorrect",
                    input_data.size(),
                    wasm_data.ref()->size,
                    WasmMemoryManagerV1::allocate_function_name);

            auto * wasm_mem = compartment->getMemory(wasm_data.ref()->ptr, wasm_data.ref()->size);
            std::copy(input_data.data(), input_data.data() + input_data.size(), wasm_mem);
        }

        auto result_ptr = compartment->invoke<WasmPtr>(function_name, {wasm_data.getPtr(), static_cast<WasmSizeT>(num_rows)});
        if (result_ptr == 0)
            throw Exception(ErrorCodes::WASM_ERROR, "WebAssembly function '{}' returned nullptr", function_name);

        WasmTypedMemoryHolder<WasmBuffer> result(wmm.get(), result_ptr);

        WasmSizeT result_size = result.ref()->size;
        auto * result_data = compartment->getMemory(result.ref()->ptr, result_size);
        ReadBufferFromMemory inbuf(result_data, result_size);

        Block result_header({ColumnWithTypeAndName(nullptr, result_type, "result")});
        auto pipeline = QueryPipeline(
            Pipe(context->getInputFormat(format_name, inbuf, result_header, /* max_block_size */ DBMS_DEFAULT_BUFFER_SIZE)));
        readSingleBlock(std::make_unique<PullingPipelineExecutor>(pipeline), result_header);

        if (result_header.columns() != 1 || result_header.rows() != num_rows)
            throw Exception(
                ErrorCodes::WASM_ERROR,
                "Unexpected result column structure: {} returned from WebAssembly function '{}'",
                result_header.dumpStructure(),
                function_name);

        auto result_columns = result_header.mutateColumns();
        return std::move(result_columns[0]);
    }
};


std::unique_ptr<UserDefinedWebAssemblyFunction> UserDefinedWebAssemblyFunction::create(
    std::shared_ptr<WebAssembly::WasmModule> wasm_module_,
    const String & function_name_,
    const Strings & argument_names_,
    const DataTypes & arguments_,
    const DataTypePtr & result_type_,
    WasmAbiVersion abi_type,
    WebAssemblyFunctionSettings function_settings)
{
    switch (abi_type)
    {
        case WasmAbiVersion::Plain:
            return std::make_unique<UserDefinedWebAssemblyFunctionSimple>(
                wasm_module_, function_name_, argument_names_, arguments_, result_type_, std::move(function_settings));
        case WasmAbiVersion::V1:
            return std::make_unique<UserDefinedWebAssemblyFunctionV1>(
                wasm_module_, function_name_, argument_names_, arguments_, result_type_, std::move(function_settings));
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR, "Unknown WebAssembly ABI version: {}", static_cast<std::underlying_type_t<WasmAbiVersion>>(abi_type));
}

String toString(WasmAbiVersion abi_type)
{
    switch (abi_type)
    {
        case WasmAbiVersion::Plain:
            return "PLAIN";
        case WasmAbiVersion::V1:
            return "V1";
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR, "Unknown WebAssembly ABI version: {}", static_cast<std::underlying_type_t<WasmAbiVersion>>(abi_type));
}

WasmAbiVersion getWasmAbiFromString(const String & str)
{
    if (!isCaseConstrained(str))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected value for WebAssembly ABI version: '{}'", str);

    for (auto abi_type : {
             WasmAbiVersion::Plain,
             WasmAbiVersion::V1,
         })
        if (Poco::toUpper(str) == toString(abi_type))
            return abi_type;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown WebAssembly ABI version: '{}'", str);
}

class WasmCompartmentPool final : private PoolBase<WebAssembly::WasmCompartment>
{
public:
    using Base = PoolBase<WasmCompartment>;
    using Object = Base::Object;
    using ObjectPtr = Base::ObjectPtr;

    explicit WasmCompartmentPool(
        unsigned limit, std::shared_ptr<WebAssembly::WasmModule> wasm_module_, WebAssembly::WasmModule::Config module_cfg_)
        : Base(limit, getLogger("WasmCompartmentPool")), wasm_module(std::move(wasm_module_)), module_cfg(std::move(module_cfg_))
    {
    }

    Entry aquire() { return get(-1); }

protected:
    ObjectPtr allocObject() override { return wasm_module->instantiate(module_cfg); }

private:
    std::shared_ptr<WebAssembly::WasmModule> wasm_module;
    WebAssembly::WasmModule::Config module_cfg;
};


WebAssembly::WasmModule::Config getWasmModuleConfigFromFunctionSettings(const WebAssemblyFunctionSettings & function_settings)
{
    WebAssembly::WasmModule::Config cfg;
    cfg.fuel_limit = function_settings.getValue("max_fuel").safeGet<UInt64>();
    cfg.memory_limit = function_settings.getValue("max_memory").safeGet<UInt64>();
    return cfg;
}

class FunctionUserDefinedWasm : public IFunction
{
public:
    FunctionUserDefinedWasm(String function_name_, std::shared_ptr<UserDefinedWebAssemblyFunction> udf_)
        : user_defined_function(std::move(udf_))
        , wasm_module(user_defined_function->getModule())
        , function_name(std::move(function_name_))
        , argument_names(user_defined_function->getArgumentNames())
        , compartment_pool(
              static_cast<UInt32>(user_defined_function->getSettings().getValue("max_instances").safeGet<UInt64>()),
              wasm_module,
              getWasmModuleConfigFromFunctionSettings(user_defined_function->getSettings()))
    {
    }

    String getName() const override { return function_name; }
    bool isVariadic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /* arguments */) const override { return false; }
    size_t getNumberOfArguments() const override { return user_defined_function->getArguments().size(); }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto & expected_arguments = user_defined_function->getArguments();
        if (arguments.size() != expected_arguments.size())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments doesn't match: passed {}, should be {}",
                arguments.size(),
                expected_arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (arguments[i]->equals(*expected_arguments[i]))
                continue;

            auto get_type_names = std::views::transform([](const auto & arg) { return arg->getName(); });
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type of arguments, expected ({}), got ({})",
                fmt::join(expected_arguments | get_type_names, ", "),
                fmt::join(arguments | get_type_names, ", "));
        }
        return user_defined_function->getResultType();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }

    bool isSuitableForConstantFolding() const override { return false; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto compartment_entry = compartment_pool.aquire();
        auto * compartment_ptr = &(*compartment_entry);
        return execute(compartment_ptr, arguments, input_rows_count);
    }

    ColumnPtr executeImplDryRun(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto cfg = getWasmModuleConfigFromFunctionSettings(user_defined_function->getSettings());
        auto compartment = wasm_module->instantiate(cfg);
        return execute(compartment.get(), arguments, input_rows_count);
    }

private:
    ColumnPtr execute(WebAssembly::WasmCompartment * compartment, const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        MutableColumnPtr result_column = user_defined_function->getResultType()->createColumn();
        size_t block_size = user_defined_function->getSettings().getValue("max_input_block_size").safeGet<UInt64>();
        if (block_size == 0)
            block_size = input_rows_count;

        auto context = Context::getGlobalContextInstance();
        for (size_t start_idx = 0; start_idx < input_rows_count; start_idx += block_size)
        {
            size_t current_block_size = std::min(block_size, input_rows_count - start_idx);
            auto current_input_block = getArgumentsBlock(arguments, start_idx, current_block_size);
            auto current_column = user_defined_function->executeOnBlock(compartment, current_input_block, context, current_block_size);

            if (!result_column->structureEquals(*current_column))
                throw Exception(
                    ErrorCodes::WASM_ERROR,
                    "Different column types in result blocks: {} and {}",
                    result_column->dumpStructure(),
                    current_column->dumpStructure());

            if (result_column->empty())
                result_column = std::move(current_column);
            else
                result_column->insertRangeFrom(*current_column, 0, current_column->size());
        }
        return result_column;
    }

    Block getArgumentsBlock(const ColumnsWithTypeAndName & arguments, size_t start_idx, size_t length) const
    {
        Block arguments_block;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            ColumnPtr column = arguments[i].column->convertToFullColumnIfConst()->cut(start_idx, length);
            String column_name = i < argument_names.size() && !argument_names[i].empty() ? argument_names[i] : arguments[i].name;
            arguments_block.insert(ColumnWithTypeAndName(column, arguments[i].type, column_name));
        }
        return arguments_block;
    }

    std::shared_ptr<UserDefinedWebAssemblyFunction> user_defined_function;
    std::shared_ptr<WebAssembly::WasmModule> wasm_module;
    String function_name;
    Strings argument_names;

    mutable WasmCompartmentPool compartment_pool;
};

std::shared_ptr<UserDefinedWebAssemblyFunction>
UserDefinedWebAssemblyFunctionFactory::addOrReplace(ASTPtr create_function_query, ContextPtr context)
{
    auto * create_query = typeid_cast<ASTCreateWasmFunctionQuery *>(create_function_query.get());
    if (!create_query)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Expected definition of WebAssembly function, got {}",
            create_function_query ? create_function_query->formatForErrorMessage() : "nullptr");

    auto function_def = create_query->validateAndGetDefinition();
    auto [wasm_module, module_hash] = context->getWasmModuleManager().getModule(function_def.module_name);
    transformEndianness<std::endian::big>(module_hash);
    String module_hash_str = getHexUIntLowercase(module_hash);
    if (function_def.module_hash.empty())
    {
        create_query->setModuleHash(module_hash_str);
    }
    else if (function_def.module_hash != module_hash_str)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Module '{}' has hash '{}', but '{}' expected",
            function_def.module_name,
            module_hash_str,
            function_def.module_hash);
    }

    const auto & internal_function_name
        = function_def.source_function_name.empty() ? function_def.function_name : function_def.source_function_name;
    std::shared_ptr<UserDefinedWebAssemblyFunction> wasm_func = UserDefinedWebAssemblyFunction::create(
        wasm_module,
        internal_function_name,
        function_def.argument_names,
        function_def.argument_types,
        function_def.result_type,
        function_def.abi_version,
        function_def.settings);

    std::unique_lock lock(registry_mutex);
    registry[function_def.function_name] = wasm_func;
    return wasm_func;
}

bool UserDefinedWebAssemblyFunctionFactory::has(const String & function_name)
{
    std::shared_lock lock(registry_mutex);
    return registry.contains(function_name);
}

FunctionOverloadResolverPtr UserDefinedWebAssemblyFunctionFactory::get(const String & function_name)
{
    std::shared_ptr<UserDefinedWebAssemblyFunction> wasm_func = nullptr;
    {
        std::shared_lock lock(registry_mutex);
        auto it = registry.find(function_name);
        if (it == registry.end())
        {
            throw Exception(
                ErrorCodes::RESOURCE_NOT_FOUND,
                "WebAssembly function '{}' not found in [{}]",
                function_name,
                fmt::join(registry | std::views::transform([](const auto & pair) { return pair.first; }), ", "));
        }
        wasm_func = it->second;
    }

    auto executable_function = std::make_shared<FunctionUserDefinedWasm>(function_name, std::move(wasm_func));
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(executable_function));
}

void UserDefinedWebAssemblyFunctionFactory::dropIfExists(const String & function_name)
{
    std::unique_lock lock(registry_mutex);
    registry.erase(function_name);
}

UserDefinedWebAssemblyFunctionFactory & UserDefinedWebAssemblyFunctionFactory::instance()
{
    static UserDefinedWebAssemblyFunctionFactory factory;
    return factory;
}

struct WebAssemblyFunctionSettingsConstraits : public IHints<>
{
    struct SettingDeffinition
    {
        explicit SettingDeffinition(std::function<void(std::string_view, const Field &)> check_, Field default_value_)
            : default_value(std::move(default_value_)), check(std::move(check_))
        {
            chassert(check);
        }

        Field default_value;
        std::function<void(std::string_view, const Field &)> check;
    };

    struct SettingUInt64Range
    {
        SettingDeffinition withDefault(UInt64 default_value) const
        {
            return SettingDeffinition(
                [min_ = this->min, max_ = this->max](std::string_view name, const Field & value) // NOLINT
                {
                    if (value.getType() != Field::Types::UInt64)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected UInt64, got '{}'", value.getTypeName());
                    UInt64 val = value.safeGet<UInt64>();
                    if (min_ > val || val > max_)
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Value {} for setting '{}' is out of range [{}, {}]",
                            val,
                            name,
                            min_,
                            max_ == std::numeric_limits<UInt64>::max() ? "inf" : std::to_string(max_));
                },
                Field(default_value));
        }

        UInt64 min = 0;
        UInt64 max = std::numeric_limits<UInt64>::max();
    };

    struct SettingStringFromSet
    {
        SettingDeffinition withDefault(String default_value) const
        {
            return SettingDeffinition(
                [values_ = this->values](std::string_view name, const Field & value) // NOLINT
                {
                    if (value.getType() != Field::Types::String)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected String, got '{}'", value.getTypeName());
                    if (!values_.contains(value.safeGet<String>()))
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Unexpected value '{}' for setting '{}', expected one of: {}",
                            value.safeGet<String>(),
                            name,
                            fmt::join(values_, ", "));
                },
                Field(default_value));
        }
        std::unordered_set<String> values;
    };

    const std::unordered_map<String, SettingDeffinition> settings_def = {
        /// Fuel limit for a single instance
        {"max_fuel", SettingUInt64Range{1'000, 100'000'000'000}.withDefault(100'000'000)},
        /// Memory limit for a single instance
        {"max_memory", SettingUInt64Range{64_KiB, 4_GiB}.withDefault(100_MiB)},
        /// Serialization format for input/output data for ABI V1
        {"serialization_format",
         SettingStringFromSet{{"MsgPack", "JSONEachRow", "CSV", "TSV", "TSVRaw", "RowBinary"}}.withDefault("MsgPack")},
        /// Limit for the number of rows in a single block
        {"max_input_block_size", SettingUInt64Range{0, DEFAULT_BLOCK_SIZE * 10}.withDefault(0)},
        /// Maximum number of instances of the webassembly module can be run in parallel for a single function
        {"max_instances", SettingUInt64Range{1, 128}.withDefault(128)},
    };

    std::vector<String> getAllRegisteredNames() const override
    {
        std::vector<String> result;
        result.reserve(settings_def.size());
        for (const auto & [name, _] : settings_def)
            result.push_back(name);
        return result;
    }

    void check(const String & name, const Field & value) const
    {
        auto it = settings_def.find(name);
        if (it == settings_def.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown setting name: '{}'{}", name, getHintsMessage(name));
        it->second.check(name, value);
    }

    Field getDefault(const String & name) const
    {
        auto it = settings_def.find(name);
        if (it == settings_def.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown setting name: '{}'{}", name, getHintsMessage(name));
        return it->second.default_value;
    }

    static const WebAssemblyFunctionSettingsConstraits & instance()
    {
        static WebAssemblyFunctionSettingsConstraits instance;
        return instance;
    }
};

void WebAssemblyFunctionSettings::trySet(const String & name, Field value)
{
    WebAssemblyFunctionSettingsConstraits::instance().check(name, value);
    settings.emplace(name, std::move(value));
}

Field WebAssemblyFunctionSettings::getValue(const String & name) const
{
    auto it = settings.find(name);
    if (it == settings.end())
        return WebAssemblyFunctionSettingsConstraits::instance().getDefault(name);
    return it->second;
}

}
