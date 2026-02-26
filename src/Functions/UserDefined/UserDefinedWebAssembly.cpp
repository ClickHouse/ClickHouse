#include <Functions/UserDefined/UserDefinedWebAssembly.h>

#include <ranges>
#include <base/hex.h>

#include <Columns/ColumnVector.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnTuple.h>

#include <Functions/IFunctionAdaptors.h>

#include <Formats/FormatFactory.h>
#include <Formats/formatBlock.h>

#include <Interpreters/Context.h>
#include <Interpreters/WasmModuleManager.h>
#include <Interpreters/WebAssembly/HostApi.h>
#include <Interpreters/WebAssembly/WasmMemory.h>

#include <Parsers/ASTCreateWasmFunctionQuery.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromStringWithMemoryTracking.h>

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
#include <Columns/ColumnString.h>
#include <base/extended_types.h>
#include <base/arithmeticOverflow.h>


#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/ProfileEvents.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>

namespace ProfileEvents
{
extern const Event WasmTotalExecuteMicroseconds;
extern const Event WasmSerializationMicroseconds;
extern const Event WasmDeserializationMicroseconds;
}


namespace DB
{

using namespace WebAssembly;

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int LOGICAL_ERROR;
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

        const auto & wasm_argument_types = function_declaration.getArgumentTypes();
        if (wasm_argument_types.size() != arguments.size())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "WebAssembly function '{}' expects {} arguments, but it's declared with {} arguments",
                function_name, wasm_argument_types.size(), arguments.size());
        }

        for (size_t i = 0; i < arguments.size(); ++i)
            checkDataTypeWithWasmValKind(arguments[i].get(), wasm_argument_types[i]);

        auto wasm_return_type = function_declaration.getReturnType();
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
            callable.template operator()<Int32>(args...)
            || callable.template operator()<UInt32>(args...)
            || callable.template operator()<Int64>(args...)
            || callable.template operator()<UInt64>(args...)
            || callable.template operator()<Float32>(args...)
            || callable.template operator()<Float64>(args...)
            || callable.template operator()<Int128>(args...)
            || callable.template operator()<UInt128>(args...)
        );
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
        ProfileEventTimeIncrement<Microseconds> timer_execute(ProfileEvents::WasmTotalExecuteMicroseconds);

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

class WasmMemoryManagerV01 final : public WasmMemoryManager
{
public:
    constexpr static std::string_view allocate_function_name = "clickhouse_create_buffer";
    constexpr static std::string_view deallocate_function_name = "clickhouse_destroy_buffer";

    static WasmFunctionDeclaration allocateFunctionDeclaration() { return {allocate_function_name, {WasmValKind::I32}, WasmValKind::I32}; }
    static WasmFunctionDeclaration deallocateFunctionDeclaration() { return {deallocate_function_name, {WasmValKind::I32}, std::nullopt}; }

    explicit WasmMemoryManagerV01(WasmCompartment * compartment_) : compartment(compartment_) { }

    WasmPtr createBuffer(WasmSizeT size) const override { return compartment->invoke<WasmPtr>(allocate_function_name, {size}); }
    void destroyBuffer(WasmPtr handle) const override { compartment->invoke<void>(deallocate_function_name, {handle}); }

    std::span<uint8_t> getMemoryView(WasmPtr handle) const override
    {
        if (handle == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wasm buffer is nullptr");

        const auto * raw_buffer_ptr = compartment->getMemory(handle, sizeof(WasmBuffer));
        WasmBuffer buffer;
        if (reinterpret_cast<uintptr_t>(raw_buffer_ptr) % alignof(WasmBuffer) != 0)
        {
            std::memcpy(&buffer, raw_buffer_ptr, sizeof(WasmBuffer));
        }
        else
        {
            buffer = *reinterpret_cast<const WasmBuffer *>(raw_buffer_ptr);
        }

        return {compartment->getMemory(buffer.ptr, buffer.size), buffer.size};
    }

private:
    WasmCompartment * compartment;
};

class UserDefinedWebAssemblyFunctionBufferedV1 : public UserDefinedWebAssemblyFunction
{
public:
    template <typename... Args>
    explicit UserDefinedWebAssemblyFunctionBufferedV1(Args &&... args) : UserDefinedWebAssemblyFunction(std::forward<Args>(args)...)
    {
        checkSignature();
    }

    void checkFunction(const WasmFunctionDeclaration & expected) const
    {
        checkFunctionDeclarationMatches(wasm_module->getExport(expected.getName()), expected);
    }

    void checkSignature() const
    {
        checkFunction(WasmFunctionDeclaration(function_name, {WasmValKind::I32, WasmValKind::I32}, WasmValKind::I32));
        checkFunction(WasmMemoryManagerV01::allocateFunctionDeclaration());
        checkFunction(WasmMemoryManagerV01::deallocateFunctionDeclaration());
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
        ProfileEventTimeIncrement<Microseconds> timer_execute(ProfileEvents::WasmTotalExecuteMicroseconds);

        String format_name = settings.getValue("serialization_format").safeGet<String>();

        if (num_rows == 0)
            return result_type->createColumn();
        if (num_rows >= std::numeric_limits<WasmSizeT>::max())
            throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large number of rows: {}", num_rows);

        auto wmm = std::make_unique<WasmMemoryManagerV01>(compartment);

        WasmMemoryGuard wasm_data = nullptr;
        if (!block.empty())
        {
            ProfileEventTimeIncrement<Microseconds> timer_serialize(ProfileEvents::WasmSerializationMicroseconds);
            StringWithMemoryTracking input_data;

            std::vector<const ColumnString *> string_columns;
            for (const auto & col : block)
            {
                const auto * string_col = checkAndGetColumn<ColumnString>(col.column.get());
                if (string_col && col.type->equals(DataTypeString()))
                    string_columns.push_back(string_col);
                else
                    string_columns.clear();
            }

            {
                WriteBufferFromStringWithMemoryTracking buf(input_data);
                auto out = context->getOutputFormat(format_name, buf, block.cloneEmpty());
                formatBlock(out, block);
            }

            wasm_data = allocateInWasmMemory(wmm.get(), input_data.size());
            auto wasm_mem = wasm_data.getMemoryView();

            if (wasm_mem.size() != input_data.size())
                throw Exception(ErrorCodes::WASM_ERROR,
                    "Cannot allocate buffer of size {}, got {} "
                    "Maybe '{}' function implementation in WebAssembly module is incorrect",
                    input_data.size(), wasm_mem.size(), WasmMemoryManagerV01::allocate_function_name);

            std::copy(input_data.data(), input_data.data() + input_data.size(), wasm_mem.begin());
        }

        auto result_ptr = compartment->invoke<WasmPtr>(function_name, {wasm_data.getHandle(), static_cast<WasmSizeT>(num_rows)});
        if (result_ptr == 0)
            throw Exception(ErrorCodes::WASM_ERROR, "WebAssembly function '{}' returned nullptr", function_name);

        WasmMemoryGuard result(wmm.get(), result_ptr);
        auto result_data = result.getMemoryView();
        ReadBufferFromMemory inbuf(result_data.data(), result_data.size());

        ProfileEventTimeIncrement<Microseconds> timer_deserialize(ProfileEvents::WasmDeserializationMicroseconds);

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
        case WasmAbiVersion::RowDirect:
            return std::make_unique<UserDefinedWebAssemblyFunctionSimple>(
                wasm_module_, function_name_, argument_names_, arguments_, result_type_, std::move(function_settings));
        case WasmAbiVersion::BufferedV1:
            return std::make_unique<UserDefinedWebAssemblyFunctionBufferedV1>(
                wasm_module_, function_name_, argument_names_, arguments_, result_type_, std::move(function_settings));
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR, "Unknown WebAssembly ABI version: {}", std::to_underlying(abi_type));
}

String toString(WasmAbiVersion abi_type)
{
    switch (abi_type)
    {
        case WasmAbiVersion::RowDirect:
            return "ROW_DIRECT";
        case WasmAbiVersion::BufferedV1:
            return "BUFFERED_V1";
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR, "Unknown WebAssembly ABI version: {}", std::to_underlying(abi_type));
}

WasmAbiVersion getWasmAbiFromString(const String & str)
{
    for (auto abi_type : {WasmAbiVersion::RowDirect, WasmAbiVersion::BufferedV1})
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
        : Base(limit, getLogger("WasmCompartmentPool"))
        , wasm_module(std::move(wasm_module_))
        , module_cfg(std::move(module_cfg_))
    {
        LOG_DEBUG(log, "WasmCompartmentPool created with limit: {}", limit);
    }

    Entry acquire() { return get(-1); }

protected:
    ObjectPtr allocObject() override
    {
        LOG_DEBUG(log, "Allocating new WasmCompartment");
        return wasm_module->instantiate(module_cfg);
    }

private:
    std::shared_ptr<WebAssembly::WasmModule> wasm_module;
    WebAssembly::WasmModule::Config module_cfg;
};


WebAssembly::WasmModule::Config getWasmModuleConfigFromFunctionSettings(const WebAssemblyFunctionSettings & function_settings)
{
    WebAssembly::WasmModule::Config cfg;

    UInt64 max_fuel = function_settings.getValue("max_fuel").safeGet<UInt64>();
    if (common::mulOverflow(max_fuel, 1024, cfg.fuel_limit))
        cfg.fuel_limit = std::numeric_limits<UInt64>::max();

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
    bool isDeterministic() const override { return false; }
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

    bool useDefaultImplementationForConstants() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }

    bool isSuitableForConstantFolding() const override { return false; }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto compartment_entry = compartment_pool.acquire();
        auto * compartment_ptr = &(*compartment_entry);
        return execute(compartment_ptr, arguments, input_rows_count);
    }

    ColumnPtr executeImplDryRun(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        MutableColumnPtr result_column = user_defined_function->getResultType()->createColumn();
        result_column->insertManyDefaults(input_rows_count);
        return result_column;
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
UserDefinedWebAssemblyFunctionFactory::addOrReplace(ASTPtr create_function_query, WasmModuleManager & module_manager)
{
    auto * create_query = typeid_cast<ASTCreateWasmFunctionQuery *>(create_function_query.get());
    if (!create_query)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected definition of WebAssembly function, got {}",
            create_function_query ? create_function_query->formatForErrorMessage() : "nullptr");

    auto function_def = create_query->validateAndGetDefinition();
    auto [wasm_module, module_hash] = module_manager.getModule(function_def.module_name);
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
            "WebAssembly module '{}' digest mismatch, expected {}, got {}",
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

bool UserDefinedWebAssemblyFunctionFactory::dropIfExists(const String & function_name)
{
    std::unique_lock lock(registry_mutex);
    return registry.erase(function_name) > 0;
}

UserDefinedWebAssemblyFunctionFactory & UserDefinedWebAssemblyFunctionFactory::instance()
{
    static UserDefinedWebAssemblyFunctionFactory factory;
    return factory;
}

struct WebAssemblyFunctionSettingsConstraits : public IHints<>
{
    struct SettingDefinition
    {
        explicit SettingDefinition(std::function<void(std::string_view, const Field &)> check_, Field default_value_)
            : default_value(std::move(default_value_)), check(std::move(check_))
        {
            chassert(check);
        }

        Field default_value;
        std::function<void(std::string_view, const Field &)> check;
    };

    struct SettingUInt64Range
    {
        SettingDefinition withDefault(UInt64 default_value) const
        {
            return SettingDefinition(
                [min_ = this->min, max_ = this->max](std::string_view name, const Field & value) // NOLINT
                {
                    if (value.getType() != Field::Types::UInt64)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected UInt64, got '{}'", value.getTypeName());
                    UInt64 val = value.safeGet<UInt64>();
                    if (min_ > val || val > max_)
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Value {} for setting '{}' is out of range [{}, {}]",
                            val, name, min_, max_ == std::numeric_limits<UInt64>::max() ? "inf" : std::to_string(max_));
                },
                Field(default_value));
        }

        UInt64 min = 0;
        UInt64 max = std::numeric_limits<UInt64>::max();
    };

    struct SettingStringFromSet
    {
        SettingDefinition withDefault(String default_value) const
        {
            return SettingDefinition(
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

    const std::unordered_map<String, SettingDefinition> settings_def = {
        /// Fuel limit for a single instance
        {"max_fuel", SettingUInt64Range{}.withDefault(100'000)},
        /// Memory limit for a single instance
        {"max_memory", SettingUInt64Range{64_KiB, 4_GiB}.withDefault(100_MiB)},
        /// Serialization format for input/output data for ABI what uses serialization
        {"serialization_format", SettingStringFromSet{{"MsgPack", "JSONEachRow", "CSV", "TSV", "TSVRaw", "RowBinary"}}.withDefault("MsgPack")},
        /// Limit for the number of rows in a single block
        {"max_input_block_size", SettingUInt64Range{0, DEFAULT_BLOCK_SIZE * 10}.withDefault(0)},
        /// Maximum number of instances of the webassembly module can be run in parallel for a single function
        {"max_instances", SettingUInt64Range{1, 1024}.withDefault(128)},
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
