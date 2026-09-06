#include <Functions/UserDefined/UserDefinedWebAssembly.h>
#include <Functions/UserDefined/UserDefinedWebAssemblyScriptAbi.h>
#include <Functions/UserDefined/UserDefinedWebAssemblyTypeHelpers.h>

#include <ranges>
#include <base/hex.h>

#include <Columns/ColumnVector.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnTuple.h>

#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>

#include <Formats/FormatFactory.h>
#include <Formats/formatBlock.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/WasmModuleManager.h>
#include <Interpreters/WebAssembly/HostApi.h>
#include <Interpreters/WebAssembly/WasmMemory.h>

#include <Parsers/ASTCreateWasmFunctionQuery.h>

#include <Interpreters/castColumn.h>
#include <IO/NullWriteBuffer.h>
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

namespace Setting
{
extern const SettingsUInt64 webassembly_udf_max_fuel;
extern const SettingsUInt64 webassembly_udf_max_memory;
extern const SettingsUInt64 webassembly_udf_max_input_block_size;
extern const SettingsUInt64 webassembly_udf_max_instances;
extern const SettingsFloat webassembly_udf_input_split_memory_ratio;
}

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
    WebAssemblyFunctionSettings function_settings_,
    bool is_deterministic_)
    : function_name(function_name_)
    , argument_names(argument_names_)
    , arguments(arguments_)
    , result_type(result_type_)
    , wasm_module(wasm_module_)
    , settings(std::move(function_settings_))
    , is_deterministic(is_deterministic_)
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

    /// Arguments and the result cross the boundary as WebAssembly values, so guest memory is
    /// never touched.
    bool requiresGuestLinearMemory() const override { return false; }

    bool serializesInputBlockToGuestMemory() const override { return false; }

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


    static void checkDataTypeWithWasmValKind(const IDataType * type, WasmValKind kind)
    {
        bool is_data_type_compatible = tryExecuteForNumericTypes(
            [type, kind]<typename T>() { return typeid_cast<const DataTypeNumber<T> *>(type) && wasmKindFor<T>() == kind; });
        if (!is_data_type_compatible)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "WebAssembly function expects type compatible with {}, but got {}",
                toString(kind),
                type->getName());
    }

    MutableColumnPtr
    executeOnBlock(WebAssembly::WasmCompartment * compartment, const Block & block, ContextPtr, size_t num_rows, StopToken stop_token) const override
    {
        ProfileEventTimeIncrement<Microseconds> timer_execute(ProfileEvents::WasmTotalExecuteMicroseconds);

        auto get_column_element = []<typename T>(const IColumn * column, size_t row_idx, WasmVal & val)
        {
            if (auto * column_typed = checkAndGetColumn<ColumnVector<T>>(column))
            {
                val = static_cast<typename WasmStorageType<T>::Type>(column_typed->getElement(row_idx));
                return true;
            }
            return false;
        };

        MutableColumnPtr result_column = result_type->createColumn();
        auto invoke_and_set_column = [&]<typename T>(const VectorWithMemoryTracking<WasmVal> & args)
        {
            if (auto * column_typed = typeid_cast<ColumnVector<T> *>(result_column.get()))
            {
                auto value = compartment->invoke<typename WasmStorageType<T>::Type>(function_name, args, stop_token);
                column_typed->insertValue(static_cast<T>(value));
                return true;
            }
            return false;
        };

        size_t num_columns = block.columns();
        VectorWithMemoryTracking<WasmVal> wasm_args(num_columns);
        for (size_t row_idx = 0; row_idx < num_rows; ++row_idx)
        {
            for (size_t col_idx = 0; col_idx < num_columns; ++col_idx)
            {
                const auto & column = block.getByPosition(col_idx);
                if (!tryExecuteForNumericTypes(get_column_element, column.column.get(), row_idx, wasm_args[col_idx]))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot convert {} to WebAssembly type", column.type->getName());
            }

            if (!tryExecuteForNumericTypes(invoke_and_set_column, wasm_args))
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

    static WasmFunctionDeclaration allocateFunctionDeclaration() { return {"", allocate_function_name, {WasmValKind::I32}, WasmValKind::I32}; }
    static WasmFunctionDeclaration deallocateFunctionDeclaration() { return {"", deallocate_function_name, {WasmValKind::I32}, std::nullopt}; }

    explicit WasmMemoryManagerV01(WasmCompartment * compartment_, StopToken stop_token_)
        : compartment(compartment_)
        , stop_token(stop_token_)
    {
    }

    WasmPtr createBuffer(WasmSizeT size) const override { return compartment->invoke<WasmPtr>(allocate_function_name, {size}, stop_token); }
    void destroyBuffer(WasmPtr handle) const override { compartment->invoke<void>(deallocate_function_name, {handle}, stop_token); }

    std::span<uint8_t> getMemoryView(WasmPtr handle) const override
    {
        if (handle == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wasm buffer is nullptr");

        auto raw_buffer_span = compartment->getMemory(handle, sizeof(WasmBuffer));
        const auto * raw_buffer_ptr = raw_buffer_span.data();
        auto ptr = loadFromWasmMemory<WasmPtr>(raw_buffer_ptr);
        auto size = loadFromWasmMemory<WasmSizeT>(raw_buffer_ptr + sizeof(WasmPtr));
        return compartment->getMemory(ptr, size);
    }

private:
    WasmCompartment * compartment;
    StopToken stop_token;
};

class UserDefinedWebAssemblyFunctionBufferedV1 : public UserDefinedWebAssemblyFunction
{
public:
    template <typename... Args>
    explicit UserDefinedWebAssemblyFunctionBufferedV1(Args &&... args) : UserDefinedWebAssemblyFunction(std::forward<Args>(args)...)
    {
        checkSignature();
    }

    /// The input block is serialized into a buffer the guest allocates, and the result read
    /// back from guest memory.
    bool requiresGuestLinearMemory() const override { return true; }

    bool serializesInputBlockToGuestMemory() const override { return true; }

    void checkFunction(const WasmFunctionDeclaration & expected) const
    {
        checkFunctionDeclarationMatches(wasm_module->getExport(expected.getName()), expected);
    }

    void checkSignature() const
    {
        checkFunction(WasmFunctionDeclaration("", function_name, {WasmValKind::I32, WasmValKind::I32}, WasmValKind::I32));
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
                    ErrorCodes::WASM_ERROR,
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

        if (result_chunk.getNumColumns() != result_block.columns())
            throw Exception(
                ErrorCodes::WASM_ERROR,
                "WebAssembly function returned a result with {} columns, expected {}",
                result_chunk.getNumColumns(), result_block.columns());

        result_block.setColumns(result_chunk.detachColumns());
    }

    MutableColumnPtr
    executeOnBlock(WebAssembly::WasmCompartment * compartment, const Block & block, ContextPtr context, size_t num_rows, StopToken stop_token) const override
    {
        ProfileEventTimeIncrement<Microseconds> timer_execute(ProfileEvents::WasmTotalExecuteMicroseconds);

        String format_name = settings.getValue("serialization_format").safeGet<String>();

        if (num_rows == 0)
            return result_type->createColumn();
        if (num_rows >= std::numeric_limits<WasmSizeT>::max())
            throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large number of rows: {}", num_rows);

        auto wmm = std::make_unique<WasmMemoryManagerV01>(compartment, stop_token);

        WasmMemoryGuard wasm_data = nullptr;
        if (!block.empty())
        {
            ProfileEventTimeIncrement<Microseconds> timer_serialize(ProfileEvents::WasmSerializationMicroseconds);
            StringWithMemoryTracking input_data;

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

        auto result_ptr = compartment->invoke<WasmPtr>(function_name, {wasm_data.getHandle(), static_cast<WasmSizeT>(num_rows)}, stop_token);
        if (result_ptr == 0)
            throw Exception(ErrorCodes::WASM_ERROR, "WebAssembly function '{}' returned nullptr", function_name);

        WasmMemoryGuard result(wmm.get(), result_ptr);
        auto result_data = result.getMemoryView();
        ReadBufferFromMemory inbuf(result_data.data(), result_data.size());

        ProfileEventTimeIncrement<Microseconds> timer_deserialize(ProfileEvents::WasmDeserializationMicroseconds);

        Block result_header({ColumnWithTypeAndName(result_type->createColumn(), result_type, "result")});

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
    WebAssemblyFunctionSettings function_settings,
    bool is_deterministic_)
{
    switch (abi_type)
    {
        case WasmAbiVersion::RowDirect:
            return std::make_unique<UserDefinedWebAssemblyFunctionSimple>(
                wasm_module_, function_name_, argument_names_, arguments_, result_type_, std::move(function_settings), is_deterministic_);
        case WasmAbiVersion::BufferedV1:
            return std::make_unique<UserDefinedWebAssemblyFunctionBufferedV1>(
                wasm_module_, function_name_, argument_names_, arguments_, result_type_, std::move(function_settings), is_deterministic_);
        case WasmAbiVersion::AssemblyScript:
            return createUserDefinedWebAssemblyFunctionAssemblyScript(
                wasm_module_, function_name_, argument_names_, arguments_, result_type_, std::move(function_settings), is_deterministic_);
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
        case WasmAbiVersion::AssemblyScript:
            return "ASSEMBLYSCRIPT";
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR, "Unknown WebAssembly ABI version: {}", std::to_underlying(abi_type));
}

WasmAbiVersion getWasmAbiFromString(const String & str)
{
    for (auto abi_type : {WasmAbiVersion::RowDirect, WasmAbiVersion::BufferedV1, WasmAbiVersion::AssemblyScript})
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
        unsigned limit,
        std::shared_ptr<WebAssembly::WasmModule> wasm_module_,
        WebAssembly::WasmModule::Config module_cfg_,
        StopToken stop_token_)
        : Base(limit, getLogger("WasmCompartmentPool"))
        , wasm_module(std::move(wasm_module_))
        , module_cfg(std::move(module_cfg_))
        , stop_token(std::move(stop_token_))
    {
        LOG_DEBUG(log, "WasmCompartmentPool created with limit: {}", limit);
    }

    Entry acquire() { return get(-1); }

protected:
    ObjectPtr allocObject() override
    {
        LOG_DEBUG(log, "Allocating new WasmCompartment");
        return wasm_module->instantiate(module_cfg, stop_token);
    }

private:
    std::shared_ptr<WebAssembly::WasmModule> wasm_module;
    WebAssembly::WasmModule::Config module_cfg;

    std::mutex acquire_mutex;
    StopToken stop_token;
};


static WebAssembly::WasmModule::Config getWasmModuleConfig(ContextPtr context, WebAssembly::FuelMode fuel_mode)
{
    WebAssembly::WasmModule::Config cfg(fuel_mode);

    UInt64 max_fuel = context->getSettingsRef()[Setting::webassembly_udf_max_fuel];
    if (common::mulOverflow(max_fuel, 1024, cfg.fuel_limit))
        cfg.fuel_limit = std::numeric_limits<UInt64>::max();

    cfg.memory_limit = context->getSettingsRef()[Setting::webassembly_udf_max_memory];

    return cfg;
}

class FunctionUserDefinedWasm final : public IFunction
{
public:
    FunctionUserDefinedWasm(String function_name_, std::shared_ptr<UserDefinedWebAssemblyFunction> udf_, ContextPtr context_)
        : user_defined_function(std::move(udf_))
        , wasm_module(user_defined_function->getModule())
        , function_name(std::move(function_name_))
        , argument_names(user_defined_function->getArgumentNames())
        , context(std::move(context_))
        , interrupt_source()
        , compartment_pool(
              static_cast<UInt32>(context->getSettingsRef()[Setting::webassembly_udf_max_instances]),
              wasm_module,
              getWasmModuleConfig(context, user_defined_function->getSettings().getFuelMode()),
              interrupt_source.get_token())
    {
        const size_t configured_memory_limit = context->getSettingsRef()[Setting::webassembly_udf_max_memory];
        if (configured_memory_limit != 0)
            module_memory_limit = configured_memory_limit;
        serialization_format = user_defined_function->getSettings().getValue("serialization_format").safeGet<String>();
    }

    /// Bytes a serialized block carries besides its rows: `BuffersWriter` prefixes the payloads
    /// with a `UInt64` column count, a `UInt64` row count and one `UInt64` size per column.
    size_t blockFramingBytes(size_t num_columns) const
    {
        return serialization_format == "Buffers" ? sizeof(UInt64) * (2 + num_columns) : 0;
    }

    String getName() const override { return function_name; }
    bool isVariadic() const override { return false; }
    bool isDeterministic() const override { return user_defined_function->getIsDeterministic(); }
    bool isSpatialPredicate() const override
    {
        auto val = user_defined_function->getSettings().getValue("is_spatial_predicate");
        if (val.getType() == Field::Types::Bool)
            return val.safeGet<bool>();
        return val.safeGet<UInt64>() != 0;
    }
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

            /// Allow implicit coercions: same kind, i32→i64, any int→any float, f32→f64.
            auto actual_kind = wasmKindForDataType(arguments[i].get());
            auto expected_kind = wasmKindForDataType(expected_arguments[i].get());
            if (actual_kind && expected_kind && canCoerce(*actual_kind, *expected_kind))
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

    /// When the function is deterministic, returning true here causes the framework to
    /// call executeImpl with a single-row block and wrap the result in ColumnConst.
    /// That ColumnConst is then recognised by the Analyzer's constant-folding check
    /// (isColumnConst(*column) in resolveFunction.cpp). Without this, executeImpl
    /// returns a plain ColumnVector which the Analyzer does not fold.
    bool useDefaultImplementationForConstants() const override { return user_defined_function->getIsDeterministic(); }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }

    bool isSuitableForConstantFolding() const override { return user_defined_function->getIsDeterministic(); }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        /// Memory grows in whole pages and the limiter refuses a growth crossing the cap, so a
        /// `webassembly_udf_max_memory` below one page leaves the guest unable to hold anything.
        /// Checked here rather than at instantiation, which does not know the ABI and would also
        /// reject a function that never touches the memory.
        /// An empty block allocates nothing in the guest, so a memory it could never use does not
        /// make the call impossible.
        if (input_rows_count > 0 && module_memory_limit && *module_memory_limit < WebAssembly::WASM_PAGE_SIZE
            && user_defined_function->requiresGuestLinearMemory())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "WebAssembly memory limit is {} bytes, which is less than a single {} byte page",
                *module_memory_limit,
                WebAssembly::WASM_PAGE_SIZE);

        auto compartment_entry = compartment_pool.acquire();
        auto * compartment_ptr = &(*compartment_entry);
        try
        {
            return execute(compartment_ptr, arguments, input_rows_count);
        }
        catch (...)
        {
            /// A trapped/faulted compartment may have leftovers, half-allocated buffers,
            /// or otherwise inconsistent guest state. Drop it so the pool recreates it.
            compartment_entry.expire();
            throw;
        }
    }

    ColumnPtr executeImplDryRun(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Deterministic functions must actually run during dry-run so the Analyzer can constant-fold them.
        /// Non-deterministic functions return defaults to avoid WASM execution at query-analysis time.
        if (user_defined_function->getIsDeterministic())
            return executeImpl(arguments, result_type, input_rows_count);

        MutableColumnPtr result_column = user_defined_function->getResultType()->createColumn();
        result_column->insertManyDefaults(input_rows_count);
        return result_column;
    }

    void cancelExecution() const override
    {
        interrupt_source.request_stop();
    }

private:
    /// The size one call's serialized input is grown up to, empty when the input is not split by
    /// its size. A batch is never taken below a single row: splitting only decides how many rows
    /// share a call, so a row too large for the guest's memory fails inside its allocator, and no
    /// budget can rescue it.
    std::optional<size_t> getInputBudget(WebAssembly::WasmCompartment * compartment, size_t fixed_block_size) const
    {
        /// Read before the range is checked, because a value out of range is only rejected where
        /// a batch size is actually decided, but a zero has to be honoured everywhere.
        const Float64 memory_ratio = static_cast<Float64>(context->getSettingsRef()[Setting::webassembly_udf_input_split_memory_ratio].value);

        /// A zero budget is the opt-out: with no part of the memory set aside for a call's input
        /// there is nothing to size a batch against, so a zero `webassembly_udf_max_input_block_size`
        /// keeps its original meaning of one call per pipeline block.
        if (memory_ratio == 0.0)
            return {};

        /// An ABI that ships no serialized input block into guest memory has no size for the
        /// memory to bound and nothing to measure - neither one passing its arguments as
        /// WebAssembly values, whose compartment may well hold nothing at all because a module
        /// declaring `memory 0 0` stays callable this way, nor `ASSEMBLYSCRIPT`, which builds one
        /// object per row and would otherwise be bounded by a `serialization_format` it ignores.
        if (!user_defined_function->serializesInputBlockToGuestMemory())
            return {};

        /// An explicit block size caps the rows per call instead of splitting by size.
        if (fixed_block_size > 0)
            return {};

        /// The ratio only sizes a batch past this point, so an out-of-range value is only rejected
        /// past this point: a query that pins the rows per call never uses it and must not be
        /// failed by it.
        if (!(memory_ratio > 0.0 && memory_ratio <= 1.0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Setting `webassembly_udf_input_split_memory_ratio` must be at least 0 and at most 1, got {}", memory_ratio);

        /// Budget a batch against a fraction of the memory the module starts with, leaving the
        /// rest for its own working set beside the input buffer. The declared initial size is
        /// what the basis must be: the current size moves with `memory.grow` and never shrinks,
        /// and compartments are pooled, so a basis taken from it would depend on which instance a
        /// worker picked up and on what earlier blocks made it grow. Identical blocks would then
        /// reach the guest in different batches, which it observes through the row count.
        ///
        /// The ceiling is no basis either, even though it is stable: a guest allocator usually
        /// serves the input out of a heap far smaller than the maximum the memory may reach, so
        /// budgeting against the ceiling proposes batches the guest cannot allocate.
        ///
        /// A module declared as `memory 0 N` starts with no pages, so the initial size alone
        /// would be zero and would disable splitting; such a memory falls back to the ceiling,
        /// which the guest can still grow into and which is equally the same for every instance.
        const std::optional<size_t> initial_memory = compartment->getInitialLinearMemorySize();
        const std::optional<size_t> budget_basis = initial_memory.value_or(0) > 0 ? initial_memory : compartment->getMaxLinearMemorySize();
        if (!budget_basis)
            return {};
        return static_cast<size_t>(static_cast<Float64>(*budget_basis) * memory_ratio);
    }

    /// Measure the wire instead of predicting it: write each row through the real output format
    /// into a `NullWriteBuffer` and read the byte count off it. Delimiters, keys, enum labels and
    /// the configured tokens are all counted, because the serializer writes them.
    ///
    /// Reports the payload of a row alone: a block-framing format writes its framing on every
    /// `write`, and the measurement writes one row at a time, so the framing would otherwise be
    /// charged to each row instead of once to the call that carries them.
    template <typename OnRow>
    void measureRows(const ColumnsWithTypeAndName & arguments, size_t input_rows_count, OnRow && on_row) const
    {
        /// A function without arguments is handed no input buffer at all, so there is nothing to
        /// measure and nothing for the size of an input to decide.
        if (arguments.empty())
            return;

        /// Cut each row out of the original arguments instead of materializing the whole block
        /// first: a wide `ColumnConst` argument would otherwise be expanded to one copy per row
        /// on the host, which is the very input the splitting below exists to rescue.
        auto header = getArgumentsBlock(arguments, 0, 0);
        NullWriteBuffer measure_buf;
        auto measure_out = context->getOutputFormat(serialization_format, measure_buf, header.cloneEmpty());
        const size_t framing_per_write = blockFramingBytes(header.columns());

        size_t written_before = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            measure_out->write(getArgumentsBlock(arguments, row, 1));

            const size_t written_after = measure_buf.count();
            on_row(row, written_after - written_before - framing_per_write);
            written_before = written_after;
        }
    }

    /// What one call's stream costs beyond its rows: the framing a block format writes on every
    /// `write`, plus whatever the format wraps the rows in once - `JSONEachRow` under
    /// `output_format_json_array_of_rows` brackets them, for instance. The wrapping is measured
    /// rather than modelled, by finalizing an empty stream through the real format.
    ///
    /// The per-row measurement runs one long-lived stream, so it charges the opening bracket to
    /// its first row and every later row a separator instead of that bracket. Counting the whole
    /// wrapping again here therefore overstates a call by the few bytes of an opening bracket,
    /// which only ever moves a batch boundary one row earlier. An input is never understated,
    /// which is what the batching budget relies on.
    size_t perCallOverheadBytes(const ColumnsWithTypeAndName & arguments) const
    {
        const size_t framing = blockFramingBytes(arguments.size());
        if (arguments.empty())
            return framing;

        NullWriteBuffer overhead_buf;
        auto overhead_out = context->getOutputFormat(serialization_format, overhead_buf, getArgumentsBlock(arguments, 0, 0));
        overhead_out->finalize();
        return framing + overhead_buf.count();
    }

    void appendBatchResult(MutableColumnPtr & result_column, MutableColumnPtr batch_column) const
    {
        if (!result_column->structureEquals(*batch_column))
            throw Exception(
                ErrorCodes::WASM_ERROR,
                "Different column types in result blocks: {} and {}",
                result_column->dumpStructure(),
                batch_column->dumpStructure());

        if (result_column->empty())
            result_column = std::move(batch_column);
        else
            result_column->insertRangeFrom(*batch_column, 0, batch_column->size());
    }

    ColumnPtr execute(WebAssembly::WasmCompartment * compartment, const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        /// A module whose linear memory is bounded at zero bytes can hold no input at all, whatever
        /// the batching is. This is reported before any measurement, because a function without
        /// arguments has no row to attribute the failure to and would otherwise fail inside the
        /// guest allocator.
        if (input_rows_count > 0 && user_defined_function->requiresGuestLinearMemory()
            && compartment->getMaxLinearMemorySize() == 0)
            throw Exception(ErrorCodes::WASM_ERROR,
                "The maximum linear memory of the module is 0 bytes, so it cannot hold the input of the function");

        MutableColumnPtr result_column = user_defined_function->getResultType()->createColumn();

        const size_t fixed_block_size = context->getSettingsRef()[Setting::webassembly_udf_max_input_block_size];
        const std::optional<size_t> budget = getInputBudget(compartment, fixed_block_size);

        size_t batch_start = 0;
        auto flush_batch = [&](size_t end_idx)
        {
            if (end_idx <= batch_start)
                return;
            const size_t batch_size = end_idx - batch_start;
            auto block = getArgumentsBlock(arguments, batch_start, batch_size);
            auto stop_token = interrupt_source.get_token();
            appendBatchResult(result_column, user_defined_function->executeOnBlock(compartment, block, context, batch_size, stop_token));
            batch_start = end_idx;
        };

        if (budget)
        {
            /// What a call costs beyond its rows, which no per-row measurement sees.
            const size_t block_framing_bytes = perCallOverheadBytes(arguments);

            /// Flush before the next row would cross the budget. A stride derived from the
            /// average row size cannot bound a skewed block: one huge row among many tiny ones
            /// would still share a call with its neighbours.
            ///
            /// A row that is itself past the budget is still passed on its own: the split stops
            /// at one row per call, and whether the guest can hold that row is for its allocator
            /// to say.
            size_t running_bytes = 0;
            measureRows(arguments, input_rows_count, [&](size_t row, size_t row_bytes)
            {
                if (row > batch_start && running_bytes + row_bytes + block_framing_bytes > *budget)
                {
                    flush_batch(row);
                    running_bytes = 0;
                }
                running_bytes += row_bytes;
            });
        }
        else if (fixed_block_size > 0)
        {
            for (size_t row = fixed_block_size; row < input_rows_count; row += fixed_block_size)
                flush_batch(row);
        }

        flush_batch(input_rows_count);
        return result_column;
    }

    Block getArgumentsBlock(const ColumnsWithTypeAndName & arguments, size_t start_idx, size_t length) const
    {
        const auto & declared_arguments = user_defined_function->getArguments();
        Block arguments_block;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            /// Cut first, materialize second: `ColumnConst::cut` is O(1), while materializing
            /// the whole block first would make the per-row measurement O(rows^2).
            ColumnPtr column = arguments[i].column->cut(start_idx, length)->convertToFullColumnIfConst();
            String column_name = i < argument_names.size() && !argument_names[i].empty() ? argument_names[i] : arguments[i].name;
            /// Cast to the declared type so serialization uses the correct width.
            /// Without this, e.g. Int8 passed to an Int32 parameter would be serialized
            /// as 1 byte by RowBinary instead of 4, causing the WASM module to read garbage.
            const DataTypePtr & declared_type = declared_arguments[i];
            if (!arguments[i].type->equals(*declared_type))
                column = castColumn(ColumnWithTypeAndName(column, arguments[i].type, column_name), declared_type);
            arguments_block.insert(ColumnWithTypeAndName(column, declared_type, column_name));
        }
        return arguments_block;
    }

    std::shared_ptr<UserDefinedWebAssemblyFunction> user_defined_function;
    std::shared_ptr<WebAssembly::WasmModule> wasm_module;
    String function_name;
    Strings argument_names;
    ContextPtr context;

    String serialization_format;

    /// Configured `webassembly_udf_max_memory` in bytes, empty when the host caps nothing.
    std::optional<size_t> module_memory_limit;

    mutable StopSource interrupt_source;
    mutable WasmCompartmentPool compartment_pool;
};

UserDefinedWebAssemblyFunctionFactory::RegisteredFunction
UserDefinedWebAssemblyFunctionFactory::prepareFunction(ASTPtr create_function_query, WasmModuleManager & module_manager) const
{
    auto * create_query = typeid_cast<ASTCreateWasmFunctionQuery *>(create_function_query.get());
    if (!create_query)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected definition of WebAssembly function, got {}",
            create_function_query ? create_function_query->formatForErrorMessage() : "nullptr");

    auto function_def = create_query->validateAndGetDefinition();
    auto fuel_mode = function_def.settings.getFuelMode();
    auto [wasm_module, module_hash] = module_manager.getModule(function_def.module_name, fuel_mode);
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
        function_def.settings,
        function_def.is_deterministic);

    return RegisteredFunction{function_def.function_name, std::move(wasm_func), std::move(create_function_query)};
}

std::shared_ptr<UserDefinedWebAssemblyFunction>
UserDefinedWebAssemblyFunctionFactory::addOrReplace(ASTPtr create_function_query, WasmModuleManager & module_manager)
{
    auto registered_function = prepareFunction(std::move(create_function_query), module_manager);
    auto wasm_func = registered_function.function;
    addOrReplace(std::move(registered_function));
    return wasm_func;
}

void UserDefinedWebAssemblyFunctionFactory::addOrReplace(RegisteredFunction registered_function)
{
    std::unique_lock lock(registry_mutex);
    registry[registered_function.sql_name] = RegistryEntry{std::move(registered_function.function), std::move(registered_function.create_query)};
}

void UserDefinedWebAssemblyFunctionFactory::replaceAll(VectorWithMemoryTracking<RegisteredFunction> registered_functions)
{
    UnorderedMapWithMemoryTracking<String, RegistryEntry> new_registry;
    new_registry.reserve(registered_functions.size());
    for (auto & registered_function : registered_functions)
        new_registry[registered_function.sql_name] = RegistryEntry{std::move(registered_function.function), std::move(registered_function.create_query)};

    std::unique_lock lock(registry_mutex);
    registry = std::move(new_registry);
}

bool UserDefinedWebAssemblyFunctionFactory::has(const String & function_name) const
{
    std::shared_lock lock(registry_mutex);
    return registry.contains(function_name);
}

void UserDefinedWebAssemblyFunctionFactory::checkWebAssemblyIsAvailable(const ContextPtr & context)
{
    /// `getWasmModuleManager` always throws `SUPPORT_IS_DISABLED` here, and it is the single place that
    /// words the difference between the engine being turned off and being absent from the build.
    if (!context->hasWasmModuleManager())
        context->getWasmModuleManager();
}

FunctionOverloadResolverPtr UserDefinedWebAssemblyFunctionFactory::get(const String & function_name, ContextPtr context)
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
        wasm_func = it->second.function;
    }

    auto executable_function = std::make_shared<FunctionUserDefinedWasm>(function_name, std::move(wasm_func), std::move(context));
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(executable_function));
}

FunctionOverloadResolverPtr UserDefinedWebAssemblyFunctionFactory::tryGet(const String & function_name, ContextPtr context)
{
    std::shared_ptr<UserDefinedWebAssemblyFunction> wasm_func = nullptr;
    {
        std::shared_lock lock(registry_mutex);
        auto it = registry.find(function_name);
        if (it == registry.end())
            return nullptr;
        wasm_func = it->second.function;
    }

    auto executable_function = std::make_shared<FunctionUserDefinedWasm>(function_name, std::move(wasm_func), std::move(context));
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(executable_function));
}

bool UserDefinedWebAssemblyFunctionFactory::dropIfExists(const String & function_name)
{
    std::unique_lock lock(registry_mutex);
    return registry.erase(function_name) > 0;
}

VectorWithMemoryTracking<UserDefinedWebAssemblyFunctionFactory::RegisteredFunction> UserDefinedWebAssemblyFunctionFactory::getAllFunctions() const
{
    std::shared_lock lock(registry_mutex);
    VectorWithMemoryTracking<RegisteredFunction> result;
    result.reserve(registry.size());
    for (const auto & [sql_name, entry] : registry)
        result.push_back(RegisteredFunction{sql_name, entry.function, entry.create_query});
    return result;
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
        explicit SettingDefinition(std::function<void(std::string_view, Field &)> normalize_and_check_, Field default_value_)
            : default_value(std::move(default_value_)), normalize_and_check(std::move(normalize_and_check_))
        {
            chassert(normalize_and_check);
        }

        Field default_value;
        std::function<void(std::string_view, Field &)> normalize_and_check;
    };

    struct SettingStringFromSet
    {
        SettingDefinition withDefault(String default_value) const
        {
            return SettingDefinition(
                [values_ = this->values](std::string_view name, Field & value) // NOLINT
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
        UnorderedSetWithMemoryTracking<String> values;
    };

    struct SettingBool
    {
        SettingDefinition withDefault(bool default_value) const
        {
            return SettingDefinition(
                [](std::string_view name, Field & value)
                {
                    if (value.getType() == Field::Types::Bool)
                        return;

                    if (value.getType() == Field::Types::UInt64)
                    {
                        UInt64 u = value.safeGet<UInt64>();
                        if (u != 0 && u != 1)
                            throw Exception(
                                ErrorCodes::BAD_ARGUMENTS,
                                "Setting '{}' must be 0/1 or false/true, got {}",
                                name,
                                u);
                        value = Field(static_cast<bool>(u));
                        return;
                    }

                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Setting '{}' must be a boolean, got {}",
                        name,
                        value.getTypeName());
                },
                Field(default_value));
        }
    };

    const UnorderedMapWithMemoryTracking<String, SettingDefinition> settings_def = {
        /// Serialization format for input/output data for ABI what uses serialization
        {"serialization_format", SettingStringFromSet{{"MsgPack", "JSONEachRow", "CSV", "TSV", "TSVRaw", "RowBinary", "Buffers"}}.withDefault("MsgPack")},
        {"webassembly_udf_enable_fuel", SettingBool{}.withDefault(true)},
        /// Whether bbox-disjoint pruning is safe for this function (see IFunctionBase::isSpatialPredicate).
        {"is_spatial_predicate", SettingBool{}.withDefault(false)},
    };

    VectorWithMemoryTracking<String> getAllRegisteredNames() const override
    {
        VectorWithMemoryTracking<String> result;
        result.reserve(settings_def.size());
        for (const auto & [name, _] : settings_def)
            result.push_back(name);
        return result;
    }

    void normalizeAndCheck(const String & name, Field & value) const
    {
        auto it = settings_def.find(name);
        if (it == settings_def.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown setting name: '{}'{}", name, getHintsMessage(name));
        it->second.normalize_and_check(name, value);
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
    WebAssemblyFunctionSettingsConstraits::instance().normalizeAndCheck(name, value);
    settings.emplace(name, std::move(value));
}

Field WebAssemblyFunctionSettings::getValue(const String & name) const
{
    auto it = settings.find(name);
    if (it == settings.end())
        return WebAssemblyFunctionSettingsConstraits::instance().getDefault(name);
    return it->second;
}

bool WebAssemblyFunctionSettings::isFuelEnabled() const
{
    return getValue("webassembly_udf_enable_fuel").safeGet<bool>();
}

WebAssembly::FuelMode WebAssemblyFunctionSettings::getFuelMode() const
{
    return isFuelEnabled() ? WebAssembly::FuelMode::Enabled : WebAssembly::FuelMode::Disabled;
}

}
