#include <Functions/UserDefined/UserDefinedWebAssembly.h>
#include <Formats/ColumnBinaryWire.h>
#include <Functions/UserDefined/UserDefinedWebAssemblyScriptAbi.h>
#include <Functions/UserDefined/UserDefinedWebAssemblyTypeHelpers.h>

#include <ranges>
#include <algorithm>
#include <base/hex.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeVariant.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeString.h>
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
using namespace ColumnBinaryWire;

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

        if (size > 0 && ptr == 0)
            throw Exception(ErrorCodes::WASM_ERROR,
                "WebAssembly buffer returned null data pointer with size {}", size);

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
        serialization_format = settings.getValue("serialization_format").safeGet<String>();
        Block input_header;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            String col_name = !argument_names[i].empty() ? argument_names[i] : fmt::format("arg{}", i);
            input_header.insert(ColumnWithTypeAndName(arguments[i], col_name));
        }
        // Validate the argument and result types eagerly, at declaration time, instead of
        // deferring to the first call. For `ColumnBinary` this is the same check its output
        // format runs in its constructor, done directly rather than by building that format:
        // building it would also demand `allow_experimental_column_binary_format`, and whether
        // the experimental wire may be used belongs to the query that calls the function, not
        // to the statement that declares it. Every other format is probed by construction,
        // which is also what rejects a serialization format that does not exist.
        if (serialization_format == "ColumnBinary")
        {
            for (const auto & column : input_header)
                validateColumnBinaryWireSupportedType(column.type);
            validateColumnBinaryWireSupportedType(result_type);
        }
        else
        {
            probe_format = FormatFactory::instance().getOutputFormatWithDefaultSettings(
                serialization_format, probe_null_wb, input_header);
        }
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

    /// Reads the whole result out of `input_format` by driving it directly, without building a
    /// `QueryPipeline` and a `PullingPipelineExecutor` around it. `FormatFactory::getInput` returns a
    /// single `IInputFormat` source with no transforms attached, so a pipeline would add nothing here
    /// beyond its own construction cost, which is substantial relative to deserializing one small
    /// in-memory frame: it dominated the WASM read-back path in profiles. `IInputFormat::generate`
    /// yields an empty chunk at end of input, exactly as `ISource::tryGenerate` (and therefore
    /// `ISource::work`) interprets it, so this loop reproduces the source's own driving logic,
    /// including the trailing `onFinish`. No input format overrides `tryGenerate`, so nothing else
    /// can be interposed between `work` and `generate`.
    static void readSingleBlock(IInputFormat & input_format, Block & result_block)
    {
        Chunk result_chunk;
        while (true)
        {
            Chunk chunk = input_format.generate();
            bool has_data = static_cast<bool>(chunk);

            if (chunk && chunk.getNumColumns() != result_block.columns())
                throw Exception(
                    ErrorCodes::WASM_ERROR,
                    "Different number of columns in result chunks, expected {}, got {}",
                    result_block.dumpStructure(),
                    chunk.dumpStructure());

            if (!result_chunk)
                result_chunk = std::move(chunk);
            else if (chunk)
            {
                // `Chunk::append` concatenates with `insertRangeFrom`, which is not const-safe, and
                // `ColumnBinary` preserves top-level const, so a multi-frame result can legitimately
                // contain const chunks. A const destination would only grow its row count and repeat
                // the first frame's value for every later frame; a const source would reach
                // `insertRangeFrom`'s `assert_cast`, which is a plain `static_cast` in release
                // builds. Materialize both sides before concatenating. The single-chunk case above
                // is untouched, so a result that is const end to end still stays const.
                convertToFullIfConst(result_chunk);
                convertToFullIfConst(chunk);
                result_chunk.append(chunk);
            }

            if (!has_data)
                break;
        }

        input_format.onFinish();

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

        if (num_rows == 0)
            return result_type->createColumn();
        if (num_rows >= std::numeric_limits<WasmSizeT>::max())
            throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large number of rows: {}", num_rows);

        auto wmm = std::make_unique<WasmMemoryManagerV01>(compartment, stop_token);

        // Build the format settings and the empty sample header once per call. `getFormatSettings`
        // reads several hundred settings and allocates for every string-valued one, and it used to
        // run three times per invocation (probe, real output format, input format), with
        // `block.cloneEmpty()` running twice on top of that. They are query-invariant, so hoisting
        // them changes nothing about which settings apply while removing the repeated work.
        const FormatSettings format_settings = getFormatSettings(context);
        const Block empty_header = block.cloneEmpty();

        WasmMemoryGuard wasm_data = nullptr;
        if (!block.empty())
        {
            ProfileEventTimeIncrement<Microseconds> timer_serialize(ProfileEvents::WasmSerializationMicroseconds);

            // Build the probe from the query's actual Context rather than reusing probe_format
            // (built once at construction with default FormatSettings, kept only for its early
            // validation side effect): otherwise this precompute/allocate fast path would
            // ignore per-query settings like column_binary_disable_preallocation while the real
            // `out` format below picks them up from context, and the two could disagree on
            // whether or how to serialize. A local NullWriteBuffer (not the probe_null_wb
            // member) avoids a data race if this const method is called concurrently for the
            // same instance.
            NullWriteBuffer local_probe_wb;
            auto probe = context->getOutputFormat(serialization_format, local_probe_wb, empty_header, format_settings);
            std::optional<uint64_t> precomputed = probe->precomputeSerializedSize(block, num_rows);

            if (precomputed)
            {
                wasm_data = allocateInWasmMemory(wmm.get(), *precomputed);
                auto wasm_mem = wasm_data.getMemoryView();
                // Same defensive check as the fallback branch below: a buggy clickhouse_create_buffer
                // implementation in the WASM module could return a handle to a smaller buffer than
                // requested. Without this check, WriteBufferFromPointer below would be constructed
                // with the *requested* size (*precomputed) rather than the actual buffer size, and
                // out->write(block) could write past the end of the real guest buffer.
                if (wasm_mem.size() != *precomputed)
                    throw Exception(ErrorCodes::WASM_ERROR,
                        "Cannot allocate WASM buffer of size {}, got {}. "
                        "Maybe '{}' function implementation in WebAssembly module is incorrect",
                        *precomputed, wasm_mem.size(), WasmMemoryManagerV01::allocate_function_name);
                WriteBufferFromPointer wb(reinterpret_cast<char *>(wasm_mem.data()), *precomputed);
                auto out = context->getOutputFormat(serialization_format, wb, empty_header, format_settings);
                // write()+finalize() instead of formatBlock(): formatBlock calls flush()
                // which triggers out.next() — fatal for WriteBufferFromPointer.
                // auto_flush defaults to false so neither write() nor finalize() flush.
                out->write(block);
                out->finalize();
                wb.cancel();
            }
            else
            {
                // Fallback: serialize into a CH-side String, then copy into WASM memory.
                // WriteBufferForWasmMemory (zero-copy path) cannot be used here because it
                // invokes clickhouse_create_buffer in the WASM compartment during construction,
                // which crashes during constant-folding dry-run (executeImplDryRun).
                StringWithMemoryTracking input_data;
                {
                    WriteBufferFromStringWithMemoryTracking buf(input_data);
                    auto out = context->getOutputFormat(serialization_format, buf, empty_header, format_settings);
                    formatBlock(out, block);
                }
                wasm_data = allocateInWasmMemory(wmm.get(), input_data.size());
                auto wasm_mem = wasm_data.getMemoryView();
                if (wasm_mem.size() != input_data.size())
                    throw Exception(ErrorCodes::WASM_ERROR,
                        "Cannot allocate WASM buffer of size {}, got {}",
                        input_data.size(), wasm_mem.size());
                std::copy(input_data.data(), input_data.data() + input_data.size(), wasm_mem.begin());
            }
        }

        auto result_ptr = compartment->invoke<WasmPtr>(function_name, {wasm_data.getHandle(), static_cast<WasmSizeT>(num_rows)}, stop_token);
        if (result_ptr == 0)
            throw Exception(ErrorCodes::WASM_ERROR, "WebAssembly function '{}' returned nullptr", function_name);

        WasmMemoryGuard result(wmm.get(), result_ptr);
        auto result_data = result.getMemoryView();
        ReadBufferFromMemory inbuf(result_data.data(), result_data.size());

        ProfileEventTimeIncrement<Microseconds> timer_deserialize(ProfileEvents::WasmDeserializationMicroseconds);

        Block result_header({ColumnWithTypeAndName(result_type->createColumn(), result_type, "result")});

        auto input_format = context->getInputFormat(
            serialization_format, inbuf, result_header, /* max_block_size */ DBMS_DEFAULT_BUFFER_SIZE,
            format_settings);
        readSingleBlock(*input_format, result_header);

        if (result_header.columns() != 1 || result_header.rows() != num_rows)
            throw Exception(
                ErrorCodes::WASM_ERROR,
                "Unexpected result column structure: {} returned from WebAssembly function '{}'",
                result_header.dumpStructure(),
                function_name);

        auto result_columns = result_header.mutateColumns();
        return std::move(result_columns[0]);
    }

private:
    String serialization_format;
    NullWriteBuffer probe_null_wb;
    OutputFormatPtr probe_format;
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

static bool computePreserveConstColumns(const ContextPtr & context, const std::shared_ptr<UserDefinedWebAssemblyFunction> & udf)
{
    const String fmt = udf->getSettings().getValue("serialization_format").safeGet<String>();
    StringWithMemoryTracking dummy_buf;
    WriteBufferFromStringWithMemoryTracking dummy_writer(dummy_buf);
    Block sample_block;
    size_t arg_idx = 0;
    for (const auto & arg : udf->getArguments())
        sample_block.insert(ColumnWithTypeAndName(arg->createColumn(), arg, "arg" + std::to_string(arg_idx++)));
    auto format = context->getOutputFormat(fmt, dummy_writer, sample_block);
    return !format->expectMaterializedColumns() || format->supportsColumnSchema();
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
        , preserve_const_columns(computePreserveConstColumns(context, user_defined_function))
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

    /// The exact number of bytes one call carrying `[start, start + length)` puts on the wire.
    ///
    /// The batch is measured whole rather than assembled out of per-row measurements. A row has
    /// no cost of its own under a block-scoped wire: `ColumnBinary` writes a frame header, a
    /// descriptor per column and one `COL_LOWCARD` dictionary per batch, and `BuffersWriter`
    /// runs `NativeWriter::writeData` once per block, which emits a fresh `LowCardinality`
    /// dictionary and the `Dynamic` / `Variant` structure prefixes for whatever rows the block
    /// holds. Summing one-row probes charges every row a whole frame and a whole dictionary,
    /// which over-prices such a batch by more than an order of magnitude, and no fixed per-write
    /// subtraction can remove state whose size depends on which rows the batch carries.
    ///
    /// What comes back here is the stream the guest is really handed - framing, wrapping and
    /// shared state included - so the budget below is compared against the actual size rather
    /// than against a bound on it.
    size_t measureBatchBytes(
        const ColumnsWithTypeAndName & arguments,
        size_t start_idx,
        size_t length,
        const std::vector<size_t> & declared_positions = {}) const
    {
        auto block = getArgumentsBlock(arguments, start_idx, length, declared_positions);
        NullWriteBuffer measure_buf;
        auto measure_out
            = context->getOutputFormat(serialization_format, measure_buf, block.cloneEmpty());

        /// `ColumnBinary` states the size of a block without writing it. This is the very
        /// primitive `executeOnBlock` sizes the guest buffer with, so the measurement and the
        /// allocation cannot disagree, and it is exact for the whole block being measured.
        if (auto precomputed = measure_out->precomputeSerializedSize(block, length))
            return *precomputed;

        measure_out->write(block);
        measure_out->finalize();
        return measure_buf.count();
    }

    /// The bytes a batch pays whatever its row count.
    ///
    /// A `ColumnConst` argument is one stored row broadcast to the batch, and a wire that carries
    /// constness writes that row once, so the argument costs the same at one row as at a whole
    /// block. Measuring the const arguments on their own prices exactly that part - and prices a
    /// merely wide first row, which is a row like any other and does shrink out of a batch, at
    /// nothing. On a wire that does not carry constness `getArgumentsBlock` materializes the
    /// argument, and what comes back is the cost of its one row, which is the truth there.
    size_t measureConstArgumentBytes(const ColumnsWithTypeAndName & arguments, size_t start_idx) const
    {
        ColumnsWithTypeAndName const_arguments;
        std::vector<size_t> declared_positions;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (arguments[i].column && isColumnConst(*arguments[i].column))
            {
                const_arguments.push_back(arguments[i]);
                declared_positions.push_back(i);
            }
        }

        if (const_arguments.empty())
            return 0;
        return measureBatchBytes(const_arguments, start_idx, 1, declared_positions);
    }

    /// How many rows the call starting at `start_idx` should carry, out of `remaining`.
    ///
    /// The cost of a batch is monotone in its row count - adding a row can only grow the payload,
    /// and can only grow a per-batch dictionary - so "the rows that fit the budget" is a prefix and
    /// can be bracketed. Each probe measures a candidate exactly, keeps the largest candidate known
    /// to fit and the smallest known to overflow, and picks the next candidate inside that bracket,
    /// so the bracket shrinks on every step and the walk ends on the real boundary rather than on
    /// the first prefix that looked full enough.
    ///
    /// The next candidate follows the marginal cost of a row, taken as the slope between the last
    /// two measurements, not the average bytes per row of the candidate. The average carries the
    /// batch-wide part of the payload - framing, a `LowCardinality` dictionary, `Dynamic` and
    /// `Variant` structure prefixes, and any single wide row already in the prefix - which is paid
    /// once and does not grow with the rows added next. Dividing by it prices every further row at
    /// the cost of the whole prefix, so a block whose first row is far wider than the rest would be
    /// handed to the guest one row per call while hundreds of its rows still fit.
    ///
    /// Every candidate is bounded by rows already measured: the walk starts at one row and grows by
    /// a bounded factor per probe. Nothing is carried over from a previous batch or block, because a
    /// row count only means something for rows of a known width - a count fitted by narrow rows
    /// would have the next batch materialize that many wide rows before any measurement justified
    /// it, recreating the oversized call the split exists to avoid.
    size_t chooseBatchRows(
        const ColumnsWithTypeAndName & arguments, size_t start_idx, size_t remaining, size_t budget) const
    {
        /// A function without arguments is handed no input buffer, so no size bounds its calls.
        if (arguments.empty())
            return remaining;

        static constexpr size_t max_probes = 16;
        /// A probe may only ask for this many times the rows the previous probe measured. The
        /// extrapolated count is read off a prefix, and a prefix of narrow rows says nothing about
        /// wider rows later in the block, so growth is paid for by rows already materialized.
        /// Reaching any batch size still costs a logarithmic number of probes.
        static constexpr size_t max_growth_per_probe = 4;

        /// Probe upwards from a single row, rather than downwards from the whole block. A probe
        /// serializes the candidate, and for a wire that does not carry constness a `ColumnConst`
        /// argument is materialized to do it, so a first probe of the whole block would expand
        /// exactly the input the splitting exists to rescue.
        size_t candidate = 1;
        size_t largest_fitting = 0;
        size_t smallest_overflowing = remaining + 1;

        /// The previous measurement, so the next candidate can be read off a slope. There is no
        /// previous measurement while `previous_rows` is zero.
        size_t previous_rows = 0;
        size_t previous_bytes = 0;

        for (size_t probe = 0; probe < max_probes; ++probe)
        {
            const size_t measured = measureBatchBytes(arguments, start_idx, candidate);
            if (measured <= budget)
            {
                largest_fitting = candidate;
                if (candidate == remaining)
                    break;
            }
            else
            {
                smallest_overflowing = candidate;
                if (candidate == 1)
                {
                    /// When what does not fit is the part of the batch that no row count changes,
                    /// no row count brings the call inside the budget: splitting then re-pays the
                    /// same bytes once per call and takes from the guest whatever it amortizes
                    /// across a call. Hand it the whole block instead - exceeding the budget once
                    /// beats exceeding it on every call of a one-row split.
                    if (measureConstArgumentBytes(arguments, start_idx) >= budget)
                        return remaining;
                    /// Otherwise it is the row itself that does not fit, and it is still passed on
                    /// its own: whether the guest can hold it is for its allocator to say.
                    break;
                }
            }

            /// The boundary is known exactly once the bracket has nothing left between its ends.
            if (largest_fitting + 1 >= smallest_overflowing)
                break;

            /// An empty payload gives no slope to follow, so nothing bounds the batch but the block.
            if (measured == 0)
            {
                candidate = remaining;
                continue;
            }

            /// The marginal bytes a row adds. With one measurement in hand the average is all there
            /// is; it over-states the marginal cost, so the step it proposes is an undershoot, and
            /// the clamp below still moves the walk on by a row, which buys the second measurement
            /// the slope needs.
            Float64 bytes_per_row = static_cast<Float64>(measured) / static_cast<Float64>(candidate);
            if (previous_rows != 0 && candidate != previous_rows)
            {
                const Float64 slope = (static_cast<Float64>(measured) - static_cast<Float64>(previous_bytes))
                    / (static_cast<Float64>(candidate) - static_cast<Float64>(previous_rows));
                if (slope > 0.0)
                    bytes_per_row = slope;
            }
            previous_rows = candidate;
            previous_bytes = measured;

            const Float64 target = static_cast<Float64>(candidate)
                + (static_cast<Float64>(budget) - static_cast<Float64>(measured)) / bytes_per_row;

            size_t next = 1;
            if (target >= static_cast<Float64>(remaining))
                next = remaining;
            else if (target > 1.0)
                next = static_cast<size_t>(target);

            if (next > candidate)
                next = std::min(next, candidate * max_growth_per_probe);
            /// The bracket both keeps the candidate meaningful and guarantees progress: a candidate
            /// that fits raises the lower end past itself, one that overflows lowers the upper end
            /// below itself, and the check above leaves at least one row between the ends.
            candidate = std::clamp(next, largest_fitting + 1, smallest_overflowing - 1);
        }

        return std::max<size_t>(largest_fitting, 1);
    }

    void appendBatchResult(MutableColumnPtr & result_column, MutableColumnPtr batch_column) const
    {
        /// Under a const-preserving wire a guest may legitimately return `COL_IS_CONST`, which
        /// `ColumnBinaryInputFormat` decodes as a `ColumnConst`. `structureEquals` only holds
        /// between two `ColumnConst`s, so compare the unwrapped nested column rather than
        /// rejecting every valid const result.
        const IColumn * batch_for_check = batch_column.get();
        if (const auto * batch_const = typeid_cast<const ColumnConst *>(batch_for_check))
            batch_for_check = &batch_const->getDataColumn();
        if (!result_column->structureEquals(*batch_for_check))
            throw Exception(
                ErrorCodes::WASM_ERROR,
                "Different column types in result blocks: {} and {}",
                result_column->dumpStructure(),
                batch_column->dumpStructure());

        /// A `ColumnConst` batch result must be materialized before it is accumulated:
        /// `ColumnConst::insertRangeFrom` only bumps the row count without copying the source's
        /// value, so a const accumulator would keep repeating the first batch's value for every
        /// row appended afterwards.
        batch_column = IColumn::mutate(batch_column->convertToFullColumnIfConst());
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
            /// Take the rows a call can hold, measure the call, and start the next one where
            /// it ended. A stride derived from an average row size cannot bound a skewed block:
            /// one huge row among many tiny ones would still share a call with its neighbours.
            while (batch_start < input_rows_count)
                flush_batch(batch_start + chooseBatchRows(arguments, batch_start, input_rows_count - batch_start, *budget));
        }
        else if (fixed_block_size > 0)
        {
            for (size_t row = fixed_block_size; row < input_rows_count; row += fixed_block_size)
                flush_batch(row);
        }

        flush_batch(input_rows_count);
        return result_column;
    }

    /// `declared_positions` says where each entry of `arguments` sits in the function's declared
    /// argument list. It is empty when `arguments` is that list in order, and is given only when a
    /// caller passes a subset of the arguments - the declared type and name of an argument are
    /// looked up by its declared position, not by where it happens to land in `arguments`.
    Block getArgumentsBlock(
        const ColumnsWithTypeAndName & arguments,
        size_t start_idx,
        size_t length,
        const std::vector<size_t> & declared_positions = {}) const
    {
        const auto & declared_arguments = user_defined_function->getArguments();
        Block arguments_block;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const size_t declared_idx = declared_positions.empty() ? i : declared_positions[i];
            /// Cut first, materialize second: `ColumnConst::cut` is O(1), while materializing
            /// the whole block first would make the per-row measurement O(rows^2). A wire that
            /// encodes constness itself keeps the wrapper instead of materializing at all.
            /// Skip the copy when the requested range already covers the whole column -
            /// the whole-block flush does exactly that for every argument.
            ColumnPtr column = arguments[i].column;
            if (start_idx != 0 || length != column->size())
                column = column->cut(start_idx, length);
            if (!preserve_const_columns)
                column = column->convertToFullColumnIfConst();
            String column_name = declared_idx < argument_names.size() && !argument_names[declared_idx].empty()
                ? argument_names[declared_idx]
                : arguments[i].name;
            /// Cast to the declared type so serialization uses the correct width.
            /// Without this, e.g. Int8 passed to an Int32 parameter would be serialized
            /// as 1 byte by RowBinary instead of 4, causing the WASM module to read garbage.
            /// `ColumnBinary`'s descriptor only encodes a coarse width class (`COL_FIXED8/16/32/64`),
            /// not exact signedness - a `UInt8(255)` and an `Int8(-1)` both serialize to the same
            /// single `0xff` byte, so a guest reading a declared `Int32` has no way to tell them
            /// apart. Always cast here regardless of format until the wire carries real logical
            /// type and signedness information.
            const DataTypePtr & declared_type = declared_arguments[declared_idx];
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
    /// Whether the configured wire keeps a top-level `ColumnConst` compact instead of
    /// materializing it - `ColumnBinary`'s `COL_IS_CONST`. Driven off the format's own
    /// capabilities rather than its name: `Buffers` exposes a native serialization but
    /// `NativeWriter::writeData` calls `convertToFullColumnIfConst` before writing, so it is
    /// not const-preserving.
    bool preserve_const_columns;

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
        {"serialization_format", SettingStringFromSet{{"MsgPack", "JSONEachRow", "CSV", "TSV", "TSVRaw", "RowBinary", "Buffers", "ColumnBinary"}}.withDefault("MsgPack")},
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
