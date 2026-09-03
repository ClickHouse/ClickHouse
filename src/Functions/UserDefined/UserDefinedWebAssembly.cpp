#include <Functions/UserDefined/UserDefinedWebAssembly.h>
#include <Formats/ColumnBinaryWire.h>
#include <Functions/UserDefined/UserDefinedWebAssemblyScriptAbi.h>
#include <Functions/UserDefined/UserDefinedWebAssemblyTypeHelpers.h>

#include <ranges>
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

/// The user-facing `ColumnBinary` format is gated behind
/// `allow_experimental_column_binary_format` because its `ColumnBinary` frame header carries no
/// wire version yet. The `ColumnBinary` WASM UDF ABI shares that wire format but not the gate:
/// WASM UDFs are experimental in their own right, and their frames never outlive a single call,
/// so no persisted data can be misparsed by a future layout change. Start from the query's own
/// format settings so per-query knobs (e.g. `column_binary_disable_preallocation`) still apply.
static FormatSettings wasmFormatSettings(const ContextPtr & context)
{
    auto format_settings = getFormatSettings(context);
    format_settings.column_binary.allow_experimental = true;
    return format_settings;
}

/// Same, for the construction-time probe format, which has no Context to read settings from.
static FormatSettings columnBinaryEnabledFormatSettings()
{
    FormatSettings format_settings;
    format_settings.column_binary.allow_experimental = true;
    return format_settings;
}

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
        // Built once, with default FormatSettings, purely for its constructor's side effect:
        // it validates argument types eagerly when serialization_format is ColumnBinary (see
        // ColumnBinaryOutputFormat's constructor) instead of deferring to the first call.
        // executeOnBlock below builds its own format from the query's actual Context for the
        // real precompute/serialize work, since this one's default settings would silently
        // diverge from whatever the query actually configured.
        probe_format = FormatFactory::instance().getOutputFormatWithDefaultSettings(
            serialization_format, probe_null_wb, input_header, columnBinaryEnabledFormatSettings());
        // The result type is only read back lazily on the first call, so validate it eagerly
        // here too.
        if (serialization_format == "ColumnBinary")
            validateColumnBinaryWireSupportedType(result_type);
    }

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
        const FormatSettings format_settings = wasmFormatSettings(context);
        const Block empty_header = block.cloneEmpty();

        WasmMemoryGuard wasm_data = nullptr;
        if (!block.empty())
        {
            ProfileEventTimeIncrement<Microseconds> timer_serialize(ProfileEvents::WasmSerializationMicroseconds);

            // Build a fresh probe from the query's actual Context here rather than reusing
            // probe_format (built once at construction with FormatFactory's default
            // FormatSettings, kept only for its early argument-type-validation side effect):
            // otherwise this precompute/allocate fast path silently ignores per-query settings
            // like column_binary_disable_preallocation while the real `out` format below
            // correctly picks them up from context, so the two could disagree on whether/how
            // to serialize. A local NullWriteBuffer (not the probe_null_wb member) avoids a
            // data race if this const method is called concurrently for the same instance.
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
    auto format = context->getOutputFormat(fmt, dummy_writer, sample_block, wasmFormatSettings(context));
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
        buffered_serialization_format = user_defined_function->getSettings().getValue("serialization_format").safeGet<String>();
        wire_size_expansion_factor = wireSizeExpansionFactor(buffered_serialization_format);
        // Only the text formats render an Enum by name; the binary wires write the numeric
        // value the estimators already model exactly, so leave their factor at 1.
        if (wire_size_expansion_factor > 1)
        {
            for (const auto & declared_argument : user_defined_function->getArguments())
                wire_size_expansion_factor = std::max(wire_size_expansion_factor, enumTextWireExpansion(declared_argument));
        }
        // Only the native-wire formats keep LowCardinality dictionary-encoded
        // (dictionary + compact indexes); RowBinary, MsgPack and the text formats
        // materialize the resolved value on every row.
        wire_encodes_low_cardinality
            = buffered_serialization_format == "ColumnBinary" || buffered_serialization_format == "Buffers";
    }

    /// The dynamic splitter prices batches with estimateTotalSerializedSize /
    /// estimateRowSerializedSize below, which size values by their in-memory column
    /// width. For text-like serialization formats a value's wire size can exceed that
    /// width (CSV/TSV decimal rendering and quote doubling, JSON escaping, MsgPack's
    /// per-scalar type byte), so the raw estimate must be scaled by a worst-case
    /// expansion factor or the splitter can miss a needed split and the call would fail
    /// later at guest-buffer allocation time even though splitting could have made it
    /// succeed.
    static size_t wireSizeExpansionFactor(const String & format)
    {
        /// These formats' wire size is bounded by the column metadata the estimators
        /// already model: fixed widths are exact, and variable-length headers (RowBinary
        /// varints, up to 9 bytes) never exceed the modelled uint64 offset entries.
        if (format == "ColumnBinary" || format == "Buffers" || format == "RowBinary")
            return 1;
        /// MsgPack adds at most one type byte per fixed-width scalar (w + 1 <= 2w for
        /// w >= 1); string headers (<= 5 bytes) are covered by the modelled 8-byte
        /// offset entries.
        if (format == "MsgPack")
            return 2;
        /// Text formats (CSV, TSV, TSVRaw, JSONEachRow): decimal renderings of
        /// fixed-width values (a 2-byte Date prints as 10 characters, a 4-byte DateTime
        /// as 19, an Int8 as up to 4), CSV/TSV quoting and escape doubling (<= 2x), and
        /// JSON string escaping (<= 6 output bytes per input byte, plus quotes) are all
        /// bounded by 8x the in-memory width.
        return 8;
    }

    /// Fixed per-row structural bytes the buffered wire adds regardless of the values:
    /// field delimiters and the row terminator for CSV/TSV, and per-row object keys for
    /// JSONEachRow, which repeats every column name on every row — an auto-generated
    /// argument name from a complex expression is a real per-row cost no value-based
    /// estimate can see. Enum name rendering is not charged here: it scales with the
    /// number of enum values in the row (one per `Array(Enum8)` element), which is a
    /// property of the data rather than a fixed per-row constant, so it is handled
    /// multiplicatively by enumTextWireExpansion below.
    size_t perRowWireOverhead(const ColumnsWithTypeAndName & arguments) const
    {
        const String & format = buffered_serialization_format;
        const bool is_json = format == "JSONEachRow";
        const bool is_text = is_json || format == "CSV" || format == "TSV" || format == "TSVRaw";
        if (!is_text)
            return 0;
        size_t overhead = is_json ? 4 : arguments.size() + 2;
        if (!is_json)
            return overhead;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            /// Same name-selection rule as getArgumentsBlock; 6x covers worst-case
            /// JSON escaping of the key, +8 covers quotes, colon and comma.
            const String & name = i < argument_names.size() && !argument_names[i].empty() ? argument_names[i] : arguments[i].name;
            overhead += 6 * name.size() + 8;
        }
        return overhead;
    }

    /// Worst-case text-wire byte cost of a single value of `type`, an `Enum8` / `Enum16`:
    /// the longest label it can render, escaped. Only characters that any of the text
    /// formats can expand are charged the full 6 bytes (JSON's `\uXXXX`, which also bounds
    /// CSV/TSV quote doubling and backslash escapes); ordinary label characters — what enum
    /// labels are made of in practice — cost one byte each, so this stays tight instead of
    /// blanket-multiplying every label by 6. The +3 covers the surrounding quotes and the
    /// field separator.
    template <typename EnumType>
    static size_t maxEnumTextWireBytes(const EnumType & type)
    {
        size_t max_size = 0;
        for (const auto & value : type.getValues())
        {
            size_t escaped = 0;
            for (char c : value.first)
                escaped += (c == '"' || c == '\\' || static_cast<unsigned char>(c) < 0x20) ? 6 : 1;
            max_size = std::max(max_size, escaped);
        }
        return max_size + 3;
    }

    /// Text formats serialize an `Enum` by its *name*, but every estimator below prices an
    /// enum value by its numeric storage width (1 byte for `Enum8`, 2 for `Enum16`) — the
    /// columns carry no type information, so a value-based estimate cannot see the label.
    /// A bare top-level `Enum` could be special-cased on the declared type, but wrapped and
    /// nested shapes cannot: `Nullable(Enum8)` goes through the fixed-width path,
    /// `LowCardinality(Enum8)` is priced over the underlying `Int8` dictionary values, and
    /// `Array(Enum8)` / `Tuple(..., Enum8)` fall through to estimateComplexRowBytes's
    /// valuesHaveFixedSize branch. A label longer than that width is then under-budgeted, so
    /// the splitter can skip a split it needed and the call fails late in guest allocation
    /// instead of succeeding on smaller batches.
    ///
    /// Correcting this additively would need the number of enum values in each row, which
    /// for `Array(Enum8)` is a property of the data, not of the type. Instead return the
    /// worst-case name-to-width ratio of any enum reachable in `type`, and scale the whole
    /// estimate by it: every enum byte the estimators counted is then covered, and the
    /// non-enum bytes are merely over-estimated, which only splits into smaller batches.
    /// Returns 1 (identity) for types containing no enum, so this is inert for every UDF
    /// that does not declare one.
    static size_t enumTextWireExpansion(const DataTypePtr & type)
    {
        if (const auto * enum8 = typeid_cast<const DataTypeEnum8 *>(type.get()))
            return (maxEnumTextWireBytes(*enum8) + sizeof(Int8) - 1) / sizeof(Int8);
        if (const auto * enum16 = typeid_cast<const DataTypeEnum16 *>(type.get()))
            return (maxEnumTextWireBytes(*enum16) + sizeof(Int16) - 1) / sizeof(Int16);
        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
            return enumTextWireExpansion(nullable->getNestedType());
        if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
            return enumTextWireExpansion(low_cardinality->getDictionaryType());
        if (const auto * array = typeid_cast<const DataTypeArray *>(type.get()))
            return enumTextWireExpansion(array->getNestedType());
        if (const auto * map = typeid_cast<const DataTypeMap *>(type.get()))
            return std::max(enumTextWireExpansion(map->getKeyType()), enumTextWireExpansion(map->getValueType()));
        size_t max_expansion = 1;
        if (const auto * tuple = typeid_cast<const DataTypeTuple *>(type.get()))
        {
            for (const auto & element : tuple->getElements())
                max_expansion = std::max(max_expansion, enumTextWireExpansion(element));
        }
        else if (const auto * variant = typeid_cast<const DataTypeVariant *>(type.get()))
        {
            for (const auto & alternative : variant->getVariants())
                max_expansion = std::max(max_expansion, enumTextWireExpansion(alternative));
        }
        return max_expansion;
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

            /// Allow implicit coercions: same kind, i32->i64, any int->any float, f32->f64.
            /// `BUFFERED_V1`'s getArgumentsBlock casts the column down to the declared
            /// (non-nullable) type before serialization, which would silently drop or fail
            /// on real NULL values, so a Nullable argument against a non-nullable declared
            /// parameter is never accepted here.
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

    /// A WASM UDF owns null propagation exactly when it declared that it can produce nulls.
    /// For a `Nullable` return type the framework leaves the arguments alone, so the guest
    /// sees real `ColumnNullable` inputs through `COL_IS_NULLABLE` and decides which rows are
    /// null; for every other return type the framework denulls the arguments and wraps the
    /// result, giving the ordinary SQL propagation every built-in function has.
    ///
    /// Keying this on the declared type's shape instead (`canBeInsideNullable`, as an earlier
    /// version did) made the guest-visible ABI depend on something unrelated to nulls: an
    /// `Array`/`Tuple` return type cannot be inside `Nullable` either, so the same module
    /// declared `RETURNS Array(UInt32)` saw nullable inputs while `RETURNS UInt32` did not.
    /// A return type that cannot be `Nullable` needs no special case here: the framework's
    /// `makeNullableSafe` already returns it as-is and evaluates null rows over the nested
    /// column's default values (see `IFunctionOverloadResolver::getReturnTypeWithoutLowCardinality`).
    bool useDefaultImplementationForNulls() const override
    {
        return !user_defined_function->getResultType()->isNullable();
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
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
    /// Estimate total serialized byte size of argument columns for an entire block.
    /// Used for dynamic block splitting when webassembly_udf_max_input_block_size = 0.
    /// preserve_const must match the same decision getArgumentsBlock/flush_batch will use:
    /// only skip a ColumnConst's contribution when it actually stays const on the wire
    /// (COL_IS_CONST / the Buffers format); for formats that materialize const columns
    /// (MsgPack, RowBinary, CSV, ...) a large constant broadcast batch_size times must
    /// count towards the estimate, or the splitter can miss a needed split.
    /// Runs in O(1) — reads column metadata, no per-row scanning (plus a one-time scan
    /// of a LowCardinality dictionary on wires that materialize its values per row).
    ///
    /// This estimate sizes values by their in-memory column width, which models the
    /// binary wires (ColumnBinary, ColumnBinary, Buffers, RowBinary) directly. For the
    /// text-like formats the callers in execute() scale the result by
    /// wire_size_expansion_factor (worst-case wire expansion of fixed-width values,
    /// quoting/escaping and Enum name rendering — see wireSizeExpansionFactor and
    /// enumTextWireExpansion above) and add perRowWireOverhead
    /// (delimiters, JSONEachRow keys), making the scaled figure an upper
    /// bound for every supported serialization_format. It remains an estimate, not the
    /// enforcement point: the actual allocation later in executeOnBlock
    /// (allocateInWasmMemory) asks the WASM guest's own allocator for the real
    /// serialized size and throws WASM_ERROR cleanly if the guest can't satisfy it
    /// (bounded by webassembly_udf_max_memory), exactly as it always has for oversized
    /// single blocks.
    size_t estimateTotalSerializedSize(const ColumnsWithTypeAndName & arguments, size_t row_count, bool preserve_const) const
    {
        const auto & declared_arguments = user_defined_function->getArguments();
        size_t total = 0;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto & arg = arguments[i];
            // Size by the declared (post-cast) type, not arg.type: getArgumentsBlock casts
            // every argument to declared_arguments[i] before serialization, so e.g. a UInt8
            // argument cast to a declared Int32 is written as 4 bytes/row, not 1.
            const DataTypePtr & declared_type = declared_arguments[i];
            const IColumn * col = arg.column.get();
            bool materialized_const = false;
            if (const auto * const_col = typeid_cast<const ColumnConst *>(col))
            {
                if (preserve_const)
                {
                    // COL_IS_CONST still serializes one full value onto the wire (just not
                    // once per row): a huge constant string/geometry contributes that value's
                    // actual size here, not 0, or a large-const-plus-tiny-varying-args call
                    // could skip the split path and still build an oversized guest buffer.
                    const IColumn * data_col = &const_col->getDataColumn();
                    size_t null_map_bytes = 0;
                    // Unwrap declared_type in lockstep with data_col: isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion()
                    // is false for DataTypeNullable itself even when its nested type is
                    // fixed-width, so sizing by the still-Nullable declared_type would fall
                    // through to the flat 256-byte fallback for every Nullable fixed-width
                    // argument (e.g. Nullable(UInt64)).
                    DataTypePtr unwrapped_declared_type = declared_type;
                    if (const auto * const_null = typeid_cast<const ColumnNullable *>(data_col))
                    {
                        data_col = &const_null->getNestedColumn();
                        unwrapped_declared_type = removeNullable(declared_type);
                        null_map_bytes = 1; // one null byte for the single represented value
                    }
                    if (const auto * const_s = typeid_cast<const ColumnString *>(data_col))
                        total += const_s->getChars().size() + null_map_bytes;
                    else if (unwrapped_declared_type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
                        total += unwrapped_declared_type->getSizeOfValueInMemory() + null_map_bytes;
                    else if (typeid_cast<const ColumnArray *>(data_col) || typeid_cast<const ColumnTuple *>(data_col))
                        total += ColumnBinaryWire::complexDataSize(*data_col, 1) + null_map_bytes;
                    else if (const auto * const_map = typeid_cast<const ColumnMap *>(data_col))
                        total += ColumnBinaryWire::complexDataSize(const_map->getNestedColumn(), 1) + null_map_bytes;
                    else if (const auto * const_lc = typeid_cast<const ColumnLowCardinality *>(data_col))
                        // COL_IS_CONST reuses the normal COL_LOWCARD layout with 1 row (see
                        // "COL_IS_CONST sets data for 1 row" in ColumnBinaryWire.h), so it still
                        // carries the full header + a 1-entry dictionary + a 1-entry index —
                        // not just the resolved value's own size. preserve_const is only ever
                        // true on the dictionary-encoding wires, hence the literal true here.
                        total += estimateLowCardTotalBytes(*const_lc, 1, false, /* wire_has_dictionary */ true) + null_map_bytes;
                    else if (const auto * const_var = typeid_cast<const ColumnVariant *>(data_col))
                        // Same reasoning: COL_IS_CONST reuses the normal COL_VARIANT layout
                        // with 1 row, so the alternatives' header is still present.
                        total += estimateVariantTotalBytes(*const_var, 1, false) + null_map_bytes;
                    else
                        total += 256 + null_map_bytes;
                    continue;
                }
                col = &const_col->getDataColumn();
                materialized_const = true;
            }
            // Declared Nullable(String)/Nullable(Array)/Nullable(Tuple) arguments carry a
            // real ColumnNullable at runtime; unwrap it so the String/Array/Tuple branches
            // below actually match instead of silently falling to the flat 256-byte guess,
            // and add the null map's own byte cost (1 byte/row).
            bool is_col_nullable = false;
            if (const auto * null_col = typeid_cast<const ColumnNullable *>(col))
            {
                col = &null_col->getNestedColumn();
                is_col_nullable = true;
            }
            size_t null_map_bytes = is_col_nullable ? row_count : 0;
            // Unwrap declared_type in lockstep with col: see the matching comment on
            // unwrapped_declared_type in the preserved-const branch above.
            DataTypePtr unwrapped_declared_type = is_col_nullable ? removeNullable(declared_type) : declared_type;
            // row_count == 0 is not just "no data": both call sites above also call this
            // function with row_count = 0 specifically to get the fixed reservation that
            // applies to every batch regardless of its size — the per-batch structural
            // overhead each argument's own wire encoding always carries (a lone sentinel
            // offset entry, a LowCardinality/Variant header), not its real content, which
            // scales with the actual number of rows in a given batch and is covered
            // separately by the per-row loop (estimateRowSerializedSize) and the whole-batch
            // check (row_count = input_rows_count) above. Getting this wrong in either
            // direction is a real bug: charging real content here (proportional to the
            // column's actual size, not row_count) turns ordinary non-const arguments into
            // false "preserved constant arguments alone require ..." exceptions; omitting the
            // structural overhead entirely (e.g. Variant's `4 + K * (4 + 40)` header) lets
            // running_bytes under-count and send an oversized batch unsplit.
            if (const auto * s = typeid_cast<const ColumnString *>(col))
            {
                size_t bytes = row_count == 0 ? 0 : s->getChars().size(); // raw bytes including null terminators
                // Wire offsets[row_count+1] (uint64) accompany every non-const String column,
                // whether it started that way or was just materialized from a ColumnConst.
                size_t offset_bytes = (row_count + 1) * sizeof(uint64_t);
                total += (materialized_const ? bytes * row_count : bytes) + offset_bytes + null_map_bytes;
            }
            else if (unwrapped_declared_type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
                total += unwrapped_declared_type->getSizeOfValueInMemory() * row_count + null_map_bytes;
            else if (typeid_cast<const ColumnArray *>(col) || typeid_cast<const ColumnTuple *>(col))
                // complexDataSize matches the exact COL_COMPLEX byte layout (uint64 offsets +
                // nested payload); a flat 256-byte guess badly undercounts e.g. an Array(UInt64)
                // row with thousands of elements, letting the splitter miss a needed split.
                // At row_count == 0, complexDataSize's nested recursion still reads the real
                // (non-row-scaled) element count, so charge just the outer sentinel offset
                // entry instead of calling into it.
                total += (row_count == 0 ? sizeof(uint64_t) : (materialized_const
                    ? ColumnBinaryWire::complexDataSize(*col, 1) * row_count
                    : ColumnBinaryWire::complexDataSize(*col, static_cast<uint32_t>(row_count)))) + null_map_bytes;
            else if (const auto * map_col = typeid_cast<const ColumnMap *>(col))
                total += (row_count == 0 ? sizeof(uint64_t) : (materialized_const
                    ? ColumnBinaryWire::complexDataSize(map_col->getNestedColumn(), 1) * row_count
                    : ColumnBinaryWire::complexDataSize(map_col->getNestedColumn(), static_cast<uint32_t>(row_count)))) + null_map_bytes;
            else if (const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(col))
                total += estimateLowCardTotalBytes(*lc_col, row_count, materialized_const, wire_encodes_low_cardinality) + null_map_bytes;
            else if (const auto * var_col = typeid_cast<const ColumnVariant *>(col))
                total += estimateVariantTotalBytes(*var_col, row_count, materialized_const) + null_map_bytes;
            else
                total += 256 * row_count + null_map_bytes; // conservative fallback
        }
        return total + perBatchWireOverhead(arguments);
    }

    /// Fixed per-batch structural bytes the block-based wires add on top of the column payload,
    /// independent of the row count. The per-column estimates above do not model them at all,
    /// so omitting them lets the splitter skip a needed split on a wide block and then fail
    /// allocating a batch larger than `input_budget`. The remaining supported formats are
    /// row-oriented and carry no such per-batch block.
    size_t perBatchWireOverhead(const ColumnsWithTypeAndName & arguments) const
    {
        const String & format = buffered_serialization_format;
        /// A frame header plus one descriptor per column precede any data; see
        /// ColumnBinaryOutputFormat::precomputeSerializedSize. A 1000-column, 1-row `UInt8`
        /// block is about 1000 bytes of payload but over 40 KB on the wire.
        if (format == "ColumnBinary")
            return ColumnBinaryWire::FRAME_HEADER_BYTES + arguments.size() * ColumnBinaryWire::COL_DESC_BYTES;
        /// BuffersWriter::write emits two uint64 block fields (column count, row count) and
        /// then one uint64 byte-size prefix per column before that column's payload.
        if (format == "Buffers")
            return 2 * sizeof(uint64_t) + arguments.size() * sizeof(uint64_t);
        return 0;
    }

    /// Recursively estimate the serialized byte size of a single row of a COL_COMPLEX-shaped
    /// column (Array/Tuple, possibly nested). Mirrors ColumnBinaryWire::complexDataSize's byte
    /// layout (uint64 offset entry per Array level, nested payload) but for one row instead
    /// of the whole column, so a single oversized row (e.g. one 10k-element Array(UInt64))
    /// is priced precisely instead of falling back to a flat 256-byte guess.
    static size_t estimateComplexRowBytes(const IColumn & col, size_t row_index)
    {
        if (const auto * null_col = typeid_cast<const ColumnNullable *>(&col))
            // Mirrors complexDataSize's nested-Nullable layout (u8 null_map[n] prepended, then
            // the nested column's own complexData layout) but for one row: 1 null byte plus the
            // nested column's per-row cost, regardless of whether this particular row is null
            // (the nested column still has a real, if default-valued, entry at that row).
            return 1 + estimateComplexRowBytes(null_col->getNestedColumn(), row_index);
        if (const auto * map_col = typeid_cast<const ColumnMap *>(&col))
            return estimateComplexRowBytes(map_col->getNestedColumn(), row_index);
        if (const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(&col))
            // Nested LowCardinality still materializes to its resolved dictionary value on
            // the wire (unlike top-level, which is directly encoded), so its per-row cost is
            // that value's own size, not the compact index width.
            return estimateComplexRowBytes(*lc_col->getDictionary().getNestedColumn(), lc_col->getIndexes().getUInt(row_index));
        if (const auto * s = typeid_cast<const ColumnString *>(&col))
            return s->getOffsets()[row_index] - (row_index > 0 ? s->getOffsets()[row_index - 1] : 0) + sizeof(uint64_t);
        if (const auto * arr = typeid_cast<const ColumnArray *>(&col))
        {
            const auto & offs = arr->getOffsets();
            size_t start = row_index > 0 ? offs[row_index - 1] : 0;
            size_t end = offs[row_index];
            size_t total = sizeof(uint64_t); // this row's own offset entry
            const IColumn & nested = arr->getData();
            for (size_t j = start; j < end; ++j)
                total += estimateComplexRowBytes(nested, j);
            return total;
        }
        if (const auto * tup = typeid_cast<const ColumnTuple *>(&col))
        {
            size_t total = 0;
            for (const auto & field : tup->getColumns())
                total += estimateComplexRowBytes(*field, row_index);
            return total;
        }
        if (col.valuesHaveFixedSize())
            return col.sizeOfValueIfFixed();
        return 256; // conservative fallback
    }

    /// O(1) aggregate byte size of a whole Variant alternative sub-column (used to size that
    /// alternative's total contribution in estimateVariantTotalBytes below without a per-row
    /// scan). Array(...)/Tuple(...)/Map(...)/top-level LowCardinality(...) are all valid
    /// Variant alternatives (accepted by both the validator and buildColDescriptor), so this
    /// must mirror their real COL_COMPLEX/COL_LOWCARD wire cost, not the flat 256-byte guess —
    /// otherwise a Variant carrying e.g. one oversized Array alternative row estimates as a few
    /// hundred bytes and never triggers the row-wise split path.
    static size_t estimateAggregateColumnBytes(const IColumn & col)
    {
        if (const auto * null_col = typeid_cast<const ColumnNullable *>(&col))
            // Mirrors complexDataSize's nested-Nullable layout: 1 null byte per row plus the
            // nested column's own aggregate cost. Needed because a LowCardinality(Nullable(T))
            // dictionary is exactly this shape (see estimateLowCardTotalBytes below).
            return col.size() + estimateAggregateColumnBytes(null_col->getNestedColumn());
        if (const auto * s = typeid_cast<const ColumnString *>(&col))
            return s->getChars().size() + (col.size() + 1) * sizeof(uint64_t);
        if (const auto * map_col = typeid_cast<const ColumnMap *>(&col))
            return ColumnBinaryWire::complexDataSize(map_col->getNestedColumn(), static_cast<uint32_t>(col.size()));
        if (typeid_cast<const ColumnArray *>(&col) || typeid_cast<const ColumnTuple *>(&col))
            return ColumnBinaryWire::complexDataSize(col, static_cast<uint32_t>(col.size()));
        if (const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(&col))
            // A LowCardinality Variant alternative only exists on the ColumnBinary wire,
            // which dictionary-encodes it.
            return estimateLowCardTotalBytes(*lc_col, col.size(), /* materialized_const */ false, /* wire_has_dictionary */ true);
        if (col.valuesHaveFixedSize())
            return col.sizeOfValueIfFixed() * col.size();
        return 256 * col.size(); // conservative fallback
    }

    /// Top-level LowCardinality is directly wire-encoded (dictionary + compact index array,
    /// COL_LOWCARD), unlike nested LowCardinality which still materializes; this mirrors that
    /// exact layout in O(1) instead of falling back to a flat per-row guess.
    static size_t estimateLowCardTotalBytes(const ColumnLowCardinality & lc, size_t row_count, bool materialized_const, bool wire_has_dictionary)
    {
        if (!wire_has_dictionary)
        {
            // RowBinary, MsgPack and the text wires have no dictionary encoding: every
            // row carries its resolved value in full, and there is no fixed per-batch
            // dictionary or header cost to reserve. (Charging the shared dictionary
            // here, as the dictionary-wire branch below must, would turn a large
            // dictionary into a spurious "constant arguments alone exceed the budget"
            // failure on wires that never send it.)
            if (row_count == 0)
                return 0;
            const IColumn & dict_col = *lc.getDictionary().getNestedColumn();
            if (materialized_const)
                return estimateComplexRowBytes(dict_col, lc.getIndexes().getUInt(0)) * row_count;
            // Bound every row by the largest dictionary value: a one-time O(dictionary
            // size) scan, which is bounded by the column's own data size.
            size_t max_value_bytes = 0;
            for (size_t j = 0; j < dict_col.size(); ++j)
                max_value_bytes = std::max(max_value_bytes, estimateComplexRowBytes(dict_col, j));
            return max_value_bytes * row_count;
        }
        constexpr size_t header_bytes = 4 + 4 + 40; // dict_row_count + index_elem_width/pad + embedded ColDescriptor
        if (materialized_const)
        {
            // header_bytes is a fixed per-column structural cost present on the wire
            // regardless of how many rows a given batch carries; a row_count == 0 reservation
            // call must still charge it, but must not charge the (row-scaled) materialized
            // value cost below.
            if (row_count == 0)
                return header_bytes;
            // getNestedNotNullableColumn, not getNestedColumn: see the matching comment on the
            // non-const path below.
            const IColumn & dict_col = *lc.getDictionary().getNestedNotNullableColumn();
            // A materializing format expands the single constant value row_count times; the
            // dictionary then holds just that one distinct value, and the index array is
            // row_count entries all pointing at it.
            return lc.getIndexes().sizeOfValueIfFixed() * row_count + estimateComplexRowBytes(dict_col, lc.getIndexes().getUInt(0)) + header_bytes;
        }
        // Unlike header_bytes, the dictionary is NOT a "real content that scales with
        // row_count, correctly zero for a 0-row batch" quantity: buildColDescriptor's
        // COL_LOWCARD branch always writes `lc.getDictionary().getNestedColumn()` — the SAME
        // dictionary object shared across every row-range slice of this argument (cut()/split
        // doesn't prune it down to just that batch's used values) — so every batch produced
        // from this same LowCardinality argument pays the dictionary's full cost regardless of
        // how many rows that particular batch has. Treating it as zero at row_count == 0 (as
        // an earlier version of this function did) undercounted the fixed per-batch
        // reservation for a large shared dictionary.
        //
        // getNestedNotNullableColumn, not getNestedColumn: for a LowCardinality(Nullable(T))
        // the latter hands back the ColumnUnique's ColumnNullable wrapper, which
        // estimateAggregateColumnBytes would charge one null-map byte per dictionary row for.
        // COL_LOWCARD writes no dictionary null map — buildColDescriptor's top-level unwrap
        // strips that wrapper and passes is_nullable=false, so nullability travels in
        // ColumnUnique's reserved slot layout instead — and over-reserving here would make a
        // large shared dictionary trip the dynamic splitter's byte-budget exception on batches
        // that actually fit.
        const IColumn & dict_col = *lc.getDictionary().getNestedNotNullableColumn();
        size_t dict_bytes = estimateAggregateColumnBytes(dict_col);
        if (row_count == 0)
            return header_bytes + dict_bytes;
        return lc.getIndexes().sizeOfValueIfFixed() * row_count + dict_bytes + header_bytes;
    }

    /// Top-level Variant only (nested Variant is validator-rejected, structurally unreachable);
    /// mirrors COL_VARIANT's wire layout (discriminators + row offsets + per-alternative data).
    static size_t estimateVariantTotalBytes(const ColumnVariant & var, size_t row_count, bool materialized_const)
    {
        size_t num_variants = var.getNumVariants();
        // num_variants is every declared alternative; the wire header only counts non-empty
        // ones (k <= num_variants, see the COL_VARIANT writer in ColumnBinaryWire.h), so this is
        // a safe upper bound rather than an exact figure.
        size_t header_bytes = 4 + num_variants * (4 + 40); // sub_rows + embedded ColDescriptor per alternative
        // Fixed per-column structural cost, present on the wire regardless of row count (see
        // the matching comment in estimateTotalSerializedSize above); must still be charged at
        // row_count == 0, but not the real (non-row-scaled) per-alternative data.
        //
        // Except: a LowCardinality alternative is itself direct-COL_LOWCARD-encoded here (the
        // Variant writer's buildColDescriptor recursion on each alternative applies the same
        // top-level dispatch used for a standalone LowCardinality argument), so it carries the
        // same shared, not-row-scaled dictionary cost — paid again for every split batch that
        // includes this alternative, exactly like a top-level LowCardinality argument (see
        // estimateLowCardTotalBytes above). That must be part of the row_count == 0 fixed
        // reservation too, or a Variant(LowCardinality(String), ...) batch can look cheap here
        // while estimateVariantRowBytes (below) only charges the per-row index width — leaving
        // the dictionary's cost uncounted on both sides of the split-path estimate.
        if (row_count == 0)
        {
            size_t fixed_sub_bytes = 0;
            for (size_t local = 0; local < num_variants; ++local)
            {
                const IColumn & sub = var.getVariantByLocalDiscriminator(local);
                if (const auto * lc_sub = typeid_cast<const ColumnLowCardinality *>(&sub); lc_sub && !sub.empty())
                    // Variant alternatives only exist on the ColumnBinary wire, which
                    // dictionary-encodes LowCardinality.
                    fixed_sub_bytes += estimateLowCardTotalBytes(*lc_sub, 0, false, /* wire_has_dictionary */ true);
            }
            return header_bytes + fixed_sub_bytes;
        }
        if (materialized_const)
            // Each of the row_count materialized rows needs its own discriminator + row-offset
            // + payload (estimateVariantRowBytes already includes all three per row).
            return estimateVariantRowBytes(var, 0) * row_count + header_bytes;
        size_t sub_bytes = 0;
        for (size_t local = 0; local < num_variants; ++local)
        {
            const IColumn & sub = var.getVariantByLocalDiscriminator(local);
            if (!sub.empty())
                sub_bytes += estimateAggregateColumnBytes(sub);
        }
        return row_count /* discriminators */ + row_count * 4 /* row offsets */ + header_bytes + sub_bytes;
    }

    /// Precise per-row Variant cost: locate the row's active alternative and size just that
    /// one value, rather than the flat 256-byte guess. Every row unconditionally carries a
    /// 1-byte discriminator and a 4-byte row-offset entry on the wire (see the COL_VARIANT
    /// writer in ColumnBinaryWire.h), even a null row, so those are never skipped.
    static size_t estimateVariantRowBytes(const ColumnVariant & var, size_t row_index)
    {
        constexpr size_t control_bytes = 1 + 4; // discriminator + row-offset entry
        auto global_discr = var.globalDiscriminatorAt(row_index);
        if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
            return control_bytes;
        const IColumn & sub = var.getVariantByGlobalDiscriminator(global_discr);
        size_t sub_row = var.getOffsets()[row_index];
        if (const auto * lc_sub = typeid_cast<const ColumnLowCardinality *>(&sub))
            // Mirrors the top-level LowCardinality argument's per-row model in
            // estimateRowSerializedSize: the dictionary's cost is already reserved once per
            // batch (via estimateVariantTotalBytes's row_count == 0 case above), so this row's
            // only marginal cost is its compact index entry. estimateComplexRowBytes would
            // instead price the resolved dictionary value per row and never account for the
            // shared dictionary at all, undercounting the real per-batch wire cost.
            return control_bytes + lc_sub->getIndexes().sizeOfValueIfFixed();
        return control_bytes + estimateComplexRowBytes(sub, sub_row);
    }

    /// Estimate the serialized byte size of a single row across all argument columns.
    /// Used for the cumulative flush pass below: a fixed stride derived from the average
    /// row size can still put an oversized row in the same batch as its neighbors and
    /// blow the input budget on a skewed block (e.g. one huge string among many tiny ones).
    /// Same wire-model contract as estimateTotalSerializedSize above (the execute()
    /// callers apply wire_size_expansion_factor and perRowWireOverhead): not a memory-safety
    /// issue, see the comment there.
    size_t estimateRowSerializedSize(const ColumnsWithTypeAndName & arguments, size_t row, bool preserve_const) const
    {
        const auto & declared_arguments = user_defined_function->getArguments();
        size_t total = 0;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto & arg = arguments[i];
            // Size by the declared (post-cast) type; see estimateTotalSerializedSize above.
            const DataTypePtr & declared_type = declared_arguments[i];
            const IColumn * col = arg.column.get();
            size_t row_index = row;
            bool materialized_const = false;
            if (typeid_cast<const ColumnConst *>(col))
            {
                if (preserve_const)
                    continue; // fixed per-batch cost, not per-row
                col = &typeid_cast<const ColumnConst &>(*col).getDataColumn();
                row_index = 0; // materialized const columns only ever have row 0
                materialized_const = true;
            }
            // See the matching unwrap in estimateTotalSerializedSize above.
            bool is_col_nullable = false;
            if (const auto * null_col = typeid_cast<const ColumnNullable *>(col))
            {
                col = &null_col->getNestedColumn();
                is_col_nullable = true;
            }
            size_t null_map_bytes = is_col_nullable ? 1 : 0;
            // See the matching unwrapped_declared_type comment in estimateTotalSerializedSize above.
            DataTypePtr unwrapped_declared_type = is_col_nullable ? removeNullable(declared_type) : declared_type;
            if (const auto * s = typeid_cast<const ColumnString *>(col))
                // + sizeof(uint64_t): amortized per-row share of the wire offsets[row_count+1]
                // array that accompanies every non-const String column; see the matching
                // comment in estimateTotalSerializedSize above.
                total += s->getOffsets()[row_index] - (row_index > 0 ? s->getOffsets()[row_index - 1] : 0) + sizeof(uint64_t) + null_map_bytes;
            else if (unwrapped_declared_type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
                total += unwrapped_declared_type->getSizeOfValueInMemory() + null_map_bytes;
            else if (typeid_cast<const ColumnArray *>(col) || typeid_cast<const ColumnTuple *>(col)
                    || typeid_cast<const ColumnMap *>(col))
                total += estimateComplexRowBytes(*col, row_index) + null_map_bytes;
            else if (const auto * lc_col = typeid_cast<const ColumnLowCardinality *>(col))
            {
                if (materialized_const || !wire_encodes_low_cardinality)
                    // No dictionary/index concept applies: either the column is a
                    // materialized constant, or the wire itself (RowBinary/MsgPack/CSV/...)
                    // has no dictionary encoding — the resolved value is broadcast in full
                    // on every row, matching what estimateComplexRowBytes already computes
                    // for the nested-materialized case.
                    total += estimateComplexRowBytes(*lc_col, row_index) + null_map_bytes;
                else
                    // Top-level direct COL_LOWCARD encoding: the dictionary's (potentially
                    // large) cost is already reserved once per batch via
                    // estimateLowCardTotalBytes (see estimateTotalSerializedSize above), so
                    // this row's only genuine marginal cost is its compact index entry —
                    // charging the resolved value's full size here too would double-count the
                    // dictionary and split far more aggressively than the real wire cost needs.
                    total += lc_col->getIndexes().sizeOfValueIfFixed() + null_map_bytes;
            }
            else if (const auto * var_col = typeid_cast<const ColumnVariant *>(col))
                total += estimateVariantRowBytes(*var_col, row_index) + null_map_bytes;
            else
                total += 256 + null_map_bytes; // conservative fallback
        }
        return total;
    }

    ColumnPtr execute(WebAssembly::WasmCompartment * compartment, const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        MutableColumnPtr result_column = user_defined_function->getResultType()->createColumn();

        const size_t fixed_block_size = context->getSettingsRef()[Setting::webassembly_udf_max_input_block_size];

        // When no explicit block size is given, split input dynamically: estimate the total
        // serialized size once (O(1)) and split only if it would exceed 50% of the guest's
        // *current* linear memory — a realistic proxy for what it can hold right now, since a
        // guest that hasn't grown its memory yet cannot suddenly hold a batch sized to the
        // configured ceiling. The hard "this can never fit" throws below instead use
        // getMaxLinearMemorySize() (the configured memory_limit ceiling): the guest's allocator
        // can still grow into it before the call is made, so gating those on the current size
        // alone would reject inputs the guest could actually satisfy after growing.
        const size_t wasm_linear_memory = compartment->getLinearMemorySize();
        const size_t input_budget = (fixed_block_size == 0 && wasm_linear_memory > 0)
            ? wasm_linear_memory / 2  // 50% for input, leave room for GEOS heap
            : 0;
        const size_t wasm_linear_memory_ceiling = compartment->getMaxLinearMemorySize();
        const size_t input_budget_ceiling = (fixed_block_size == 0 && wasm_linear_memory_ceiling > 0)
            ? wasm_linear_memory_ceiling / 2
            : 0;

        size_t batch_start = 0;

        // Only formats that neither expect materialized columns nor support column-schema
        // output actually keep ColumnConst compact on the wire (e.g. ColumnBinary's
        // COL_IS_CONST); Buffers goes through NativeWriter::writeData, which unconditionally
        // calls convertToFullColumnIfConst() before writing, so it must NOT be treated as
        // const-preserving here despite exposing a native serialization format. Drive this off
        // the same format-capability check the constructor already computed
        // (computePreserveConstColumns) rather than hardcoding format names, so the splitter's
        // size estimate always matches what the serializer actually does.
        const bool preserve_const = preserve_const_columns;

        auto flush_batch = [&](size_t end_idx)
        {
            if (end_idx <= batch_start)
                return;
            size_t batch_size = end_idx - batch_start;
            auto block = getArgumentsBlock(arguments, batch_start, batch_size, preserve_const);
            auto stop_token = interrupt_source.get_token();
            auto col = user_defined_function->executeOnBlock(compartment, block, context, batch_size, stop_token);

            // Under BUFFERED_V1 + ColumnBinary (preserve_const), a guest may legitimately
            // return COL_IS_CONST, which ColumnBinaryInputFormat decodes as a ColumnConst;
            // structureEquals only holds between two ColumnConst instances, so compare the
            // unwrapped nested column against result_column instead of rejecting every valid
            // const result. See the matching fix in flush_columnar_batch above.
            const IColumn * col_for_check = col.get();
            if (const auto * col_const = typeid_cast<const ColumnConst *>(col_for_check))
                col_for_check = &col_const->getDataColumn();
            if (!result_column->structureEquals(*col_for_check))
                throw Exception(
                    ErrorCodes::WASM_ERROR,
                    "Different column types in result blocks: {} and {}",
                    result_column->dumpStructure(),
                    col->dumpStructure());

            // A ColumnConst batch result must be materialized before it's accumulated:
            // ColumnConst::insertRangeFrom only bumps the row count, it doesn't copy in the
            // source's actual value, so concatenating a later (possibly different) batch into a
            // ColumnConst accumulator would silently keep repeating the first batch's value for
            // every row appended afterwards.
            col = IColumn::mutate(col->convertToFullColumnIfConst());
            if (result_column->empty())
                result_column = col->assumeMutable();
            else
                result_column->insertRangeFrom(*col, 0, col->size());

            batch_start = end_idx;
        };

        if (input_budget > 0)
        {
            // Worst-case wire expansion of the in-memory estimates for this
            // serialization format (including the enum name-to-width ratio folded in by
            // enumTextWireExpansion), plus the per-row structural bytes (delimiters,
            // JSONEachRow keys) the value-based estimates cannot see; both are identity
            // for the binary formats. See wireSizeExpansionFactor / perRowWireOverhead above.
            const size_t expansion = wire_size_expansion_factor;
            const size_t per_row_overhead = perRowWireOverhead(arguments);

            // O(1) block-level check: only scan per-row when splits are actually needed.
            // The common case (block fits in budget) pays zero per-row overhead.
            size_t total_bytes = estimateTotalSerializedSize(arguments, input_rows_count, preserve_const) * expansion
                + per_row_overhead * input_rows_count;
            if (total_bytes > input_budget)
            {
                // Preserved ColumnConst arguments, and the format's own fixed per-batch
                // overhead (a ColumnBinary frame header plus its descriptor table), are
                // charged once per batch rather than per row by estimateTotalSerializedSize;
                // calling it with row_count=0 zeroes out every per-row-scaled term and leaves
                // just that fixed reserved cost. Every batch this loop produces still has to
                // pay it, so seed running_bytes with it (and fail up front if it alone can
                // never fit), or a batch of many tiny rows could still exceed input_budget by
                // the preserved const's size or by the descriptor table of a wide block.
                size_t const_reserved_bytes = estimateTotalSerializedSize(arguments, 0, preserve_const) * expansion;
                if (const_reserved_bytes > input_budget_ceiling)
                    throw Exception(ErrorCodes::WASM_ERROR,
                        "WASM UDF preserved constant arguments and per-batch format overhead alone "
                        "require an estimated {} bytes, exceeding the {} byte input budget derived "
                        "from the module's linear memory",
                        const_reserved_bytes, input_budget_ceiling);

                // Cumulative per-row pass: flush before the next row would cross the
                // budget. A fixed stride derived from the average row size cannot bound
                // a skewed block (e.g. one huge string among many tiny ones) — the
                // oversized row would still land in a batch together with its neighbors.
                size_t running_bytes = const_reserved_bytes;
                for (size_t row = 0; row < input_rows_count; ++row)
                {
                    size_t row_bytes = estimateRowSerializedSize(arguments, row, preserve_const) * expansion + per_row_overhead;
                    if (const_reserved_bytes + row_bytes > input_budget_ceiling)
                        throw Exception(ErrorCodes::WASM_ERROR,
                            "WASM UDF input row {} alone requires an estimated {} bytes, exceeding the "
                            "{} byte input budget derived from the module's linear memory; it cannot be "
                            "split into a smaller batch",
                            row, row_bytes, input_budget_ceiling);
                    if (row > batch_start && running_bytes + row_bytes > input_budget)
                    {
                        flush_batch(row);
                        running_bytes = const_reserved_bytes;
                    }
                    running_bytes += row_bytes;
                }
            }
        }
        else if (fixed_block_size > 0)
        {
            for (size_t row = fixed_block_size; row < input_rows_count; row += fixed_block_size)
                flush_batch(row);
        }

        flush_batch(input_rows_count);
        return result_column;
    }

    Block getArgumentsBlock(const ColumnsWithTypeAndName & arguments, size_t start_idx, size_t length, bool preserve_const) const
    {
        const auto & declared_arguments = user_defined_function->getArguments();
        Block arguments_block;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            ColumnPtr column = arguments[i].column;
            if (!preserve_const)
                column = column->convertToFullColumnIfConst();
            /// Skip the copy when the requested range already covers the whole column.
            if (start_idx != 0 || length != column->size())
                column = column->cut(start_idx, length);
            String column_name = i < argument_names.size() && !argument_names[i].empty() ? argument_names[i] : arguments[i].name;
            /// Cast to the declared type so serialization uses the correct width.
            /// Without this, e.g. Int8 passed to an Int32 parameter would be serialized
            /// as 1 byte by RowBinary instead of 4, causing the WASM module to read garbage.
            /// ColumnBinary's descriptor only encodes a coarse width class (COL_FIXED8/16/32/64),
            /// not exact signedness — a UInt8(255) and an Int8(-1) both serialize to the same
            /// single 0xff byte, so a guest reading a declared Int32 has no way to tell them
            /// apart. Always cast here regardless of format until the wire format carries
            /// real logical type/signedness information.
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
    bool preserve_const_columns;
    /// Empty for ColumnBinary (its wire never goes through a serialization format).
    String buffered_serialization_format;
    /// Worst-case wire-size expansion of the in-memory estimate for this wire; see
    /// wireSizeExpansionFactor above.
    size_t wire_size_expansion_factor = 1;
    /// Whether the wire keeps LowCardinality dictionary-encoded (dictionary + compact
    /// indexes) rather than materializing the resolved value on every row.
    bool wire_encodes_low_cardinality = true;

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
