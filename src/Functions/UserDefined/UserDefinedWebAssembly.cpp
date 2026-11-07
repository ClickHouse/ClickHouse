#include <Functions/UserDefined/UserDefinedWebAssembly.h>
#include <Functions/UserDefined/UserDefinedWebAssemblyBatchRows.h>
#include <Functions/UserDefined/UserDefinedWebAssemblyColumnarCodec.h>
#include <Functions/UserDefined/UserDefinedWebAssemblyScriptAbi.h>
#include <Functions/UserDefined/UserDefinedWebAssemblyTypeHelpers.h>

#include <ranges>
#include <algorithm>
#include <Common/Hex.h>

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
using namespace ColumnarWire;

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

    /// Drives `input_format` directly: a QueryPipeline around one source adds only its own
    /// construction cost, which dominated read-back in profiles.
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
                result_chunk.append(chunk);

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

        /// Built once per call: `getFormatSettings` is expensive.
        const FormatSettings format_settings = getFormatSettings(context);
        const Block empty_header = block.cloneEmpty();

        WasmMemoryGuard wasm_data = nullptr;
        if (!block.empty())
        {
            ProfileEventTimeIncrement<Microseconds> timer_serialize(ProfileEvents::WasmSerializationMicroseconds);

            /// Serialize into a String first: WriteBufferForWasmMemory calls into the guest on
            /// construction, which crashes during the constant-folding dry run.
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
};

/// COLUMNAR_V1 ABI: the guest export is `(i32 input_handle, i32 num_rows) -> i32 result_handle`.
/// Frame layout: UserDefinedWebAssemblyColumnarWire.h.

class UserDefinedWebAssemblyFunctionColumnarV1 : public UserDefinedWebAssemblyFunction
{
public:
    template <typename... Args>
    explicit UserDefinedWebAssemblyFunctionColumnarV1(Args &&... args)
        : UserDefinedWebAssemblyFunction(std::forward<Args>(args)...)
    {
        col_function_name = function_name;
        checkSignature();
        /// Reject unsupported types at CREATE FUNCTION time.
        for (const auto & arg : arguments)
        {
            validateColumnarWireSupportedType(arg);
            /// The frame cannot tell LowCardinality(Nullable(T)) from LowCardinality(T): NULL is
            /// just dictionary row 0. Declare Nullable(T) or LowCardinality(T) instead.
            if (arg->isLowCardinalityNullable())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "WebAssembly COLUMNAR_V1 does not support LowCardinality(Nullable(T)) arguments: {}; "
                    "the LowCardinality wire frame cannot represent a nullable dictionary",
                    arg->getName());
        }
        validateColumnarWireSupportedType(result_type);
        /// The guest cannot return COL_LOWCARD.
        if (result_type->lowCardinality())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "WebAssembly COLUMNAR_V1 does not support a LowCardinality result type: {}; "
                "the guest cannot emit a LowCardinality column",
                result_type->getName());
    }

    bool requiresGuestLinearMemory() const override { return true; }

    bool serializesInputBlockToGuestMemory() const override { return true; }

    /// Keep LowCardinality arguments as COL_LOWCARD instead of materializing them.
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    MutableColumnPtr executeColumnar(
        WebAssembly::WasmCompartment * compartment,
        const ColumnsWithTypeAndName & cols,
        size_t input_rows_count,
        ContextPtr,
        StopToken stop_token) const
    {
        ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::WasmTotalExecuteMicroseconds);

        if (input_rows_count == 0)
            return result_type->createColumn();

        if (input_rows_count >= std::numeric_limits<uint32_t>::max())
            throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large number of rows: {}", input_rows_count);

        const uint32_t num_cols = static_cast<uint32_t>(cols.size());

        std::vector<ColDescriptor> descs; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<const IColumn *> inner_cols; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<bool> is_nullable_flags; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<uint32_t> row_counts; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<ColumnPtr> casted_columns; // STYLE_CHECK_ALLOW_STD_CONTAINERS -- keeps casted columns alive through writeColData below

        const uint64_t total_buf_size = buildColumnarLayout(
            cols, input_rows_count, /* declared_positions */ {},
            descs, inner_cols, is_nullable_flags, row_counts, casted_columns);

        {
            auto wmm = std::make_unique<WasmMemoryManagerV01>(compartment, stop_token);
            WasmMemoryGuard wasm_input = nullptr;

            /// Times allocation and writing only, not the guest call or the read-back.
            {
                ProfileEventTimeIncrement<Microseconds> timer_ser(ProfileEvents::WasmSerializationMicroseconds);

                wasm_input = allocateInWasmMemory(wmm.get(), total_buf_size);
                auto wasm_mem = wasm_input.getMemoryView();
                /// The guest may return a smaller buffer than requested.
                if (wasm_mem.size() != total_buf_size)
                    throw Exception(ErrorCodes::WASM_ERROR,
                        "Cannot allocate WASM buffer of size {}, got {}. "
                        "Maybe '{}' function implementation in WebAssembly module is incorrect",
                        total_buf_size, wasm_mem.size(), WasmMemoryManagerV01::allocate_function_name);

                // Write the frame header defined in `UserDefinedWebAssemblyColumnarWire.h`: magic,
                // version and reserved word, then the row and column counts.
                writeFrameHeader(wasm_mem.data(), static_cast<uint32_t>(input_rows_count), num_cols);

                for (uint32_t ci = 0; ci < num_cols; ++ci)
                    std::memcpy(wasm_mem.data() + FRAME_HEADER_BYTES + ci * COL_DESC_BYTES,
                                &descs[ci], COL_DESC_BYTES);

                for (uint32_t ci = 0; ci < num_cols; ++ci)
                    writeColData(inner_cols[ci], is_nullable_flags[ci], row_counts[ci],
                                 descs[ci], wasm_mem);
            }

            auto result_ptr = compartment->invoke<WasmPtr>(
                col_function_name,
                {wasm_input.getHandle(), static_cast<WasmSizeT>(input_rows_count)},
                stop_token);

            if (result_ptr == 0)
                throw Exception(ErrorCodes::WASM_ERROR,
                    "COLUMNAR_V1 function '{}' returned nullptr", col_function_name);

            WasmMemoryGuard result_guard(wmm.get(), result_ptr);

            {
                ProfileEventTimeIncrement<Microseconds> timer_de(ProfileEvents::WasmDeserializationMicroseconds);
                auto out_view = result_guard.getMemoryView();
                return readColumnarOutput(
                    {out_view.data(), out_view.size()},
                    result_type,
                    input_rows_count);
            }
        }
    }

    /// Unused: FunctionUserDefinedWasm calls executeColumnar directly.
    MutableColumnPtr executeOnBlock(
        WebAssembly::WasmCompartment * compartment,
        const Block & block,
        ContextPtr context,
        size_t num_rows,
        StopToken stop_token) const override
    {
        ColumnsWithTypeAndName args;
        args.reserve(block.columns());
        for (size_t i = 0; i < block.columns(); ++i)
            args.push_back(block.getByPosition(i));
        return executeColumnar(compartment, args, num_rows, context, stop_token);
    }

    /// The exact bytes a call carrying `[start_idx, start_idx + length)` puts in guest memory.
    /// `declared_positions` maps each entry of `cols` to its declared argument; empty means in order.
    size_t measureColumnarBytes(
        const ColumnsWithTypeAndName & cols,
        size_t start_idx,
        size_t length,
        const VectorWithMemoryTracking<size_t> & declared_positions = {}) const
    {
        ColumnsWithTypeAndName batch;
        batch.reserve(cols.size());
        for (const auto & col : cols)
        {
            /// A const column stays one row on the wire; pass it whole.
            bool whole_column = isColumnConst(*col.column) || (start_idx == 0 && length == col.column->size());
            batch.emplace_back(whole_column ? col.column : col.column->cut(start_idx, length), col.type, col.name);
        }

        std::vector<ColDescriptor> descs; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<const IColumn *> inner_cols; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<bool> is_nullable_flags; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<uint32_t> row_counts; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<ColumnPtr> casted_columns; // STYLE_CHECK_ALLOW_STD_CONTAINERS

        return buildColumnarLayout(
            batch, length, declared_positions, descs, inner_cols, is_nullable_flags, row_counts, casted_columns);
    }

private:
    /// Fills the descriptors and returns the frame size. `casted_columns` owns the casts
    /// and must outlive `inner_cols`.
    uint64_t buildColumnarLayout(
        const ColumnsWithTypeAndName & cols,
        size_t input_rows_count,
        const VectorWithMemoryTracking<size_t> & declared_positions,
        std::vector<ColDescriptor> & descs, // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<const IColumn *> & inner_cols, // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<bool> & is_nullable_flags, // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<uint32_t> & row_counts, // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<ColumnPtr> & casted_columns) const // STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        const uint32_t num_cols = static_cast<uint32_t>(cols.size());
        uint64_t cursor = FRAME_HEADER_BYTES + num_cols * COL_DESC_BYTES;

        descs.assign(num_cols, ColDescriptor{});
        inner_cols.assign(num_cols, nullptr);
        is_nullable_flags.assign(num_cols, false);
        row_counts.assign(num_cols, 0);
        casted_columns.assign(num_cols, nullptr);

        for (uint32_t ci = 0; ci < num_cols; ++ci)
        {
            /// Cast to the declared type so the wire width matches it. Keep Nullable if the
            /// actual argument is nullable, or the cast throws on the first NULL.
            const DataTypePtr & declared_arg_type = arguments[declared_positions.empty() ? ci : declared_positions[ci]];
            DataTypePtr target_type = (removeLowCardinality(cols[ci].type)->isNullable() && !declared_arg_type->isNullable())
                ? makeNullable(declared_arg_type)
                : declared_arg_type;
            casted_columns[ci] = compactLowCardinalityDictionaries(cols[ci].type->equals(*target_type)
                ? cols[ci].column
                : castColumn(cols[ci], target_type), target_type);
            const IColumn * col = casted_columns[ci].get();
            bool is_const = false;

            if (const auto * cc = typeid_cast<const ColumnConst *>(col))
            {
                col = &cc->getDataColumn();
                is_const = true;
            }

            bool is_nullable = typeid_cast<const ColumnNullable *>(col) != nullptr;
            uint32_t nrows = is_const ? 1u : static_cast<uint32_t>(input_rows_count);

            is_nullable_flags[ci] = is_nullable;
            inner_cols[ci] = col;
            row_counts[ci] = nrows;

            cursor = buildColDescriptor(col, is_const, is_nullable, nrows, cursor, descs[ci]);
        }

        return cursor;
    }

    void checkSignature() const
    {
        auto decl = wasm_module->getExport(col_function_name);
        WasmFunctionDeclaration expected("", col_function_name,
            {WasmValKind::I32, WasmValKind::I32}, WasmValKind::I32);
        checkFunctionDeclarationMatches(decl, expected);
        checkFunctionDeclarationMatches(
            wasm_module->getExport(WasmMemoryManagerV01::allocate_function_name),
            WasmMemoryManagerV01::allocateFunctionDeclaration());
        checkFunctionDeclarationMatches(
            wasm_module->getExport(WasmMemoryManagerV01::deallocate_function_name),
            WasmMemoryManagerV01::deallocateFunctionDeclaration());
    }

    String col_function_name;
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
        case WasmAbiVersion::ColumnarV1:
            return std::make_unique<UserDefinedWebAssemblyFunctionColumnarV1>(
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
        case WasmAbiVersion::ColumnarV1:
            return "COLUMNAR_V1";
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR, "Unknown WebAssembly ABI version: {}", std::to_underlying(abi_type));
}

WasmAbiVersion getWasmAbiFromString(const String & str)
{
    for (auto abi_type : {WasmAbiVersion::RowDirect, WasmAbiVersion::BufferedV1, WasmAbiVersion::AssemblyScript, WasmAbiVersion::ColumnarV1})
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
        : Base(limit, getLogger("WasmCompartmentPool"), /* pool_outlives_query_ = */ false)
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

    String getName() const override { return function_name; }
    bool isVariadic() const override { return false; }
    bool isDeterministic() const override { return user_defined_function->getIsDeterministic(); }
    /// A UDF not declared `DETERMINISTIC` may return different values for the same arguments even
    /// within a single query - the module can keep state or read entropy - so it must not be treated
    /// as query-deterministic. `IFunction` answers `true` by default, which would let query plan
    /// optimizations duplicate or reorder such a call. `Executable` UDFs answer `false` here as well.
    bool isDeterministicInScopeOfQuery() const override { return user_defined_function->getIsDeterministic(); }
    bool isSpatialPredicate() const override
    {
        auto val = user_defined_function->getSettings().getValue("is_spatial_predicate");
        if (val.getType() == Field::Types::Bool)
            return val.safeGet<bool>();
        return val.safeGet<UInt64>() != 0;
    }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /* arguments */) const override { return false; }
    size_t getNumberOfArguments() const override { return user_defined_function->getArguments().size(); }

    /// A declared Nullable(T) parameter means the guest handles NULL rows itself.
    bool useDefaultImplementationForNulls() const override
    {
        return !(isColumnarV1() && declaresArgument([](const DataTypePtr & type) { return type->isNullable(); }));
    }

    /// A declared Variant parameter takes the whole Variant column. The default Variant adaptor
    /// also handles NULL rows itself, so it is used only together with the default NULL handling,
    /// as in `IFunction`: a guest that handles NULL must not lose its NULL rows to the adaptor.
    bool useDefaultImplementationForVariant() const override
    {
        return useDefaultImplementationForNulls()
            && !(isColumnarV1() && declaresArgument([](const DataTypePtr & type) { return typeid_cast<const DataTypeVariant *>(type.get()) != nullptr; }));
    }

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

            /// With default NULL handling off, arguments arrive Nullable. COLUMNAR_V1 passes the
            /// null map on, so strip Nullable and match again. BUFFERED_V1 casts to the declared
            /// type and would lose the NULLs, so it does not get this.
            bool allow_nullable_relaxation
                = dynamic_cast<const UserDefinedWebAssemblyFunctionColumnarV1 *>(user_defined_function.get()) != nullptr;
            if (allow_nullable_relaxation)
            {
                /// The framework's NULL handling only sees Nullable(T), and LowCardinality is kept
                /// as is, so a LowCardinality(Nullable(T)) argument would skip it and hand the guest
                /// a null map it did not ask for. Only a function that already handles NULL itself
                /// may take one.
                if (useDefaultImplementationForNulls() && arguments[i]->isLowCardinalityNullable())
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "WebAssembly COLUMNAR_V1 function does not handle NULL itself, so it cannot take a "
                        "LowCardinality(Nullable(T)) argument: {}; declare a Nullable parameter or cast the argument",
                        arguments[i]->getName());

                const DataTypePtr & stripped = removeNullable(arguments[i]);
                if (stripped->equals(*expected_arguments[i]))
                    continue;

                /// A bare `NULL` is `Nullable(Nothing)`; buildColumnarLayout casts it to the Nullable declared type.
                if (arguments[i]->onlyNull() && !expected_arguments[i]->lowCardinality()
                    && (expected_arguments[i]->isNullable() || expected_arguments[i]->canBeInsideNullable()))
                    continue;

                /// COLUMNAR_V1 keeps LowCardinality, so LowCardinality(T) and T match either way;
                /// the cast in buildColumnarLayout converts the data. A nullable argument cannot be
                /// passed as LowCardinality(T): the frame has no null map for it.
                const bool nullable_into_lowcard
                    = expected_arguments[i]->lowCardinality() && removeLowCardinality(arguments[i])->isNullable();
                auto strip_lc = [](const DataTypePtr & type) -> DataTypePtr
                {
                    if (const auto * lc = typeid_cast<const DataTypeLowCardinality *>(type.get()))
                        return lc->getDictionaryType();
                    return type;
                };
                const DataTypePtr actual_cmp = removeNullable(strip_lc(stripped));
                const DataTypePtr expected_cmp = removeNullable(strip_lc(expected_arguments[i]));
                if (!nullable_into_lowcard && actual_cmp->equals(*expected_cmp))
                    continue;

                /// Allow implicit coercions: same kind, i32→i64, any int→any float, f32→f64.
                auto actual_kind = wasmKindForDataType(actual_cmp.get());
                auto expected_kind = wasmKindForDataType(expected_cmp.get());
                if (!nullable_into_lowcard && actual_kind && expected_kind && canCoerce(*actual_kind, *expected_kind))
                    continue;
            }
            else
            {
                auto actual_kind = wasmKindForDataType(arguments[i].get());
                auto expected_kind = wasmKindForDataType(expected_arguments[i].get());
                if (actual_kind && expected_kind && canCoerce(*actual_kind, *expected_kind))
                    continue;
            }

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

    /// COLUMNAR_V1 keeps LowCardinality columns so they reach the guest as COL_LOWCARD.
    bool useDefaultImplementationForLowCardinalityColumns() const override
    {
        return user_defined_function->useDefaultImplementationForLowCardinalityColumns();
    }

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
            /// COLUMNAR_V1 passes columns directly, split by the same limits as the buffered path.
            if (const auto * cv1 = dynamic_cast<const UserDefinedWebAssemblyFunctionColumnarV1 *>(user_defined_function.get()))
            {
                checkModuleCanHoldInput(compartment_ptr, input_rows_count);

                auto stop_token = interrupt_source.get_token();
                auto expected_col = user_defined_function->getResultType()->createColumn();
                Columns result_parts;

                auto flush_columnar_batch = [&](size_t batch_start, size_t end_idx)
                {
                    if (end_idx <= batch_start)
                        return;
                    size_t batch_size = end_idx - batch_start;
                    ColumnsWithTypeAndName batch_cols;
                    batch_cols.reserve(arguments.size());
                    for (const auto & arg : arguments)
                    {
                        /// cut() copies; skip it when the batch is the whole column.
                        bool whole_column = batch_start == 0 && batch_size == arg.column->size();
                        batch_cols.emplace_back(
                            whole_column ? arg.column : arg.column->cut(batch_start, batch_size), arg.type, arg.name);
                    }
                    auto result = cv1->executeColumnar(compartment_ptr, batch_cols, batch_size, context, stop_token);
                    /// A const result is valid; compare its nested column.
                    const IColumn * result_for_check = result.get();
                    if (const auto * result_const = typeid_cast<const ColumnConst *>(result_for_check))
                        result_for_check = &result_const->getDataColumn();
                    if (!result_for_check->structureEquals(*expected_col))
                        throw Exception(ErrorCodes::WASM_ERROR,
                            "COLUMNAR_V1: returned column structure {} does not match declared type {}",
                            result->dumpStructure(),
                            user_defined_function->getResultType()->getName());
                    result_parts.push_back(result->convertToFullColumnIfConst());
                };

                const size_t fixed_block_size = context->getSettingsRef()[Setting::webassembly_udf_max_input_block_size];
                if (fixed_block_size > 0)
                {
                    for (size_t start = 0; start < input_rows_count; start += fixed_block_size)
                        flush_columnar_batch(start, std::min(start + fixed_block_size, input_rows_count));
                    return concatColumnarResults(result_parts, user_defined_function->getResultType());
                }

                /// Same budget and walk as the buffered path, measured by this frame's layout pass.
                const std::optional<size_t> budget = getInputBudget(compartment_ptr, fixed_block_size);
                if (!budget)
                {
                    flush_columnar_batch(0, input_rows_count);
                    return concatColumnarResults(result_parts, user_defined_function->getResultType());
                }

                const WasmBatchMeasurer measure_columnar
                    = [cv1](const ColumnsWithTypeAndName & batch_cols, size_t start_idx, size_t length,
                            const VectorWithMemoryTracking<size_t> & declared_positions)
                    { return cv1->measureColumnarBytes(batch_cols, start_idx, length, declared_positions); };

                size_t batch_start = 0;
                while (batch_start < input_rows_count)
                {
                    const size_t batch_rows
                        = chooseWasmBatchRows(
                            arguments, batch_start, input_rows_count - batch_start, *budget,
                            compartment_ptr->getMaxLinearMemorySize(), measure_columnar);
                    flush_columnar_batch(batch_start, batch_start + batch_rows);
                    batch_start += batch_rows;
                }
                return concatColumnarResults(result_parts, user_defined_function->getResultType());
            }

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
    bool isColumnarV1() const
    {
        return dynamic_cast<const UserDefinedWebAssemblyFunctionColumnarV1 *>(user_defined_function.get()) != nullptr;
    }

    template <typename Predicate>
    bool declaresArgument(Predicate && predicate) const
    {
        const auto & declared = user_defined_function->getArguments();
        return std::any_of(declared.begin(), declared.end(), predicate);
    }

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

    /// The exact bytes one call carrying `[start, start + length)` puts on the wire. Measured
    /// per batch because frame header, dictionaries and structure prefixes are paid once per call.
    ///
    /// What comes back here is the stream the guest is really handed - framing, wrapping and
    /// shared state included - so the budget below is compared against the actual size rather
    /// than against a bound on it.
    size_t measureBatchBytes(
        const ColumnsWithTypeAndName & arguments,
        size_t start_idx,
        size_t length,
        const VectorWithMemoryTracking<size_t> & declared_positions = {}) const
    {
        auto block = getArgumentsBlock(arguments, start_idx, length, declared_positions);
        NullWriteBuffer measure_buf;
        auto measure_out
            = context->getOutputFormat(serialization_format, measure_buf, block.cloneEmpty());

        measure_out->write(block);
        measure_out->finalize();
        return measure_buf.count();
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

    /// A module whose linear memory is bounded at zero bytes can hold no input at all, whatever
    /// the batching is. This is reported before any measurement, because a function without
    /// arguments has no row to attribute the failure to and would otherwise fail inside the
    /// guest allocator.
    void checkModuleCanHoldInput(const WebAssembly::WasmCompartment * compartment, size_t input_rows_count) const
    {
        if (input_rows_count > 0 && user_defined_function->requiresGuestLinearMemory()
            && compartment->getMaxLinearMemorySize() == 0)
            throw Exception(ErrorCodes::WASM_ERROR,
                "The maximum linear memory of the module is 0 bytes, so it cannot hold the input of the function");
    }

    ColumnPtr execute(WebAssembly::WasmCompartment * compartment, const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        checkModuleCanHoldInput(compartment, input_rows_count);

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
            const WasmBatchMeasurer measure_wire = [this](
                const ColumnsWithTypeAndName & batch_cols, size_t start_idx, size_t length,
                const VectorWithMemoryTracking<size_t> & declared_positions)
            { return measureBatchBytes(batch_cols, start_idx, length, declared_positions); };

            while (batch_start < input_rows_count)
                flush_batch(
                    batch_start
                    + chooseWasmBatchRows(
                        arguments, batch_start, input_rows_count - batch_start, *budget,
                        compartment->getMaxLinearMemorySize(), measure_wire));
        }
        else if (fixed_block_size > 0)
        {
            for (size_t row = fixed_block_size; row < input_rows_count; row += fixed_block_size)
                flush_batch(row);
        }

        flush_batch(input_rows_count);
        return result_column;
    }

    /// `declared_positions` maps each entry of `arguments` to its declared argument; empty means in order.
    Block getArgumentsBlock(
        const ColumnsWithTypeAndName & arguments,
        size_t start_idx,
        size_t length,
        const VectorWithMemoryTracking<size_t> & declared_positions = {}) const
    {
        const auto & declared_arguments = user_defined_function->getArguments();
        Block arguments_block;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const size_t declared_idx = declared_positions.empty() ? i : declared_positions[i];
            /// Cut first, materialize second: `ColumnConst::cut` is O(1), while materializing
            /// the whole block first would make the per-row measurement O(rows^2).
            /// Skip the copy when the requested range already covers the whole column -
            /// the whole-block flush does exactly that for every argument.
            ColumnPtr column = arguments[i].column;
            if (start_idx != 0 || length != column->size())
                column = column->cut(start_idx, length);
            column = column->convertToFullColumnIfConst();
            String column_name = declared_idx < argument_names.size() && !argument_names[declared_idx].empty()
                ? argument_names[declared_idx]
                : arguments[i].name;
            /// Cast to the declared type so serialization uses the correct width.
            /// Without this, e.g. Int8 passed to an Int32 parameter would be serialized
            /// as 1 byte by RowBinary instead of 4, causing the WASM module to read garbage.
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
