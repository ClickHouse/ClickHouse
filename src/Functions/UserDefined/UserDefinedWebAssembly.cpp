#include <Functions/UserDefined/UserDefinedWebAssembly.h>
#include <Functions/UserDefined/UserDefinedWebAssemblyScriptAbi.h>
#include <Functions/UserDefined/UserDefinedWebAssemblyTypeHelpers.h>

#include <ranges>
#include <base/hex.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>

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

// ─────────────────────────────────────────────────────────────────────────────
// COLUMNAR_V1 ABI
//
// Wire format (all offsets are byte offsets from the buffer start):
//
//   BufHeader (8 bytes): num_rows:u32, num_cols:u32
//   ColDescriptor[num_cols] (20 bytes each):
//     type:u32, null_offset:u32, offsets_offset:u32, data_offset:u32, data_size:u32
//   Data blocks at the described offsets.
//
//   type bits: ColType (0-7) | IS_CONST (0x80) if ColumnConst
//
//   COL_BYTES     (0): start-based u32 offsets[rows+1] + chars data (with null terms)
//   COL_NULL_BYTES(1): null_map[rows] + same as COL_BYTES
//   COL_FIXED8    (2): u8[rows]
//   COL_NULL_FIXED8(3): null_map[rows] + u8[rows]
//   COL_FIXED64   (6): u64/f64[rows]
//   COL_NULL_FIXED64(7): null_map[rows] + u64/f64[rows]
//
// The WASM export is <function_name>_col(i32 buf_handle, i32 num_rows) -> i32.
// The caller (CH) allocates the input buffer with clickhouse_create_buffer,
// fills it, then invokes the function.  The function returns a handle to an
// output buffer (same layout, 1 column) which CH reads and frees.
// ─────────────────────────────────────────────────────────────────────────────

namespace
{

constexpr uint32_t COL_BYTES        = 0;
constexpr uint32_t COL_NULL_BYTES   = 1;
constexpr uint32_t COL_FIXED8       = 2;
constexpr uint32_t COL_NULL_FIXED8  = 3;
constexpr uint32_t COL_FIXED32      = 4;
constexpr uint32_t COL_NULL_FIXED32 = 5;
constexpr uint32_t COL_FIXED64      = 6;
constexpr uint32_t COL_NULL_FIXED64 = 7;
constexpr uint32_t COL_IS_CONST     = 0x80u;

constexpr uint32_t COLUMNAR_HEADER_BYTES  = 8;
constexpr uint32_t COLUMNAR_DESC_BYTES    = 20;

struct ColDescriptor
{
    uint32_t type;
    uint32_t null_offset;
    uint32_t offsets_offset;
    uint32_t data_offset;
    uint32_t data_size;
};
static_assert(sizeof(ColDescriptor) == COLUMNAR_DESC_BYTES);

// Compute the byte offset where column i's data blocks start and fill in desc.
// Returns the offset after all data for this column (= start of next column's data).
uint32_t buildColDescriptor(
    const IColumn * col,       // already stripped of ColumnConst
    bool is_const,
    bool is_nullable,
    uint32_t num_rows,         // logical row count (1 if const)
    uint32_t write_cursor,     // current byte offset in output buffer
    ColDescriptor & desc)
{
    const ColumnString * str_col = typeid_cast<const ColumnString *>(col);
    const ColumnNullable * null_col = typeid_cast<const ColumnNullable *>(col);

    if (null_col)
        str_col = typeid_cast<const ColumnString *>(&null_col->getNestedColumn());

    if (str_col)
    {
        uint32_t base_type = is_nullable ? COL_NULL_BYTES : COL_BYTES;
        desc.type = base_type | (is_const ? COL_IS_CONST : 0u);

        // null_map
        if (is_nullable)
        {
            desc.null_offset = write_cursor;
            write_cursor += num_rows;
        }
        else
        {
            desc.null_offset = 0;
        }

        // offsets: align to 4
        write_cursor = (write_cursor + 3u) & ~3u;
        desc.offsets_offset = write_cursor;
        write_cursor += (num_rows + 1u) * sizeof(uint32_t);

        // data — ColumnString in CH 26.4+ has no null terminators; we add one per string
        // in the wire format so WASM can use the get_bytes(-1) formula unchanged.
        desc.data_offset = write_cursor;
        uint32_t total_chars = static_cast<uint32_t>(str_col->getChars().size()) + num_rows;
        desc.data_size = total_chars;
        write_cursor += total_chars;
        return write_cursor;
    }

    // Fixed-width column: use element size
    uint32_t elem_size = static_cast<uint32_t>(col->sizeOfValueIfFixed());
    uint32_t base_type;
    if      (elem_size == 1) base_type = is_nullable ? COL_NULL_FIXED8  : COL_FIXED8;
    else if (elem_size == 4) base_type = is_nullable ? COL_NULL_FIXED32 : COL_FIXED32;
    else                     base_type = is_nullable ? COL_NULL_FIXED64 : COL_FIXED64;
    desc.type = base_type | (is_const ? COL_IS_CONST : 0u);
    desc.null_offset = 0; // TODO: nullable fixed columns if needed
    desc.offsets_offset = 0;
    desc.data_offset = write_cursor;
    desc.data_size = num_rows * elem_size;
    write_cursor += num_rows * elem_size;
    return write_cursor;
}

// Write column data into the WASM buffer span.
void writeColData(
    const IColumn * col,
    bool is_nullable,
    uint32_t num_rows,
    const ColDescriptor & desc,
    std::span<uint8_t> buf)
{
    const ColumnNullable * null_col = typeid_cast<const ColumnNullable *>(col);
    if (null_col)
        col = &null_col->getNestedColumn();

    // null_map
    if (is_nullable && null_col && desc.null_offset)
    {
        const auto & nm = null_col->getNullMapData();
        std::memcpy(buf.data() + desc.null_offset, nm.data(), num_rows);
    }

    const ColumnString * str_col = typeid_cast<const ColumnString *>(col);
    if (str_col)
    {
        // CH 26.4+ ColumnString has NO null terminators; offsets[i] = cumulative byte count.
        // Wire format requires null terminators so WASM get_bytes() formula (end-start-1) works.
        // Write each string followed by an explicit '\0', build cumulative wire offsets.
        const auto & ch_offsets = str_col->getOffsets();
        const auto & chars = str_col->getChars();
        uint32_t * wire_offsets = reinterpret_cast<uint32_t *>(buf.data() + desc.offsets_offset);
        uint8_t * data_dst = buf.data() + desc.data_offset;
        wire_offsets[0] = 0;
        uint32_t wire_pos = 0;
        uint32_t ch_pos = 0;
        for (uint32_t i = 0; i < num_rows; ++i)
        {
            uint32_t str_end = static_cast<uint32_t>(ch_offsets[i]);
            uint32_t str_len = str_end - ch_pos;
            std::memcpy(data_dst + wire_pos, chars.data() + ch_pos, str_len);
            wire_pos += str_len;
            data_dst[wire_pos++] = '\0';
            wire_offsets[i + 1] = wire_pos;
            ch_pos = str_end;
        }
        return;
    }

    // Fixed-width: raw byte copy.
    const auto * raw = col->getRawData().data();
    std::memcpy(buf.data() + desc.data_offset, raw, desc.data_size);
}

// Read a single-column columnar output buffer back into a MutableColumnPtr.
MutableColumnPtr readColumnarOutput(std::span<const uint8_t> buf, const DataTypePtr & result_type, size_t expected_rows)
{
    if (buf.size() < COLUMNAR_HEADER_BYTES + COLUMNAR_DESC_BYTES)
        throw Exception(ErrorCodes::WASM_ERROR, "COLUMNAR_V1 output buffer too small: {} bytes", buf.size());

    uint32_t num_rows, num_cols;
    std::memcpy(&num_rows, buf.data(),     4);
    std::memcpy(&num_cols, buf.data() + 4, 4);

    if (num_rows != expected_rows)
        throw Exception(ErrorCodes::WASM_ERROR,
            "COLUMNAR_V1 output row count mismatch: expected {}, got {}", expected_rows, num_rows);
    if (num_cols != 1)
        throw Exception(ErrorCodes::WASM_ERROR,
            "COLUMNAR_V1 output must have exactly 1 column, got {}", num_cols);

    ColDescriptor desc;
    std::memcpy(&desc, buf.data() + COLUMNAR_HEADER_BYTES, sizeof(desc));

    uint32_t raw_type = desc.type & ~COL_IS_CONST;

    // Bytes column → ColumnString
    if (raw_type == COL_BYTES || raw_type == COL_NULL_BYTES)
    {
        const uint32_t * wire_offsets = reinterpret_cast<const uint32_t *>(buf.data() + desc.offsets_offset);
        const uint8_t * data = buf.data() + desc.data_offset;

        auto col_str = ColumnString::create();
        auto & chars = col_str->getChars();
        auto & offsets = col_str->getOffsets();

        // WASM output has null terminators in wire format (ColBytesWriter adds '\0').
        // CH 26.4+ ColumnString has NO null terminators; offsets are cumulative without nulls.
        // Strip the null terminators when building the output column.
        offsets.resize(num_rows);
        uint32_t ch_pos = 0;
        for (uint32_t i = 0; i < num_rows; ++i)
        {
            uint32_t wire_end = wire_offsets[i + 1];           // includes '\0'
            uint32_t wire_start = wire_offsets[i];
            uint32_t str_len = wire_end - wire_start;
            if (str_len > 0) str_len--;                        // strip null terminator
            chars.resize(ch_pos + str_len);
            std::memcpy(chars.data() + ch_pos, data + wire_start, str_len);
            ch_pos += str_len;
            offsets[i] = ch_pos;
        }

        if (raw_type == COL_NULL_BYTES && desc.null_offset)
        {
            auto null_col = ColumnUInt8::create(num_rows);
            std::memcpy(null_col->getData().data(), buf.data() + desc.null_offset, num_rows);
            return ColumnNullable::create(std::move(col_str), std::move(null_col));
        }
        return col_str;
    }

    // Fixed8 → ColumnUInt8
    if (raw_type == COL_FIXED8 || raw_type == COL_NULL_FIXED8)
    {
        auto col_u8 = ColumnUInt8::create(num_rows);
        std::memcpy(col_u8->getData().data(), buf.data() + desc.data_offset, num_rows);
        if (raw_type == COL_NULL_FIXED8 && desc.null_offset)
        {
            auto null_col = ColumnUInt8::create(num_rows);
            std::memcpy(null_col->getData().data(), buf.data() + desc.null_offset, num_rows);
            return ColumnNullable::create(std::move(col_u8), std::move(null_col));
        }
        return col_u8;
    }

    // Fixed64 — create column matching the declared return type (Float64, UInt64, Int64, etc.)
    if (raw_type == COL_FIXED64 || raw_type == COL_NULL_FIXED64)
    {
        const DataTypePtr & base_type = (raw_type == COL_NULL_FIXED64)
            ? dynamic_cast<const DataTypeNullable &>(*result_type).getNestedType()
            : result_type;
        auto col64 = base_type->createColumn();
        col64->insertManyDefaults(num_rows);
        // ColumnVector<T> stores data as a contiguous POD array starting at offset 0.
        // getRawData() returns std::string_view; the column is freshly created (not const),
        // so const_cast on the underlying pointer is safe.
        std::memcpy(const_cast<char *>(col64->getRawData().data()),
                    buf.data() + desc.data_offset, num_rows * 8);
        if (raw_type == COL_NULL_FIXED64 && desc.null_offset)
        {
            auto null_col = ColumnUInt8::create(num_rows);
            std::memcpy(null_col->getData().data(), buf.data() + desc.null_offset, num_rows);
            return ColumnNullable::create(std::move(col64), std::move(null_col));
        }
        return col64;
    }

    throw Exception(ErrorCodes::WASM_ERROR, "COLUMNAR_V1: unsupported output ColType {}", raw_type);
}

} // anonymous namespace

class UserDefinedWebAssemblyFunctionColumnarV1 : public UserDefinedWebAssemblyFunction
{
public:
    template <typename... Args>
    explicit UserDefinedWebAssemblyFunctionColumnarV1(Args &&... args)
        : UserDefinedWebAssemblyFunction(std::forward<Args>(args)...)
    {
        // WASM export name matches the registered function name directly
        col_function_name = function_name;
        checkSignature();
    }

    // Direct columnar execution — bypasses RowBinary batching.
    // Called from FunctionUserDefinedWasm::executeImpl() for ColumnarV1 functions.
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

        // ── Build the columnar input buffer ──────────────────────────────────
        const uint32_t num_cols = static_cast<uint32_t>(cols.size());
        uint32_t cursor = COLUMNAR_HEADER_BYTES + num_cols * COLUMNAR_DESC_BYTES;

        std::vector<ColDescriptor> descs(num_cols);
        std::vector<const IColumn *> inner_cols(num_cols);
        std::vector<bool> is_const_flags(num_cols);
        std::vector<bool> is_nullable_flags(num_cols);
        std::vector<uint32_t> row_counts(num_cols);

        for (uint32_t ci = 0; ci < num_cols; ++ci)
        {
            const IColumn * col = cols[ci].column.get();
            bool is_const = false;

            if (const auto * cc = typeid_cast<const ColumnConst *>(col))
            {
                col = &cc->getDataColumn();
                is_const = true;
            }

            bool is_nullable = typeid_cast<const ColumnNullable *>(col) != nullptr;
            uint32_t nrows = is_const ? 1u : static_cast<uint32_t>(input_rows_count);

            is_const_flags[ci] = is_const;
            is_nullable_flags[ci] = is_nullable;
            inner_cols[ci] = col;
            row_counts[ci] = nrows;

            cursor = buildColDescriptor(col, is_const, is_nullable, nrows, cursor, descs[ci]);
        }

        uint32_t total_buf_size = cursor;

        // ── Allocate buffer in WASM memory ───────────────────────────────────
        {
            ProfileEventTimeIncrement<Microseconds> timer_ser(ProfileEvents::WasmSerializationMicroseconds);

            auto wmm = std::make_unique<WasmMemoryManagerV01>(compartment, stop_token);
            WasmMemoryGuard wasm_input = allocateInWasmMemory(wmm.get(), total_buf_size);
            auto wasm_mem = wasm_input.getMemoryView();

            // Write header
            uint32_t n_rows32 = static_cast<uint32_t>(input_rows_count);
            std::memcpy(wasm_mem.data(),     &n_rows32,  4);
            std::memcpy(wasm_mem.data() + 4, &num_cols,  4);

            // Write descriptors
            for (uint32_t ci = 0; ci < num_cols; ++ci)
                std::memcpy(wasm_mem.data() + COLUMNAR_HEADER_BYTES + ci * COLUMNAR_DESC_BYTES,
                            &descs[ci], COLUMNAR_DESC_BYTES);

            // Write column data
            for (uint32_t ci = 0; ci < num_cols; ++ci)
                writeColData(inner_cols[ci], is_nullable_flags[ci], row_counts[ci],
                             descs[ci], wasm_mem);

            // ── Invoke WASM ──────────────────────────────────────────────────
            auto result_ptr = compartment->invoke<WasmPtr>(
                col_function_name,
                {wasm_input.getHandle(), static_cast<WasmSizeT>(input_rows_count)},
                stop_token);

            if (result_ptr == 0)
                throw Exception(ErrorCodes::WASM_ERROR,
                    "COLUMNAR_V1 function '{}' returned nullptr", col_function_name);

            WasmMemoryGuard result_guard(wmm.get(), result_ptr);

            // ── Read output ──────────────────────────────────────────────────
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

    // executeOnBlock is required by the base class but unused for ColumnarV1
    // (FunctionUserDefinedWasm calls executeColumnar directly).
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

private:
    void checkSignature() const
    {
        auto decl = wasm_module->getExport(col_function_name);
        WasmFunctionDeclaration expected("", col_function_name,
            {WasmValKind::I32, WasmValKind::I32}, WasmValKind::I32);
        checkFunctionDeclarationMatches(decl, expected);
        // Also require clickhouse_create_buffer / clickhouse_destroy_buffer
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
    }

    String getName() const override { return function_name; }
    bool isVariadic() const override { return false; }
    bool isDeterministic() const override { return user_defined_function->getIsDeterministic(); }
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
        auto compartment_entry = compartment_pool.acquire();
        auto * compartment_ptr = &(*compartment_entry);
        try
        {
            // COLUMNAR_V1: bypass RowBinary batching, pass columns directly (ColumnConst stays const).
            if (const auto * cv1 = dynamic_cast<const UserDefinedWebAssemblyFunctionColumnarV1 *>(user_defined_function.get()))
            {
                auto stop_token = interrupt_source.get_token();
                return cv1->executeColumnar(compartment_ptr, arguments, input_rows_count, context, stop_token);
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
    ColumnPtr execute(WebAssembly::WasmCompartment * compartment, const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        MutableColumnPtr result_column = user_defined_function->getResultType()->createColumn();
        size_t block_size = context->getSettingsRef()[Setting::webassembly_udf_max_input_block_size];
        if (block_size == 0)
            block_size = input_rows_count;

        for (size_t start_idx = 0; start_idx < input_rows_count; start_idx += block_size)
        {
            size_t current_block_size = std::min(block_size, input_rows_count - start_idx);
            auto current_input_block = getArgumentsBlock(arguments, start_idx, current_block_size);
            auto stop_token = interrupt_source.get_token();
            auto current_column = user_defined_function->executeOnBlock(compartment, current_input_block, context, current_block_size, stop_token);

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
        const auto & declared_arguments = user_defined_function->getArguments();
        Block arguments_block;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            ColumnPtr column = arguments[i].column->convertToFullColumnIfConst()->cut(start_idx, length);
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

    mutable StopSource interrupt_source;
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

    std::unique_lock lock(registry_mutex);
    registry[function_def.function_name] = RegistryEntry{wasm_func, create_function_query};
    return wasm_func;
}

bool UserDefinedWebAssemblyFunctionFactory::has(const String & function_name) const
{
    std::shared_lock lock(registry_mutex);
    return registry.contains(function_name);
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
    };

    Strings getAllRegisteredNames() const override
    {
        Strings result;
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
