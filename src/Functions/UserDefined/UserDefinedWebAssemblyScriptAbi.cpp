#include <Functions/UserDefined/UserDefinedWebAssemblyScriptAbi.h>
#include <Functions/UserDefined/UserDefinedWebAssemblyTypeHelpers.h>

#include <bit>
#include <cstring>
#include <utility>

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Common/Exception.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/ProfileEvents.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/logger_useful.h>

#include <base/scope_guard.h>

#include <Interpreters/WebAssembly/WasmEngine.h>
#include <Interpreters/WebAssembly/WasmTypes.h>


namespace ProfileEvents
{
extern const Event WasmTotalExecuteMicroseconds;
}

namespace DB
{

using namespace WebAssembly;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int WASM_ERROR;
}

namespace
{

/// Convert UTF-8 to UTF-16LE in place. Writes at most `4 * utf8.size()` bytes (each input
/// byte becomes at most one surrogate pair = 4 output bytes). Returns the number of UTF-16
/// code units actually written. Invalid sequences emit U+FFFD.
size_t convertUTF8ToUTF16LE(std::string_view utf8, uint8_t * out)
{
    const auto * data = reinterpret_cast<const uint8_t *>(utf8.data());
    size_t n = utf8.size();
    size_t i = 0;
    size_t out_units = 0;

    while (i < n)
    {
        uint32_t cp = 0;
        uint8_t b = data[i];

        if (b < 0x80)
        {
            cp = b;
            i += 1;
        }
        else if ((b & 0xE0) == 0xC0 && i + 1 < n && (data[i + 1] & 0xC0) == 0x80)
        {
            cp = (uint32_t(b & 0x1F) << 6) | uint32_t(data[i + 1] & 0x3F);
            /// Reject overlong: cp < 0x80 should have used the 1-byte form.
            if (cp < 0x80)
                cp = 0xFFFD;
            i += 2;
        }
        else if ((b & 0xF0) == 0xE0 && i + 2 < n
                 && (data[i + 1] & 0xC0) == 0x80 && (data[i + 2] & 0xC0) == 0x80)
        {
            cp = (uint32_t(b & 0x0F) << 12)
                | (uint32_t(data[i + 1] & 0x3F) << 6)
                | uint32_t(data[i + 2] & 0x3F);
            /// Reject overlong (cp < 0x800, should have used a shorter form) and surrogates
            /// (U+D800..U+DFFF are forbidden as Unicode scalars per RFC 3629 §3).
            if (cp < 0x800 || (cp >= 0xD800 && cp <= 0xDFFF))
                cp = 0xFFFD;
            i += 3;
        }
        else if ((b & 0xF8) == 0xF0 && i + 3 < n
                 && (data[i + 1] & 0xC0) == 0x80
                 && (data[i + 2] & 0xC0) == 0x80
                 && (data[i + 3] & 0xC0) == 0x80)
        {
            cp = (uint32_t(b & 0x07) << 18)
                | (uint32_t(data[i + 1] & 0x3F) << 12)
                | (uint32_t(data[i + 2] & 0x3F) << 6)
                | uint32_t(data[i + 3] & 0x3F);
            /// Reject overlong (cp < 0x10000) and out-of-range (Unicode tops out at U+10FFFF).
            if (cp < 0x10000 || cp > 0x10FFFF)
                cp = 0xFFFD;
            i += 4;
        }
        else
        {
            cp = 0xFFFD;
            i += 1;
        }
        if (cp < 0x10000)
        {
            storeToWasmMemory<uint16_t>(out + out_units * 2, static_cast<uint16_t>(cp));
            out_units += 1;
        }
        else
        {
            cp -= 0x10000;
            storeToWasmMemory<uint16_t>(out + out_units * 2, static_cast<uint16_t>(0xD800 | (cp >> 10)));
            storeToWasmMemory<uint16_t>(out + (out_units + 1) * 2, static_cast<uint16_t>(0xDC00 | (cp & 0x3FF)));
            out_units += 2;
        }
    }

    return out_units;
}

/// Decode `byte_count` bytes of UTF-16LE from `le_bytes` and append the UTF-8 transcoding
/// to `out`. Lone surrogates are replaced with U+FFFD.
void appendUTF16LEAsUTF8(const uint8_t * le_bytes, size_t byte_count, std::string & out)
{
    size_t num_units = byte_count / 2;
    out.reserve(out.size() + byte_count); /// upper bound: every BMP unit is at most 3 UTF-8 bytes

    for (size_t i = 0; i < num_units; ++i)
    {
        uint32_t cp = loadFromWasmMemory<uint16_t>(le_bytes + i * 2);
        if (cp >= 0xD800 && cp <= 0xDBFF)
        {
            /// High surrogate. Need a following low surrogate to form a valid pair;
            /// otherwise (no next unit, or next is not a low surrogate) substitute U+FFFD.
            if (i + 1 < num_units)
            {
                uint16_t next = loadFromWasmMemory<uint16_t>(le_bytes + (i + 1) * 2);
                if (next >= 0xDC00 && next <= 0xDFFF)
                {
                    cp = 0x10000 + (((cp - 0xD800) << 10) | (next - 0xDC00));
                    ++i;
                }
                else
                {
                    cp = 0xFFFD;
                }
            }
            else
            {
                cp = 0xFFFD;
            }
        }
        else if (cp >= 0xDC00 && cp <= 0xDFFF)
        {
            cp = 0xFFFD;
        }

        if (cp < 0x80)
        {
            out.push_back(static_cast<char>(cp));
        }
        else if (cp < 0x800)
        {
            out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        }
        else if (cp < 0x10000)
        {
            out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        }
        else
        {
            out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        }
    }
}

/// Thin wrapper around AssemblyScript's managed runtime inside a WASM compartment.
///
/// User-defined class ids are not stable
/// (https://github.com/AssemblyScript/assemblyscript/issues/2982), so this ABI only
/// exposes `String` and primitive types to ClickHouse.
///
/// Object lifetime: AS objects are GC-managed. Argument `String` objects are allocated
/// one at a time by the host and live in `wasm_args` until the user function is invoked.
/// Between two `__new` calls the AS runtime cannot see the previous pointer as a root,
/// so a later allocation can collect it. The host must therefore `__pin` every freshly
/// allocated argument object and `__unpin` it after the call (guaranteed cleanup).
/// The returned object does not need pinning: nothing between the call's return and
/// `readStringInto` allocates, so it cannot be collected before being read.
class AssemblyScriptRuntime
{
public:
    static constexpr uint32_t string_rt_id = 2;

    /// Standard runtime exports produced by `asc --exportRuntime`.
    static constexpr std::string_view new_function_name = "__new";
    static constexpr std::string_view pin_function_name = "__pin";
    static constexpr std::string_view unpin_function_name = "__unpin";

    /// Header offsets relative to the payload pointer.
    static constexpr WasmSizeT header_offset_rt_size = 4;
    static constexpr WasmSizeT header_offset_rt_id = 8;

    static WasmFunctionDeclaration newFunctionDeclaration()
    {
        return {"", new_function_name, {WasmValKind::I32, WasmValKind::I32}, WasmValKind::I32};
    }
    static WasmFunctionDeclaration pinFunctionDeclaration()
    {
        return {"", pin_function_name, {WasmValKind::I32}, WasmValKind::I32};
    }
    static WasmFunctionDeclaration unpinFunctionDeclaration()
    {
        return {"", unpin_function_name, {WasmValKind::I32}, std::nullopt};
    }

    AssemblyScriptRuntime(WasmCompartment * compartment_, StopToken stop_token_)
        : compartment(compartment_), stop_token(std::move(stop_token_))
    {
    }

    /// `__new(payload_size, class_id)`. Returns a pointer to the payload.
    WasmPtr allocateObject(WasmSizeT payload_size, uint32_t class_id) const
    {
        return compartment->invoke<WasmPtr>(
            new_function_name, {payload_size, class_id}, stop_token);
    }

    void pinObject(WasmPtr ptr)
    {
        compartment->invoke<WasmPtr>(pin_function_name, {ptr}, stop_token);
    }

    void unpinObject(WasmPtr ptr)
    {
        compartment->invoke<void>(unpin_function_name, {ptr}, stop_token);
    }

    /// Read `(rtId, rtSize)` from the object header at `ptr - 8`.
    std::pair<uint32_t, uint32_t> readObjectHeader(WasmPtr ptr) const
    {
        if (ptr == 0)
            throw Exception(ErrorCodes::WASM_ERROR, "AssemblyScript pointer is null");
        if (ptr < header_offset_rt_id)
            throw Exception(ErrorCodes::WASM_ERROR,
                "AssemblyScript pointer {} is too small to fit the runtime header", ptr);

        auto header = compartment->getMemory(ptr - header_offset_rt_id, header_offset_rt_id);
        uint32_t rt_id = loadFromWasmMemory<uint32_t>(header.data());
        uint32_t rt_size = loadFromWasmMemory<uint32_t>(header.data() + sizeof(rt_id));
        return {rt_id, rt_size};
    }

    /// Allocate a fresh AS String from UTF-8 input. Allocates an upper-bound buffer
    /// (each input byte ≤ one surrogate pair = 4 bytes), transcodes UTF-8 → UTF-16
    /// directly into compartment memory, then patches `rtSize` to the actual byte length.
    /// Avoids an intermediate host-side vector at the cost of some over-allocation.
    WasmPtr createString(std::string_view utf8) const
    {
        size_t max_bytes = utf8.size() * 4;
        if (max_bytes > std::numeric_limits<WasmSizeT>::max())
            throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE,
                "AssemblyScript string is too large: {} UTF-8 bytes", utf8.size());

        WasmPtr ptr = allocateObject(static_cast<WasmSizeT>(max_bytes), string_rt_id);
        if (ptr == 0)
            throw Exception(ErrorCodes::WASM_ERROR,
                "AssemblyScript __new returned null for {}-byte String upper bound", max_bytes);

        if (max_bytes > 0)
        {
            auto dest = compartment->getMemory(ptr, static_cast<WasmSizeT>(max_bytes));
            size_t actual_units = convertUTF8ToUTF16LE(utf8, dest.data());
            WasmSizeT actual_bytes = static_cast<WasmSizeT>(actual_units * 2);

            /// Patch rtSize at (ptr - 4); `__new` set it to `max_bytes`.
            auto rt_size_slot = compartment->getMemory(ptr - header_offset_rt_size, sizeof(WasmSizeT));
            storeToWasmMemory<WasmSizeT>(rt_size_slot.data(), actual_bytes);
        }
        return ptr;
    }

    /// Read an AS String at `ptr` and append its UTF-8 transcoding to `out`.
    /// Verifies rtId == 2 and that rtSize is even (UTF-16 byte length).
    void readStringInto(WasmPtr ptr, ColumnString & out) const
    {
        auto [rt_id, rt_size] = readObjectHeader(ptr);
        if (rt_id != string_rt_id)
            throw Exception(ErrorCodes::WASM_ERROR,
                "AssemblyScript function returned an object with rtId {} (expected String rtId {})",
                rt_id, string_rt_id);
        if (rt_size % 2 != 0)
            throw Exception(ErrorCodes::WASM_ERROR,
                "AssemblyScript String has odd byte length {} (must be a multiple of 2 for UTF-16)",
                rt_size);

        if (rt_size == 0)
        {
            out.insertData(nullptr, 0);
            return;
        }

        auto src = compartment->getMemory(ptr, rt_size);
        std::string utf8;
        appendUTF16LEAsUTF8(src.data(), rt_size, utf8);
        out.insertData(utf8.data(), utf8.size());
    }

private:
    WasmCompartment * compartment;
    StopToken stop_token;
};

/// Row-by-row ABI for AssemblyScript modules.
///
/// Numeric arguments are passed as AS primitives (i32/i64/f32/f64) the same way as in
/// `RowDirect` (with Int8/UInt8/Int16/UInt16 widened to i32). ClickHouse `String`
/// arguments are transcoded to UTF-16 and allocated inside the WASM heap as
/// AssemblyScript `String` objects (rtId = 2) before each call; the user function
/// declares its parameters as `string`. A `String` return type is read back via the AS
/// object header (rtSize / rtId at offset -4 / -8 from the returned pointer) and
/// transcoded to UTF-8.
///
/// We do not support AssemblyScript `Array<T>` or other custom-class types: their
/// runtime class ids are not stable across AS compilations
/// (https://github.com/AssemblyScript/assemblyscript/issues/2982).
class UserDefinedWebAssemblyFunctionAssemblyScript : public UserDefinedWebAssemblyFunction
{
public:
    template <typename... Args>
    explicit UserDefinedWebAssemblyFunctionAssemblyScript(Args &&... args)
        : UserDefinedWebAssemblyFunction(std::forward<Args>(args)...)
    {
        checkSignature();
    }

    void checkFunction(const WasmFunctionDeclaration & expected) const
    {
        checkFunctionDeclarationMatches(wasm_module->getExport(expected.getName()), expected);
    }

    /// Map a ClickHouse data type to the WASM kind used to pass it across the AS boundary.
    /// `String` is passed as an i32 pointer (to the AS String payload); numeric types reuse
    /// the same widening rules as the simple ABI (Int8/UInt8/Int16/UInt16 → i32).
    static WasmValKind wasmKindForArgument(const IDataType * type)
    {
        if (typeid_cast<const DataTypeString *>(type))
            return WasmValKind::I32;
        auto kind = wasmKindForDataType(type);
        if (!kind)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Type {} is not supported by AssemblyScript ABI; allowed: String and numeric types",
                type->getName());
        return *kind;
    }

    void checkSignature() const
    {
        checkFunction(AssemblyScriptRuntime::newFunctionDeclaration());
        checkFunction(AssemblyScriptRuntime::pinFunctionDeclaration());
        checkFunction(AssemblyScriptRuntime::unpinFunctionDeclaration());

        auto declared = wasm_module->getExport(function_name);

        const auto & wasm_argument_types = declared.getArgumentTypes();
        if (wasm_argument_types.size() != arguments.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "AssemblyScript function '{}' expects {} arguments, got {}",
                function_name, wasm_argument_types.size(), arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            auto expected_kind = wasmKindForArgument(arguments[i].get());
            if (wasm_argument_types[i] != expected_kind)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "AssemblyScript function '{}' argument #{} expected to be {}, got {}",
                    function_name, i, toString(expected_kind), toString(wasm_argument_types[i]));
        }

        auto wasm_return_type = declared.getReturnType();
        if (!wasm_return_type)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "AssemblyScript function '{}' must return a value", function_name);
        auto expected_return_kind = wasmKindForArgument(result_type.get());
        if (*wasm_return_type != expected_return_kind)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "AssemblyScript function '{}' return type expected to be {}, got {}",
                function_name, toString(expected_return_kind), toString(*wasm_return_type));
    }

    MutableColumnPtr executeOnBlock(
        WasmCompartment * compartment, const Block & block, ContextPtr,
        size_t num_rows, StopToken stop_token) const override
    {
        ProfileEventTimeIncrement<Microseconds> timer_execute(ProfileEvents::WasmTotalExecuteMicroseconds);

        AssemblyScriptRuntime as_rt(compartment, stop_token);
        MutableColumnPtr result_column = result_type->createColumn();

        size_t num_columns = block.columns();
        VectorWithMemoryTracking<WasmVal> wasm_args(num_columns);

        VectorWithMemoryTracking<WasmPtr> pinned_args;

        auto * result_string_column = typeid_cast<ColumnString *>(result_column.get());

        auto get_numeric_arg = []<typename T>(const IColumn * column, size_t row_idx, WasmVal & val)
        {
            if (auto * column_typed = checkAndGetColumn<ColumnVector<T>>(column))
            {
                using StorageT = typename WasmStorageType<T>::Type;
                if constexpr (sizeof(T) == sizeof(StorageT))
                    /// Same-width: preserve exact bit pattern (matters for float NaNs).
                    val = std::bit_cast<StorageT>(column_typed->getElement(row_idx));
                else
                    /// Small integers (Int8/UInt8/Int16/UInt16) widen to i32.
                    val = static_cast<StorageT>(column_typed->getElement(row_idx));
                return true;
            }
            return false;
        };

        auto set_numeric_result = [&]<typename T>(const WasmVal & ret_val)
        {
            if (auto * column_typed = typeid_cast<ColumnVector<T> *>(result_column.get()))
            {
                using StorageT = typename WasmStorageType<T>::Type;
                const auto & raw = std::get<StorageT>(ret_val);
                if constexpr (sizeof(T) == sizeof(StorageT))
                    column_typed->insertValue(std::bit_cast<T>(raw));
                else
                    column_typed->insertValue(static_cast<T>(raw));
                return true;
            }
            return false;
        };

        for (size_t row_idx = 0; row_idx < num_rows; ++row_idx)
        {
            pinned_args.clear();

            for (size_t col_idx = 0; col_idx < num_columns; ++col_idx)
            {
                const auto & col = block.getByPosition(col_idx);
                const IColumn * column = col.column.get();

                if (const auto * column_string = checkAndGetColumn<ColumnString>(column))
                {
                    std::string_view str = column_string->getDataAt(row_idx);
                    WasmPtr ptr = as_rt.createString(str);
                    pinned_args.push_back(ptr);
                    as_rt.pinObject(ptr);
                    wasm_args[col_idx] = static_cast<uint32_t>(ptr);
                }
                else if (!tryExecuteForNumericTypes(get_numeric_arg, column, row_idx, wasm_args[col_idx]))
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Cannot pass column of type {} to AssemblyScript function '{}'",
                        col.type->getName(), function_name);
                }
            }

            if (result_string_column)
            {
                auto ret_ptr = compartment->invoke<WasmPtr>(function_name, wasm_args, stop_token);
                as_rt.readStringInto(ret_ptr, *result_string_column);
            }
            else
            {
                /// Pull the result with the kind matching the declared return type, then
                /// route it to the column. checkSignature() already verified the kind.
                auto ret_kind = wasmKindForDataType(result_type.get());
                if (!ret_kind)
                    throw Exception(ErrorCodes::WASM_ERROR,
                        "Unsupported AssemblyScript return type for function '{}': {}",
                        function_name, result_type->getName());

                WasmVal ret_val = [&]() -> WasmVal
                {
                    switch (*ret_kind)
                    {
                        case WasmValKind::I32: return compartment->invoke<uint32_t>(function_name, wasm_args, stop_token);
                        case WasmValKind::I64: return compartment->invoke<int64_t>(function_name, wasm_args, stop_token);
                        case WasmValKind::F32: return compartment->invoke<float>(function_name, wasm_args, stop_token);
                        case WasmValKind::F64: return compartment->invoke<double>(function_name, wasm_args, stop_token);
                        case WasmValKind::V128: return compartment->invoke<Int128>(function_name, wasm_args, stop_token);
                    }
                    UNREACHABLE();
                }();

                if (!tryExecuteForNumericTypes(set_numeric_result, ret_val))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Cannot store AssemblyScript result of kind {} into result column {}",
                        toString(*ret_kind), result_column->getName());
            }

            /// Unpin only on happy path.
            /// if the call threw, the compartment may be in a faulted state where even the pin/unpin calls could also fail,
            /// so just let it leak and expire instead of risking another exception during cleanup.
            for (auto ptr : pinned_args)
                as_rt.unpinObject(ptr);
        }

        return result_column;
    }
};

}

std::unique_ptr<UserDefinedWebAssemblyFunction> createUserDefinedWebAssemblyFunctionAssemblyScript(
    std::shared_ptr<WebAssembly::WasmModule> wasm_module,
    const String & function_name,
    const Strings & argument_names,
    const DataTypes & arguments,
    const DataTypePtr & result_type,
    WebAssemblyFunctionSettings function_settings,
    bool is_deterministic)
{
    return std::make_unique<UserDefinedWebAssemblyFunctionAssemblyScript>(
        std::move(wasm_module),
        function_name,
        argument_names,
        arguments,
        result_type,
        std::move(function_settings),
        is_deterministic);
}

}
