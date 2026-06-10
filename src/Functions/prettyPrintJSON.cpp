#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Common/AllocationInterceptors.h>
#include <Common/CurrentMemoryTracker.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include "config.h"

#if USE_RAPIDJSON

/// Prevent stack overflow:
#define RAPIDJSON_PARSE_DEFAULT_FLAGS (kParseIterativeFlag)

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <rapidjson/reader.h>
#include <rapidjson/memorystream.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/error/en.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Allocator for rapidjson `StringBuffer` that routes the output-buffer growth
/// through ClickHouse's `MemoryTracker` so it counts against `max_memory_usage`.
///
/// The rapidjson default `CrtAllocator` bypasses the `MemoryTracker`, so a
/// deeply nested JSON combined with a non-zero `indent` produces O(N^2) output
/// that can grow to GiB invisibly to `max_memory_usage`, letting the process be
/// OOM-killed instead of returning a ClickHouse exception. Accounting the buffer
/// through the throwing `CurrentMemoryTracker::alloc` path *before* the raw
/// allocation turns that into a `MEMORY_LIMIT_EXCEEDED` exception bounded by the
/// query memory budget.
///
/// rapidjson's `Allocator::Free` is static and receives no size, so the actual
/// allocation size is stored in a small header prefixed to every block.
class MemoryTrackedAllocator
{
public:
    /// rapidjson's Allocator concept requires a static `kNeedFree` flag and a
    /// default-constructible allocator (its `internal::Stack` default-constructs
    /// one for an `ownAllocator_` fallback path we never reach, since we always
    /// pass an explicit allocator). The implicit default constructor satisfies
    /// this because the allocator is stateless.
    [[maybe_unused]] static const bool kNeedFree = true;

    [[maybe_unused]] void * Malloc(size_t size)
    {
        if (size == 0)
            return nullptr;
        return allocateTracked(size);
    }

    void * Realloc(void * original_ptr, size_t /*original_size*/, size_t new_size)
    {
        if (new_size == 0)
        {
            freeTracked(original_ptr);
            return nullptr;
        }

        if (original_ptr == nullptr)
            return allocateTracked(new_size);

        /// rapidjson passes the *logical* old size, but the tracked size is read
        /// from the header so accounting stays correct regardless.
        Header old_header = readHeader(original_ptr);
        size_t old_total = old_header.allocated_size + sizeof(Header);
        size_t new_total = new_size + sizeof(Header);
        void * old_base = headerBase(original_ptr);

        /// Reserve the new size first; `realloc` may move or grow in place, so
        /// account for the worst case before the raw call, then release the old
        /// reservation once it succeeds (mirrors `Allocator::realloc`).
        auto trace_alloc = CurrentMemoryTracker::alloc(static_cast<Int64>(new_total));

        /// `__real_realloc` bypasses the C allocation interceptors so the buffer
        /// is accounted only once, through the explicit `CurrentMemoryTracker`
        /// calls above (mirrors `Allocator` / `AllocatorWithMemoryTracking`).
        void * new_base = __real_realloc(old_base, new_total);
        if (new_base == nullptr)
        {
            [[maybe_unused]] auto rollback = CurrentMemoryTracker::free(static_cast<Int64>(new_total));
            throw Exception(
                ErrorCodes::CANNOT_ALLOCATE_MEMORY,
                "Cannot reallocate {} bytes for the output buffer of function prettyPrintJSON",
                new_total);
        }

        auto trace_free = CurrentMemoryTracker::free(static_cast<Int64>(old_total));
        void * new_ptr = writeHeader(new_base, new_size);
        trace_free.onFree(old_base, old_total);
        trace_alloc.onAlloc(new_base, new_total);
        return new_ptr;
    }

    static void Free(void * ptr) noexcept
    {
        freeTracked(ptr);
    }

private:
    /// Stored in front of every block so the sizeless static `Free` and the
    /// `Realloc` shrink/grow accounting know how much was reserved from the
    /// `MemoryTracker`. Sized to keep the returned pointer suitably aligned.
    struct alignas(std::max_align_t) Header
    {
        size_t allocated_size;
    };

    static void * headerBase(void * user_ptr) noexcept
    {
        return static_cast<char *>(user_ptr) - sizeof(Header);
    }

    static Header readHeader(void * user_ptr) noexcept
    {
        Header header{};
        std::memcpy(&header, headerBase(user_ptr), sizeof(Header));
        return header;
    }

    static void * writeHeader(void * base, size_t allocated_size) noexcept
    {
        Header header{allocated_size};
        std::memcpy(base, &header, sizeof(Header));
        return static_cast<char *>(base) + sizeof(Header);
    }

    static void * allocateTracked(size_t size)
    {
        size_t total = size + sizeof(Header);
        auto trace = CurrentMemoryTracker::alloc(static_cast<Int64>(total));

        void * base = __real_malloc(total);
        if (base == nullptr)
        {
            [[maybe_unused]] auto rollback = CurrentMemoryTracker::free(static_cast<Int64>(total));
            throw Exception(
                ErrorCodes::CANNOT_ALLOCATE_MEMORY,
                "Cannot allocate {} bytes for the output buffer of function prettyPrintJSON",
                total);
        }

        void * user_ptr = writeHeader(base, size);
        trace.onAlloc(base, total);
        return user_ptr;
    }

    static void freeTracked(void * user_ptr) noexcept
    {
        if (user_ptr == nullptr)
            return;

        Header header = readHeader(user_ptr);
        size_t total = header.allocated_size + sizeof(Header);
        void * base = headerBase(user_ptr);

        __real_free(base);
        auto trace = CurrentMemoryTracker::free(static_cast<Int64>(total));
        trace.onFree(base, total);
    }
};

using MemoryTrackedStringBuffer = rapidjson::GenericStringBuffer<rapidjson::UTF8<>, MemoryTrackedAllocator>;

class FunctionPrettyPrintJSON : public IFunction
{
public:
    static constexpr auto name = "prettyPrintJSON";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPrettyPrintJSON>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"json", &isString, nullptr, "String"}
        };
        FunctionArgumentDescriptors optional_args{
            {"indent", &isNativeUInt, isColumnConst, "const UInt*"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a String column", getName());

        unsigned indent_count = 4;
        if (arguments.size() > 1)
        {
            UInt64 indent_value = assert_cast<const ColumnConst &>(*arguments[1].column).getValue<UInt64>();
            if (indent_value > 32)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Indent parameter of function {} must be between 0 and 32, got {}",
                    getName(),
                    indent_value);
            indent_count = static_cast<unsigned>(indent_value);
        }

        auto result = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto str_view = col->getDataAt(i);

            /// Since RapidJSON uses '\0' as end-of-stream char in its stream abstraction,
            /// we have to default to this check to prevent silent truncation of the input.
            /// Unescaped '\0' is not valid in JSON strings anyway.
            if (str_view.find('\0') != std::string_view::npos)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid JSON string in function {}: embedded NULL byte",
                    getName());

            MemoryTrackedAllocator buffer_allocator;
            MemoryTrackedStringBuffer buffer(&buffer_allocator);
            rapidjson::PrettyWriter<MemoryTrackedStringBuffer> writer(buffer);
            writer.SetIndent(' ', indent_count);

            /// Stream JSON directly from `Reader` to `PrettyWriter` without building
            /// a DOM. Both `Reader` (with `kParseIterativeFlag`) and `PrettyWriter`
            /// use heap-allocated stacks, so arbitrarily deep nesting is safe.
            /// The `MemoryTrackedAllocator` accounts the output buffer in the query
            /// `MemoryTracker`, so O(N^2) blowup on deeply nested input with a
            /// non-zero `indent` throws `MEMORY_LIMIT_EXCEEDED` instead of letting
            /// the process be OOM-killed.
            rapidjson::MemoryStream ms(str_view.data(), str_view.size());
            rapidjson::Reader reader;
            auto parse_result = reader.Parse(ms, writer);

            if (parse_result.IsError())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid JSON string in function {}: {}",
                    getName(),
                    rapidjson::GetParseError_En(parse_result.Code()));

            result->insertData(buffer.GetString(), buffer.GetSize());
        }

        return result;
    }
};

}

REGISTER_FUNCTION(PrettyPrintJSON)
{
    FunctionDocumentation::Description description = R"(
Returns a pretty-printed version of a JSON string with newlines and indentation with spaces.
    )";
    FunctionDocumentation::Syntax syntax = "prettyPrintJSON(json [, indent])";
    FunctionDocumentation::Arguments arguments = {
        {"json", "A valid JSON string to format.", {"String"}},
        {"indent", "Number of spaces per indentation level. Default: 4. Max: 32", {"UInt*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"A pretty-printed JSON string.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {
            "Simple object",
            R"(SELECT prettyPrintJSON('{"a":1,"b":"hello"}');)",
            R"(
{
    "a": 1,
    "b": "hello"
}
            )"
        },
        {
            "Custom indent",
            R"(SELECT prettyPrintJSON('{"a":1}', 8);)",
            R"(
{
        "a": 1
}
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::JSON;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPrettyPrintJSON>(documentation);
}

}

#endif
