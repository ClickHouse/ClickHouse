#if (defined(__ELF__) && !defined(OS_FREEBSD)) || defined(OS_DARWIN)

#include <Common/Dwarf.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteBufferFromArena.h>
#include <IO/WriteHelpers.h>
#include <Functions/FunctionAccessCheck.h>
#include <Access/Common/AccessFlags.h>

#include <Functions/addressToLine.h>


namespace DB
{

namespace
{

class FunctionAddressToLine final : public FunctionAddressToLineBase<std::string_view, Dwarf::LocationInfoMode::FAST>
{
public:
    static constexpr auto name = "addressToLine";
    String getName() const override { return name; }
    String getSignatureString() const override { return "(UInt64) -> String"; }
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionAddressToLine>(context);
    }
    explicit FunctionAddressToLine(const ContextPtr & context) : access_check(context) {}
    void checkAccessOnBuild() const override { access_check.check(AccessType::addressToLine); }
protected:
    DataTypePtr getDataType() const override
    {
        return std::make_shared<DataTypeString>();
    }
    ColumnPtr getResultColumn(const typename ColumnVector<UInt64>::Container & data, size_t input_rows_count) const override
    {
        auto result_column = ColumnString::create();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view res_str = implCached(data[i]);
            result_column->insertData(res_str.data(), res_str.size());
        }
        return result_column;
    }

    void setResult(std::string_view & result, const Dwarf::LocationInfo & location, const VectorWithMemoryTracking<Dwarf::SymbolizedFrame> &) const override
    {
        const char * arena_begin = nullptr;
        WriteBufferFromArena out(cache.arena, arena_begin);

        writeString(location.file.toString(), out);
        writeChar(':', out);
        writeIntText(location.line, out);

        out.finalize();
        result = out.complete();
    }

private:
    FunctionAccessCheck access_check;
};

}

REGISTER_FUNCTION(AddressToLine)
{
    FunctionDocumentation::Description description = R"(
Converts an address of an instruction inside the main ClickHouse executable to a filename and line number in ClickHouse's source code.

The address must be an offset inside the main ClickHouse executable, the form in which addresses are stored in the `trace` columns of `system.trace_log`, `system.stack_trace`, and `system.crash_log`, and the `last_error_trace` column of `system.error_log`.
Runtime (virtual memory) addresses are not accepted, so that the function cannot be used to learn the memory layout of the server process.
Frames outside the main executable are stored as `0`, and the function returns an empty result for them.

<Note>
This function is slow and may impose security considerations.
</Note>

To enable this introspection function:

- Install the `clickhouse-common-static-dbg` package.
- Set setting [`allow_introspection_functions`](/reference/settings/session-settings/allow#allow_introspection_functions) to `1`.
    )";
    FunctionDocumentation::Syntax syntax = "addressToLine(address_of_binary_instruction)";
    FunctionDocumentation::Arguments arguments = {
        {"address_of_binary_instruction", "Offset of an instruction inside the main ClickHouse executable, as stored in the `trace` column of `system.trace_log` and similar tables.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a source code filename and line number delimited by a colon, for example, `/build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199`. Returns the name of the binary, if no debug information could be found for the address, or an empty string for `0`.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Selecting the first string from the `trace_log` system table",
        R"(
SET allow_introspection_functions=1;
SELECT * FROM system.trace_log LIMIT 1 FORMAT Vertical;
        )",
        R"(
-- The `trace` field contains the stack trace at the moment of sampling.
Row 1:
──────
event_date:              2019-11-19
event_time:              2019-11-19 18:57:23
revision:                54429
timer_type:              Real
thread_number:           48
query_id:                421b6855-1858-45a5-8f37-f383409d6d72
trace:                   [0,0,0,0,631010292,205306916,447260096,447257472,447067860,447068840,443102728,443098700,442466608,442461536,442451604,442504896,442512004,442579276,290968008,555786732,291036904,205189920,0,0]
        )"
    },
    {
        "Getting the source code filename and the line number for a single address",
        R"(
SET allow_introspection_functions=1;
SELECT addressToLine(447257472::UInt64) AS source_line FORMAT Vertical;
        )",
        R"(
Row 1:
──────
source_line: src/Processors/Formats/LazyOutputFormat.cpp:25
        )"
    },
    {
        "Applying the function to the whole stack trace",
        R"(
-- The arrayMap function in this example processing each individual element of the trace array by the addressToLine function.
-- The result of this processing is seen in the trace_source_code_lines column of output.

SELECT
    arrayStringConcat(arrayMap(x -> addressToLine(x), trace), '\n') AS trace_source_code_lines
FROM system.trace_log
LIMIT 1
FORMAT Vertical;
        )",
        R"(
Row 1:
──────
trace_source_code_lines:


contrib/llvm-project/libcxx/src/condition_variable.cpp:83
contrib/llvm-project/libcxx/include/__condition_variable/condition_variable.h:165
contrib/llvm-project/libcxx/include/__condition_variable/condition_variable.h:135
src/Processors/Formats/LazyOutputFormat.cpp:25
src/Processors/Executors/PullingAsyncPipelineExecutor.cpp:177
src/Processors/Executors/PullingAsyncPipelineExecutor.cpp:190
contrib/llvm-project/libcxx/include/__vector/vector.h:394
src/Client/LocalConnection.cpp:786
src/Client/ClientBase.cpp:2091
contrib/boost/boost/smart_ptr/intrusive_ptr.hpp:76
contrib/boost/boost/smart_ptr/intrusive_ptr.hpp:76
contrib/boost/boost/smart_ptr/intrusive_ptr.hpp:76
src/Client/ClientBase.cpp:0
src/Client/ClientBase.cpp:5507
programs/local/LocalServer.cpp:1541
base/poco/Util/src/Application.cpp:0
programs/local/LocalServer.cpp:0
contrib/llvm-project/libcxx/include/__vector/vector.h:248


        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Introspection;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAddressToLine>(documentation);
}

}

#endif
