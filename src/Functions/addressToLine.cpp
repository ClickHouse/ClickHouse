#if defined(__ELF__) && !defined(OS_FREEBSD)

#include <Common/Dwarf.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteBufferFromArena.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Access/Common/AccessFlags.h>

#include <Functions/addressToLine.h>


namespace DB
{

namespace
{

class FunctionAddressToLine : public FunctionAddressToLineBase<StringRef, Dwarf::LocationInfoMode::FAST>
{
public:
    static constexpr auto name = "addressToLine";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr context)
    {
        context->checkAccess(AccessType::addressToLine);
        return std::make_shared<FunctionAddressToLine>();
    }
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
            StringRef res_str = implCached(data[i]);
            result_column->insertData(res_str.data, res_str.size);
        }
        return result_column;
    }

    void setResult(StringRef & result, const Dwarf::LocationInfo & location, const std::vector<Dwarf::SymbolizedFrame> &) const override
    {
        const char * arena_begin = nullptr;
        WriteBufferFromArena out(cache.arena, arena_begin);

        writeString(location.file.toString(), out);
        writeChar(':', out);
        writeIntText(location.line, out);

        out.finalize();
        result = out.complete();
    }
};

}

REGISTER_FUNCTION(AddressToLine)
{
    FunctionDocumentation::Description description = R"(
Converts a virtual memory address inside the ClickHouse server process to a filename and line number in ClickHouse's source code.

:::note
This function is slow and may impose security considerations.
:::

To enable this introspection function:

- Install the `clickhouse-common-static-dbg` package.
- Set setting [`allow_introspection_functions`](../../operations/settings/settings.md#allow_introspection_functions) to `1`.
    )";
    FunctionDocumentation::Syntax syntax = "addressToLine(address_of_binary_instruction)";
    FunctionDocumentation::Arguments arguments = {
        {"address_of_binary_instruction", "Address of instruction in a running process.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a source code filename and line number delimited by a colon, for example, `/build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199`. Returns the name of a binary, if no debug information could be found, otherwise an empty string, if the address is not valid.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Selecting the first string from the `trace_log` system table",
        R"(
SET allow_introspection_functions=1;
SELECT * FROM system.trace_log LIMIT 1 \G;
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
trace:                   [140658411141617,94784174532828,94784076370703,94784076372094,94784076361020,94784175007680,140658411116251,140658403895439]
        )"
    },
    {
        "Getting the source code filename and the line number for a single address",
        R"(
SET allow_introspection_functions=1;
SELECT addressToLine(94784076370703) \G;
        )",
        R"(
Row 1:
──────
addressToLine(94784076370703): /build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199
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
\G
        )",
        R"(
Row 1:
──────
trace_source_code_lines: /lib/x86_64-linux-gnu/libpthread-2.27.so
/usr/lib/debug/usr/bin/clickhouse
/build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.cpp:199
/build/obj-x86_64-linux-gnu/../src/Common/ThreadPool.h:155
/usr/include/c++/9/bits/atomic_base.h:551
/usr/lib/debug/usr/bin/clickhouse
/lib/x86_64-linux-gnu/libpthread-2.27.so
/build/glibc-OTsEL5/glibc-2.27/misc/../sysdeps/unix/sysv/linux/x86_64/clone.S:97
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Introspection;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAddressToLine>(documentation);
}

}

#endif
