#if defined(__ELF__) && !defined(OS_FREEBSD)

#include <Common/Dwarf.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteBufferFromArena.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Access/Common/AccessFlags.h>

#include <Functions/addressToLine.h>
#include <vector>


namespace DB
{

namespace
{

class FunctionAddressToLineWithInlines: public FunctionAddressToLineBase<StringRefs, Dwarf::LocationInfoMode::FULL_WITH_INLINE>
{
public:
    static constexpr auto name = "addressToLineWithInlines";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr context)
    {
        context->checkAccess(AccessType::addressToLineWithInlines);
        return std::make_shared<FunctionAddressToLineWithInlines>();
    }

protected:
    DataTypePtr getDataType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr getResultColumn(const typename ColumnVector<UInt64>::Container & data, size_t input_rows_count) const override
    {
        auto result_column = ColumnArray::create(ColumnString::create());
        ColumnString & result_strings = typeid_cast<ColumnString &>(result_column->getData());
        ColumnArray::Offsets & result_offsets = result_column->getOffsets();

        ColumnArray::Offset current_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            StringRefs res = implCached(data[i]);
            for (auto & r : res)
                result_strings.insertData(r.data, r.size);
            current_offset += res.size();
            result_offsets.push_back(current_offset);
        }

        return result_column;
    }

    void setResult(StringRefs & result, const Dwarf::LocationInfo & location, const std::vector<Dwarf::SymbolizedFrame> & inline_frames) const override
    {
        appendLocationToResult(result, location, nullptr);
        for (const auto & inline_frame : inline_frames)
            appendLocationToResult(result, inline_frame.location, &inline_frame);
    }

private:
    void appendLocationToResult(StringRefs & result, const Dwarf::LocationInfo & location, const Dwarf::SymbolizedFrame * frame) const
    {
        const char * arena_begin = nullptr;
        WriteBufferFromArena out(cache.arena, arena_begin);

        writeString(location.file.toString(), out);
        writeChar(':', out);
        writeIntText(location.line, out);

        if (frame && frame->name != nullptr)
        {
            writeChar(':', out);
            int status = 0;
            writeString(demangle(frame->name, status), out);
        }

        result.emplace_back(out.complete());
        out.finalize();
    }

};

}

REGISTER_FUNCTION(AddressToLineWithInlines)
{
    FunctionDocumentation::Description description = R"(
Similar to `addressToLine`, but returns an Array with all inline functions.
As a result of this, it is slower than `addressToLine`.

To enable this introspection function:

- Install the `clickhouse-common-static-dbg` package.
- Set setting [`allow_introspection_functions`](../../operations/settings/settings.md#allow_introspection_functions) to `1`.
    )";
    FunctionDocumentation::Syntax syntax = "addressToLineWithInlines(address_of_binary_instruction)";
    FunctionDocumentation::Arguments arguments = {
        {"address_of_binary_instruction", "The address of an instruction in a running process.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array whose first element is the source code filename and line number delimited by a colon. The second, third, etc. element list inline functions' source code filenames, line numbers and function names. If no debug information could be found, then an array with a single element equal to the name of the binary is returned, otherwise an empty array is returned if the address is not valid.", {"Array(String)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Applying the function to an address",
        R"(
SET allow_introspection_functions=1;
SELECT addressToLineWithInlines(531055181::UInt64);
        )",
        R"(
┌─addressToLineWithInlines(CAST('531055181', 'UInt64'))────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ ['./src/Functions/addressToLineWithInlines.cpp:98','./build_normal_debug/./src/Functions/addressToLineWithInlines.cpp:176:DB::(anonymous namespace)::FunctionAddressToLineWithInlines::implCached(unsigned long) const'] │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Applying the function to the whole stack trace",
        R"(
SET allow_introspection_functions=1;

-- The arrayJoin function will split array to rows

SELECT
    ta, addressToLineWithInlines(arrayJoin(trace) AS ta)
FROM system.trace_log
WHERE
    query_id = '5e173544-2020-45de-b645-5deebe2aae54';
        )",
        R"(
┌────────ta─┬─addressToLineWithInlines(arrayJoin(trace))───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ 365497529 │ ['./build_normal_debug/./contrib/libcxx/include/string_view:252']                                                                                                                                                        │
│ 365593602 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:191']                                                                                                                                                                      │
│ 365593866 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365592528 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365591003 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:477']                                                                                                                                                                      │
│ 365590479 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:442']                                                                                                                                                                      │
│ 365590600 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:457']                                                                                                                                                                      │
│ 365598941 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365607098 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365590571 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:451']                                                                                                                                                                      │
│ 365598941 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365607098 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365590571 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:451']                                                                                                                                                                      │
│ 365598941 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365607098 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365590571 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:451']                                                                                                                                                                      │
│ 365598941 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:0']                                                                                                                                                                        │
│ 365597289 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:807']                                                                                                                                                                      │
│ 365599840 │ ['./build_normal_debug/./src/Common/Dwarf.cpp:1118']                                                                                                                                                                     │
│ 531058145 │ ['./build_normal_debug/./src/Functions/addressToLineWithInlines.cpp:152']                                                                                                                                                │
│ 531055181 │ ['./src/Functions/addressToLineWithInlines.cpp:98','./build_normal_debug/./src/Functions/addressToLineWithInlines.cpp:176:DB::(anonymous namespace)::FunctionAddressToLineWithInlines::implCached(unsigned long) const'] │
│ 422333613 │ ['./build_normal_debug/./src/Functions/IFunctionAdaptors.h:21']                                                                                                                                                          │
│ 586866022 │ ['./build_normal_debug/./src/Functions/IFunction.cpp:216']                                                                                                                                                               │
│ 586869053 │ ['./build_normal_debug/./src/Functions/IFunction.cpp:264']                                                                                                                                                               │
│ 586873237 │ ['./build_normal_debug/./src/Functions/IFunction.cpp:334']                                                                                                                                                               │
│ 597901620 │ ['./build_normal_debug/./src/Interpreters/ExpressionActions.cpp:601']                                                                                                                                                    │
│ 597898534 │ ['./build_normal_debug/./src/Interpreters/ExpressionActions.cpp:718']                                                                                                                                                    │
│ 630442912 │ ['./build_normal_debug/./src/Processors/Transforms/ExpressionTransform.cpp:23']                                                                                                                                          │
│ 546354050 │ ['./build_normal_debug/./src/Processors/ISimpleTransform.h:38']                                                                                                                                                          │
│ 626026993 │ ['./build_normal_debug/./src/Processors/ISimpleTransform.cpp:89']                                                                                                                                                        │
│ 626294022 │ ['./build_normal_debug/./src/Processors/Executors/ExecutionThreadContext.cpp:45']                                                                                                                                        │
│ 626293730 │ ['./build_normal_debug/./src/Processors/Executors/ExecutionThreadContext.cpp:63']                                                                                                                                        │
│ 626169525 │ ['./build_normal_debug/./src/Processors/Executors/PipelineExecutor.cpp:213']                                                                                                                                             │
│ 626170308 │ ['./build_normal_debug/./src/Processors/Executors/PipelineExecutor.cpp:178']                                                                                                                                             │
│ 626166348 │ ['./build_normal_debug/./src/Processors/Executors/PipelineExecutor.cpp:329']                                                                                                                                             │
│ 626163461 │ ['./build_normal_debug/./src/Processors/Executors/PipelineExecutor.cpp:84']                                                                                                                                              │
│ 626323536 │ ['./build_normal_debug/./src/Processors/Executors/PullingAsyncPipelineExecutor.cpp:85']                                                                                                                                  │
│ 626323277 │ ['./build_normal_debug/./src/Processors/Executors/PullingAsyncPipelineExecutor.cpp:112']                                                                                                                                 │
│ 626323133 │ ['./build_normal_debug/./contrib/libcxx/include/type_traits:3682']                                                                                                                                                       │
│ 626323041 │ ['./build_normal_debug/./contrib/libcxx/include/tuple:1415']                                                                                                                                                             │
└───────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Introspection;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAddressToLineWithInlines>(documentation);
}

}

#endif
