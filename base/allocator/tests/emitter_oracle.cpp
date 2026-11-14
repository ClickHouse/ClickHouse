/// Runs the same scripts of emitter operations on `Emitter` and on jemalloc's `emitter.h` (see
/// `emitter_oracle_ref.c`) in all three output modes and compares the output byte-for-byte, including the boundaries
/// of the individual `write_callback` calls.

#include "Test.h"
#include "emitter_script.h"

#include <climits>
#include <cstring>
#include <iterator>
#include <random>
#include <string>
#include <vector>

namespace
{

void recordingCallback(void * opaque, const char * s)
{
    std::string & out = *static_cast<std::string *>(opaque);
    out += s;
    out += '\x01';
}

void compare(const char * name, const EmitterOp * ops, size_t num_ops)
{
    for (int output = EMITTER_OUTPUT_JSON; output <= EMITTER_OUTPUT_TABLE; ++output)
    {
        std::string expected;
        std::string actual;
        ref_emitter_run(output, ops, num_ops, recordingCallback, &expected);
        newEmitterRun(output, ops, num_ops, recordingCallback, &actual);
        if (expected != actual)
        {
            size_t pos = 0;
            while (pos < expected.size() && pos < actual.size() && expected[pos] == actual[pos])
                ++pos;
            std::fprintf(
                stderr,
                "%s (output %d): mismatch at byte %zu of %zu/%zu\n  expected: %.200s\n  actual:   %.200s\n",
                name,
                output,
                pos,
                expected.size(),
                actual.size(),
                expected.c_str() + std::min(pos, expected.size()),
                actual.c_str() + std::min(pos, actual.size()));
            ++allocator_test::failureCount();
        }
    }
}

template <size_t N>
void compare(const char * name, const EmitterOp (&ops)[N])
{
    compare(name, ops, N);
}

void compare(const char * name, const std::vector<EmitterOp> & ops)
{
    compare(name, ops.data(), ops.size());
}

/// Strings that live as long as the test executable.
std::vector<std::string> & stringPool()
{
    static std::vector<std::string> pool;
    return pool;
}

const char * keep(std::string s)
{
    auto & pool = stringPool();
    pool.reserve(100000);
    REQUIRE(pool.size() < pool.capacity());
    pool.push_back(std::move(s));
    return pool.back().c_str();
}

std::string makeString(size_t len, unsigned seed)
{
    std::string s;
    for (size_t i = 0; i < len; ++i)
        s += char('a' + (i * 7 + seed) % 26);
    return s;
}

std::vector<EmitterValue> interestingValues()
{
    using namespace emitter;
    return {
        boolValue(false),
        boolValue(true),
        intValue(0),
        intValue(-1),
        intValue(INT_MIN),
        intValue(INT_MAX),
        intValue(42),
        int64Value(0),
        int64Value(INT64_MIN),
        int64Value(INT64_MAX),
        int64Value(-1234567890123LL),
        unsignedValue(0),
        unsignedValue(UINT_MAX),
        unsignedValue(7),
        uint32Value(0),
        uint32Value(UINT32_MAX),
        uint32Value(789),
        uint64Value(0),
        uint64Value(UINT64_MAX),
        uint64Value(10000000000ULL),
        sizeValue(0),
        sizeValue(SIZE_MAX),
        sizeValue(4096),
        ssizeValue(0),
        ssizeValue(-1),
        ssizeValue(SSIZE_MAX),
        ssizeValue(-SSIZE_MAX - 1),
        stringValue(""),
        stringValue("x"),
        stringValue("with \"quotes\" and \\backslash\\"),
        stringValue("tab\tnewline\n"),
        titleValue(""),
        titleValue("Title"),
        titleValue("a longer title with spaces"),
    };
}

}

TEST(EmitterOracle, JemallocUnitTests)
{
    compare("dict", emitter::script_dict);
    compare("table_printf", emitter::script_table_printf);
    compare("nested_dict", emitter::script_nested_dict);
    compare("types", emitter::script_types);
    compare("modal", emitter::script_modal);
    compare("json_array", emitter::script_json_array);
    compare("json_nested_array", emitter::script_json_nested_array);
    compare("table_row", emitter::script_table_row);
}

TEST(EmitterOracle, AllTypes)
{
    using namespace emitter;
    std::vector<EmitterValue> values = interestingValues();
    std::vector<EmitterOp> ops{begin(), dictBegin("all", "All types:")};
    for (const EmitterValue & v : values)
    {
        ops.push_back(keyValue("key", "Key", v));
        for (const EmitterValue & note : values)
            ops.push_back(keyValueNote("k", "K", v, "note", note));
        ops.push_back(keyValueNote("k", "K", v, nullptr, boolValue(false)));
        ops.push_back(jsonKeyValue("json", v));
        ops.push_back(tableKeyValue("Table", v));
        ops.push_back(tableKeyValueNote("Table", v, "tnote", v));
    }
    ops.push_back(jsonArrayKeyValueBegin("array"));
    for (const EmitterValue & v : values)
        ops.push_back(jsonValue(v));
    ops.push_back(jsonArrayEnd());
    ops.push_back(dictEnd());
    ops.push_back(end());
    compare("all_types", ops);
}

/// Strings around the 256-byte chunking boundaries of `emitter_emit_str` and the 4096-byte `malloc_vcprintf` buffer.
TEST(EmitterOracle, LongStrings)
{
    using namespace emitter;
    for (size_t len = 0; len < 1100; ++len)
    {
        if (len > 600 && len % 17 != 0 && !(len >= 760 && len <= 770) && !(len >= 1015 && len <= 1025))
            continue;
        const char * s = keep(makeString(len, unsigned(len)));
        std::vector<EmitterOp> ops{
            begin(),
            keyValue(s, s, stringValue(s)),
            keyValueNote("k", "K", stringValue(s), s, stringValue(s)),
            jsonArrayKeyValueBegin("arr"),
            jsonValue(stringValue(s)),
            jsonValue(titleValue(s)),
            jsonArrayEnd(),
            tableKeyValue("T", titleValue(s)),
            dictBegin(s, s),
            dictEnd(),
            tablePrintfS("%s\n", s),
            end(),
        };
        compare("long_string", ops);
    }
    for (size_t len : {4094, 4095, 4096, 4097, 5000, 9000})
    {
        const char * s = keep(makeString(len, 3));
        std::vector<EmitterOp> ops{
            begin(),
            keyValue(s, s, stringValue(s)),
            keyValue("k", "K", titleValue(s)),
            tablePrintfS("%s", s),
            tablePrintfS("prefix %s suffix\n", s),
            end(),
        };
        compare("very_long_string", ops);
    }
}

/// Table rows with every type, justification and a range of widths (including widths that exceed the 4096-byte
/// output buffer, and strings that are chunked).
TEST(EmitterOracle, TableRows)
{
    using namespace emitter;
    std::vector<EmitterValue> values = interestingValues();
    for (const char * s : {"", "abc", "x"})
        values.push_back(stringValue(s));
    values.push_back(stringValue(keep(makeString(300, 1))));
    values.push_back(stringValue(keep(makeString(700, 2))));
    values.push_back(titleValue(keep(makeString(300, 1))));

    /// Width 0 is not used: `%-0d` is rejected by an assertion of `malloc_vsnprintf`.
    static constexpr int widths[] = {1, 2, 5, 9, 10, 13, 20, 64, 255, 256, 300, 4095, 4096, 5000, 9999};
    std::vector<EmitterOp> ops{begin(), rowInit(0), rowInit(1)};
    int num_columns = 0;
    for (int justify : {EMITTER_JUSTIFY_LEFT, EMITTER_JUSTIFY_RIGHT})
        for (int width : widths)
            if (num_columns < EMITTER_MAX_COLUMNS)
                ops.push_back(columnInit(num_columns < 20 ? 0 : 1, num_columns, justify, width)), ++num_columns;
    for (size_t r = 0; r < values.size(); ++r)
    {
        for (int c = 0; c < num_columns; ++c)
            ops.push_back(columnSet(c, values[(r + size_t(c)) % values.size()]));
        ops.push_back(tableRow(0));
        ops.push_back(tableRow(1));
    }
    /// An empty row.
    ops.push_back(rowInit(2));
    ops.push_back(tableRow(2));
    ops.push_back(end());
    compare("table_rows", ops);
}

TEST(EmitterOracle, DeepNesting)
{
    using namespace emitter;
    std::vector<EmitterOp> ops{begin()};
    for (int i = 0; i < 60; ++i)
    {
        ops.push_back(dictBegin("level", "Level"));
        ops.push_back(keyValue("depth", "Depth", intValue(i)));
        if (i % 3 == 0)
        {
            ops.push_back(jsonArrayKeyValueBegin("a"));
            ops.push_back(jsonValue(intValue(i)));
            ops.push_back(jsonObjectBegin());
            ops.push_back(jsonObjectEnd());
            ops.push_back(jsonArrayEnd());
        }
        if (i % 4 == 0)
            ops.push_back(tableDictBegin("Extra"));
    }
    for (int i = 59; i >= 0; --i)
    {
        if (i % 4 == 0)
            ops.push_back(tableDictEnd());
        ops.push_back(keyValue("after", "After", intValue(i)));
        ops.push_back(dictEnd());
    }
    ops.push_back(end());
    compare("deep_nesting", ops);
}

TEST(EmitterOracle, TablePrintfFormats)
{
    using namespace emitter;
    std::vector<EmitterOp> ops{
        begin(),
        tablePrintf(""),
        tablePrintf("plain\n"),
        tablePrintf("%%\n"),
        tablePrintfU64("%" FORMAT_U64 "\n", UINT64_MAX),
        tablePrintfU64("[%20" FORMAT_U64 "]\n", 12345),
        tablePrintfU64("[%-20" FORMAT_X64 "]\n", 0xdeadbeef),
        tablePrintfU64("[%#" FORMAT_X64 "]\n", 0xdeadbeef),
        tablePrintfS("[%10s]\n", "abc"),
        tablePrintfS("[%-10s]\n", "abc"),
        tablePrintfS("[%.2s]\n", "abc"),
        end(),
    };
    compare("table_printf_formats", ops);
}

/// Random structurally valid documents.
TEST(EmitterOracle, Random)
{
    using namespace emitter;
    std::vector<EmitterValue> values = interestingValues();
    const char * keys[] = {"a", "key", "", "Long key name", keep(makeString(260, 5))};
    std::mt19937_64 rng(12345);
    for (int doc = 0; doc < 3000; ++doc)
    {
        enum Kind
        {
            Dict,
            JSONObject,
            JSONArray,
            TableDict,
        };
        std::vector<Kind> stack;
        std::vector<EmitterOp> ops{begin()};
        int num_rows = 0;
        int num_columns = 0;
        size_t num_ops = rng() % 200;
        auto key = [&] { return keys[rng() % std::size(keys)]; };
        auto value = [&] { return values[rng() % values.size()]; };
        for (size_t i = 0; i < num_ops; ++i)
        {
            switch (rng() % 20)
            {
                case 0:
                    ops.push_back(dictBegin(key(), key()));
                    stack.push_back(Dict);
                    break;
                case 1:
                    ops.push_back(jsonObjectKeyValueBegin(key()));
                    stack.push_back(JSONObject);
                    break;
                case 2:
                    ops.push_back(jsonObjectBegin());
                    stack.push_back(JSONObject);
                    break;
                case 3:
                    ops.push_back(jsonArrayKeyValueBegin(key()));
                    stack.push_back(JSONArray);
                    break;
                case 4:
                    ops.push_back(jsonArrayBegin());
                    stack.push_back(JSONArray);
                    break;
                case 5:
                    ops.push_back(tableDictBegin(key()));
                    stack.push_back(TableDict);
                    break;
                case 6:
                case 7:
                    if (!stack.empty())
                    {
                        switch (stack.back())
                        {
                            case Dict: ops.push_back(dictEnd()); break;
                            case JSONObject: ops.push_back(jsonObjectEnd()); break;
                            case JSONArray: ops.push_back(jsonArrayEnd()); break;
                            case TableDict: ops.push_back(tableDictEnd()); break;
                        }
                        stack.pop_back();
                    }
                    break;
                case 8: ops.push_back(keyValue(key(), key(), value())); break;
                case 9: ops.push_back(keyValueNote(key(), key(), value(), rng() % 2 ? key() : nullptr, value())); break;
                case 10: ops.push_back(jsonKeyValue(key(), value())); break;
                case 11: ops.push_back(jsonValue(value())); break;
                case 12: ops.push_back(jsonKey(key())); break;
                case 13: ops.push_back(tableKeyValue(key(), value())); break;
                case 14: ops.push_back(tableKeyValueNote(key(), value(), rng() % 2 ? key() : nullptr, value())); break;
                case 15: ops.push_back(tablePrintfS("%s\n", key())); break;
                case 16:
                    if (num_rows < EMITTER_MAX_ROWS)
                        ops.push_back(rowInit(num_rows++));
                    break;
                case 17:
                    if (num_rows > 0 && num_columns < EMITTER_MAX_COLUMNS)
                        ops.push_back(columnInit(int(rng() % num_rows), num_columns++, int(rng() % 2), int(1 + rng() % 30)));
                    break;
                case 18:
                    if (num_columns > 0)
                        ops.push_back(columnSet(int(rng() % num_columns), value()));
                    break;
                case 19:
                    if (num_rows > 0)
                        ops.push_back(tableRow(int(rng() % num_rows)));
                    break;
            }
        }
        while (!stack.empty())
        {
            switch (stack.back())
            {
                case Dict: ops.push_back(dictEnd()); break;
                case JSONObject: ops.push_back(jsonObjectEnd()); break;
                case JSONArray: ops.push_back(jsonArrayEnd()); break;
                case TableDict: ops.push_back(tableDictEnd()); break;
            }
            stack.pop_back();
        }
        ops.push_back(end());
        compare("random", ops);
    }
}
