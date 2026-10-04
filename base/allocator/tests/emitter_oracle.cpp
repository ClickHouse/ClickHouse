/// Runs the same scripts of emitter operations on `Emitter` and on jemalloc's `emitter.h` (see
/// `emitter_oracle_ref.c`) in all three output modes and compares the output byte-for-byte, including the boundaries
/// of the individual `write_cb` calls.

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

void compare(const char * name, const EmOp * ops, size_t nops)
{
    for (int output = EM_OUT_JSON; output <= EM_OUT_TABLE; ++output)
    {
        std::string expected;
        std::string actual;
        ref_emitter_run(output, ops, nops, recordingCallback, &expected);
        newEmitterRun(output, ops, nops, recordingCallback, &actual);
        if (expected != actual)
        {
            size_t pos = 0;
            while (pos < expected.size() && pos < actual.size() && expected[pos] == actual[pos])
                ++pos;
            std::fprintf(stderr, "%s (output %d): mismatch at byte %zu of %zu/%zu\n  expected: %.200s\n  actual:   %.200s\n",
                name, output, pos, expected.size(), actual.size(), expected.c_str() + std::min(pos, expected.size()),
                actual.c_str() + std::min(pos, actual.size()));
            ++allocator_test::failureCount();
        }
    }
}

template <size_t N>
void compare(const char * name, const EmOp (&ops)[N])
{
    compare(name, ops, N);
}

void compare(const char * name, const std::vector<EmOp> & ops)
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

std::vector<EmValue> interestingValues()
{
    using namespace em;
    return {
        vBool(false), vBool(true),
        vInt(0), vInt(-1), vInt(INT_MIN), vInt(INT_MAX), vInt(42),
        vInt64(0), vInt64(INT64_MIN), vInt64(INT64_MAX), vInt64(-1234567890123LL),
        vUnsigned(0), vUnsigned(UINT_MAX), vUnsigned(7),
        vUint32(0), vUint32(UINT32_MAX), vUint32(789),
        vUint64(0), vUint64(UINT64_MAX), vUint64(10000000000ULL),
        vSize(0), vSize(SIZE_MAX), vSize(4096),
        vSsize(0), vSsize(-1), vSsize(SSIZE_MAX), vSsize(-SSIZE_MAX - 1),
        vString(""), vString("x"), vString("with \"quotes\" and \\backslash\\"), vString("tab\tnewline\n"),
        vTitle(""), vTitle("Title"), vTitle("a longer title with spaces"),
    };
}

}

TEST(EmitterOracle, JemallocUnitTests)
{
    compare("dict", em::script_dict);
    compare("table_printf", em::script_table_printf);
    compare("nested_dict", em::script_nested_dict);
    compare("types", em::script_types);
    compare("modal", em::script_modal);
    compare("json_array", em::script_json_array);
    compare("json_nested_array", em::script_json_nested_array);
    compare("table_row", em::script_table_row);
}

TEST(EmitterOracle, AllTypes)
{
    using namespace em;
    std::vector<EmValue> values = interestingValues();
    std::vector<EmOp> ops{begin(), dictBegin("all", "All types:")};
    for (const EmValue & v : values)
    {
        ops.push_back(kv("key", "Key", v));
        for (const EmValue & note : values)
            ops.push_back(kvNote("k", "K", v, "note", note));
        ops.push_back(kvNote("k", "K", v, nullptr, vBool(false)));
        ops.push_back(jsonKv("json", v));
        ops.push_back(tableKv("Table", v));
        ops.push_back(tableKvNote("Table", v, "tnote", v));
    }
    ops.push_back(jsonArrayKvBegin("array"));
    for (const EmValue & v : values)
        ops.push_back(jsonValue(v));
    ops.push_back(jsonArrayEnd());
    ops.push_back(dictEnd());
    ops.push_back(end());
    compare("all_types", ops);
}

/// Strings around the 256-byte chunking boundaries of `emitter_emit_str` and the 4096-byte `malloc_vcprintf` buffer.
TEST(EmitterOracle, LongStrings)
{
    using namespace em;
    for (size_t len = 0; len < 1100; ++len)
    {
        if (len > 600 && len % 17 != 0 && !(len >= 760 && len <= 770) && !(len >= 1015 && len <= 1025))
            continue;
        const char * s = keep(makeString(len, unsigned(len)));
        std::vector<EmOp> ops{
            begin(),
            kv(s, s, vString(s)),
            kvNote("k", "K", vString(s), s, vString(s)),
            jsonArrayKvBegin("arr"),
            jsonValue(vString(s)),
            jsonValue(vTitle(s)),
            jsonArrayEnd(),
            tableKv("T", vTitle(s)),
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
        std::vector<EmOp> ops{
            begin(),
            kv(s, s, vString(s)),
            kv("k", "K", vTitle(s)),
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
    using namespace em;
    std::vector<EmValue> values = interestingValues();
    for (const char * s : {"", "abc", "x"})
        values.push_back(vString(s));
    values.push_back(vString(keep(makeString(300, 1))));
    values.push_back(vString(keep(makeString(700, 2))));
    values.push_back(vTitle(keep(makeString(300, 1))));

    /// Width 0 is not used: `%-0d` is rejected by an assertion of `malloc_vsnprintf`.
    static constexpr int widths[] = {1, 2, 5, 9, 10, 13, 20, 64, 255, 256, 300, 4095, 4096, 5000, 9999};
    std::vector<EmOp> ops{begin(), rowInit(0), rowInit(1)};
    int ncols = 0;
    for (int justify : {EM_J_LEFT, EM_J_RIGHT})
        for (int width : widths)
            if (ncols < EM_MAX_COLS)
                ops.push_back(colInit(ncols < 20 ? 0 : 1, ncols, justify, width)), ++ncols;
    for (size_t r = 0; r < values.size(); ++r)
    {
        for (int c = 0; c < ncols; ++c)
            ops.push_back(colSet(c, values[(r + size_t(c)) % values.size()]));
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
    using namespace em;
    std::vector<EmOp> ops{begin()};
    for (int i = 0; i < 60; ++i)
    {
        ops.push_back(dictBegin("level", "Level"));
        ops.push_back(kv("depth", "Depth", vInt(i)));
        if (i % 3 == 0)
        {
            ops.push_back(jsonArrayKvBegin("a"));
            ops.push_back(jsonValue(vInt(i)));
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
        ops.push_back(kv("after", "After", vInt(i)));
        ops.push_back(dictEnd());
    }
    ops.push_back(end());
    compare("deep_nesting", ops);
}

TEST(EmitterOracle, TablePrintfFormats)
{
    using namespace em;
    std::vector<EmOp> ops{
        begin(),
        tablePrintf(""),
        tablePrintf("plain\n"),
        tablePrintf("%%\n"),
        tablePrintfU64("%" FMTu64 "\n", UINT64_MAX),
        tablePrintfU64("[%20" FMTu64 "]\n", 12345),
        tablePrintfU64("[%-20" FMTx64 "]\n", 0xdeadbeef),
        tablePrintfU64("[%#" FMTx64 "]\n", 0xdeadbeef),
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
    using namespace em;
    std::vector<EmValue> values = interestingValues();
    const char * keys[] = {"a", "key", "", "Long key name", keep(makeString(260, 5))};
    std::mt19937_64 rng(12345);
    for (int doc = 0; doc < 3000; ++doc)
    {
        enum Kind
        {
            Dict,
            JsonObject,
            JsonArray,
            TableDict,
        };
        std::vector<Kind> stack;
        std::vector<EmOp> ops{begin()};
        int nrows = 0;
        int ncols = 0;
        size_t nops = rng() % 200;
        auto key = [&] { return keys[rng() % std::size(keys)]; };
        auto value = [&] { return values[rng() % values.size()]; };
        for (size_t i = 0; i < nops; ++i)
        {
            switch (rng() % 20)
            {
                case 0: ops.push_back(dictBegin(key(), key())); stack.push_back(Dict); break;
                case 1: ops.push_back(jsonObjectKvBegin(key())); stack.push_back(JsonObject); break;
                case 2: ops.push_back(jsonObjectBegin()); stack.push_back(JsonObject); break;
                case 3: ops.push_back(jsonArrayKvBegin(key())); stack.push_back(JsonArray); break;
                case 4: ops.push_back(jsonArrayBegin()); stack.push_back(JsonArray); break;
                case 5: ops.push_back(tableDictBegin(key())); stack.push_back(TableDict); break;
                case 6:
                case 7:
                    if (!stack.empty())
                    {
                        switch (stack.back())
                        {
                            case Dict: ops.push_back(dictEnd()); break;
                            case JsonObject: ops.push_back(jsonObjectEnd()); break;
                            case JsonArray: ops.push_back(jsonArrayEnd()); break;
                            case TableDict: ops.push_back(tableDictEnd()); break;
                        }
                        stack.pop_back();
                    }
                    break;
                case 8: ops.push_back(kv(key(), key(), value())); break;
                case 9: ops.push_back(kvNote(key(), key(), value(), rng() % 2 ? key() : nullptr, value())); break;
                case 10: ops.push_back(jsonKv(key(), value())); break;
                case 11: ops.push_back(jsonValue(value())); break;
                case 12: ops.push_back(jsonKey(key())); break;
                case 13: ops.push_back(tableKv(key(), value())); break;
                case 14: ops.push_back(tableKvNote(key(), value(), rng() % 2 ? key() : nullptr, value())); break;
                case 15: ops.push_back(tablePrintfS("%s\n", key())); break;
                case 16:
                    if (nrows < EM_MAX_ROWS)
                        ops.push_back(rowInit(nrows++));
                    break;
                case 17:
                    if (nrows > 0 && ncols < EM_MAX_COLS)
                        ops.push_back(colInit(int(rng() % nrows), ncols++, int(rng() % 2), int(1 + rng() % 30)));
                    break;
                case 18:
                    if (ncols > 0)
                        ops.push_back(colSet(int(rng() % ncols), value()));
                    break;
                case 19:
                    if (nrows > 0)
                        ops.push_back(tableRow(int(rng() % nrows)));
                    break;
            }
        }
        while (!stack.empty())
        {
            switch (stack.back())
            {
                case Dict: ops.push_back(dictEnd()); break;
                case JsonObject: ops.push_back(jsonObjectEnd()); break;
                case JsonArray: ops.push_back(jsonArrayEnd()); break;
                case TableDict: ops.push_back(tableDictEnd()); break;
            }
            stack.pop_back();
        }
        ops.push_back(end());
        compare("random", ops);
    }
}
