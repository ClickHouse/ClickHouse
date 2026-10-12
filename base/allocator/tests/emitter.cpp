/// The test cases of jemalloc's `test/unit/emitter.c` (expected outputs copied literally) run on `Emitter`.

#include "Test.h"
#include "emitter_script.h"

#include <cstring>
#include <iterator>

namespace
{

struct Buffer
{
    char data[jemalloc::MALLOC_PRINTF_BUF_SIZE];
    size_t len = 0;
};

void forwardingCallback(void * opaque, const char * s)
{
    Buffer & buf = *static_cast<Buffer *>(opaque);
    size_t n = std::strlen(s);
    REQUIRE(buf.len + n < sizeof(buf.data));
    std::memcpy(buf.data + buf.len, s, n + 1);
    buf.len += n;
}

template <size_t N>
void expectEmitOutput(const EmitterOp (&script)[N], const char * json, const char * json_compact, const char * table)
{
    const char * expected[] = {json, json_compact, table};
    for (int output = EMITTER_OUTPUT_JSON; output <= EMITTER_OUTPUT_TABLE; ++output)
    {
        Buffer buf;
        buf.data[0] = '\0';
        newEmitterRun(output, script, N, forwardingCallback, &buf);
        CHECK_STREQ(expected[output], buf.data);
    }
}

}

TEST(Emitter, Dict)
{
    expectEmitOutput(
        emitter::script_dict,
        "{\n"
        "\t\"foo\": {\n"
        "\t\t\"abc\": false,\n"
        "\t\t\"def\": true,\n"
        "\t\t\"ghi\": 123,\n"
        "\t\t\"jkl\": \"a string\"\n"
        "\t}\n"
        "}\n",
        "{"
        "\"foo\":{"
        "\"abc\":false,"
        "\"def\":true,"
        "\"ghi\":123,"
        "\"jkl\":\"a string\""
        "}"
        "}",
        "This is the foo table:\n"
        "  ABC: false\n"
        "  DEF: true\n"
        "  GHI: 123 (note_key1: \"a string\")\n"
        "  JKL: \"a string\" (note_key2: false)\n");
}

TEST(Emitter, TablePrintf)
{
    expectEmitOutput(
        emitter::script_table_printf,
        "{\n"
        "}\n",
        "{}",
        "Table note 1\n"
        "Table note 2 with format string\n");
}

TEST(Emitter, NestedDict)
{
    expectEmitOutput(
        emitter::script_nested_dict,
        "{\n"
        "\t\"json1\": {\n"
        "\t\t\"json2\": {\n"
        "\t\t\t\"primitive\": 123\n"
        "\t\t},\n"
        "\t\t\"json3\": {\n"
        "\t\t}\n"
        "\t},\n"
        "\t\"json4\": {\n"
        "\t\t\"primitive\": 123\n"
        "\t}\n"
        "}\n",
        "{"
        "\"json1\":{"
        "\"json2\":{"
        "\"primitive\":123"
        "},"
        "\"json3\":{"
        "}"
        "},"
        "\"json4\":{"
        "\"primitive\":123"
        "}"
        "}",
        "Dict 1\n"
        "  Dict 2\n"
        "    A primitive: 123\n"
        "  Dict 3\n"
        "Dict 4\n"
        "  Another primitive: 123\n");
}

#define LONG_STR \
    "abcdefghijklmnopqrstuvwxyz " \
    "abcdefghijklmnopqrstuvwxyz " \
    "abcdefghijklmnopqrstuvwxyz " \
    "abcdefghijklmnopqrstuvwxyz " \
    "abcdefghijklmnopqrstuvwxyz " \
    "abcdefghijklmnopqrstuvwxyz " \
    "abcdefghijklmnopqrstuvwxyz " \
    "abcdefghijklmnopqrstuvwxyz " \
    "abcdefghijklmnopqrstuvwxyz " \
    "abcdefghijklmnopqrstuvwxyz"

TEST(Emitter, Types)
{
    expectEmitOutput(
        emitter::script_types,
        "{\n"
        "\t\"k1\": false,\n"
        "\t\"k2\": -123,\n"
        "\t\"k3\": 123,\n"
        "\t\"k4\": -456,\n"
        "\t\"k5\": 456,\n"
        "\t\"k6\": \"string\",\n"
        "\t\"k7\": \"" LONG_STR "\",\n"
        "\t\"k8\": 789,\n"
        "\t\"k9\": 10000000000\n"
        "}\n",
        "{"
        "\"k1\":false,"
        "\"k2\":-123,"
        "\"k3\":123,"
        "\"k4\":-456,"
        "\"k5\":456,"
        "\"k6\":\"string\","
        "\"k7\":\"" LONG_STR "\","
        "\"k8\":789,"
        "\"k9\":10000000000"
        "}",
        "K1: false\n"
        "K2: -123\n"
        "K3: 123\n"
        "K4: -456\n"
        "K5: 456\n"
        "K6: \"string\"\n"
        "K7: \"" LONG_STR "\"\n"
        "K8: 789\n"
        "K9: 10000000000\n");
}

TEST(Emitter, Modal)
{
    expectEmitOutput(
        emitter::script_modal,
        "{\n"
        "\t\"j0\": {\n"
        "\t\t\"j1\": {\n"
        "\t\t\t\"i1\": 123,\n"
        "\t\t\t\"i2\": 123,\n"
        "\t\t\t\"i4\": 123\n"
        "\t\t},\n"
        "\t\t\"i5\": 123,\n"
        "\t\t\"i6\": 123\n"
        "\t}\n"
        "}\n",
        "{"
        "\"j0\":{"
        "\"j1\":{"
        "\"i1\":123,"
        "\"i2\":123,"
        "\"i4\":123"
        "},"
        "\"i5\":123,"
        "\"i6\":123"
        "}"
        "}",
        "T0\n"
        "  I1: 123\n"
        "  I3: 123\n"
        "  T1\n"
        "    I4: 123\n"
        "    I5: 123\n"
        "  I6: 123\n");
}

TEST(Emitter, JSONArray)
{
    expectEmitOutput(
        emitter::script_json_array,
        "{\n"
        "\t\"dict\": {\n"
        "\t\t\"arr\": [\n"
        "\t\t\t{\n"
        "\t\t\t\t\"foo\": 123\n"
        "\t\t\t},\n"
        "\t\t\t123,\n"
        "\t\t\t123,\n"
        "\t\t\t{\n"
        "\t\t\t\t\"bar\": 123,\n"
        "\t\t\t\t\"baz\": 123\n"
        "\t\t\t}\n"
        "\t\t]\n"
        "\t}\n"
        "}\n",
        "{"
        "\"dict\":{"
        "\"arr\":["
        "{"
        "\"foo\":123"
        "},"
        "123,"
        "123,"
        "{"
        "\"bar\":123,"
        "\"baz\":123"
        "}"
        "]"
        "}"
        "}",
        "");
}

TEST(Emitter, JSONNestedArray)
{
    expectEmitOutput(
        emitter::script_json_nested_array,
        "{\n"
        "\t[\n"
        "\t\t[\n"
        "\t\t\t123,\n"
        "\t\t\t\"foo\",\n"
        "\t\t\t123,\n"
        "\t\t\t\"foo\"\n"
        "\t\t],\n"
        "\t\t[\n"
        "\t\t\t123\n"
        "\t\t],\n"
        "\t\t[\n"
        "\t\t\t\"foo\",\n"
        "\t\t\t123\n"
        "\t\t],\n"
        "\t\t[\n"
        "\t\t]\n"
        "\t]\n"
        "}\n",
        "{"
        "["
        "["
        "123,"
        "\"foo\","
        "123,"
        "\"foo\""
        "],"
        "["
        "123"
        "],"
        "["
        "\"foo\","
        "123"
        "],"
        "["
        "]"
        "]"
        "}",
        "");
}

TEST(Emitter, TableRow)
{
    expectEmitOutput(
        emitter::script_table_row,
        "{\n"
        "}\n",
        "{}",
        "ABC title       DEF title  GHI\n"
        "123                  true  456\n"
        "789                 false 1011\n"
        "\"a string\"          false  ghi\n");
}

/// The table mode always calls the callback at least once, even for an empty document.
TEST(Emitter, BeginWritesOnce)
{
    struct Counter
    {
        int calls = 0;
        static void callback(void * opaque, const char * s)
        {
            ++static_cast<Counter *>(opaque)->calls;
            CHECK_EQ(s[0], '\0');
        }
    };
    Counter counter;
    const EmitterOp script[] = {emitter::begin(), emitter::end()};
    newEmitterRun(EMITTER_OUTPUT_TABLE, script, std::size(script), &Counter::callback, &counter);
    CHECK_EQ(counter.calls, 1);
}
