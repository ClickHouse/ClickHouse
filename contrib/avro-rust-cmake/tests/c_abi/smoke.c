/// Compiled as C and as C++ by tests/c_abi.rs and linked with the static library:
/// the header is the contract, this program uses nothing but the header.
#include "avro_rust.h"

#include <stdio.h>
#include <string.h>

#ifdef __cplusplus
#    define STATIC_ASSERT(c, m) static_assert(c, m)
#else
#    define STATIC_ASSERT(c, m) _Static_assert(c, m)
#endif

STATIC_ASSERT(sizeof(AvroSinkDesc) == 12, "AvroSinkDesc");
STATIC_ASSERT(sizeof(AvroPlanNode) == 48, "AvroPlanNode");
STATIC_ASSERT(sizeof(AvroSink) == 56, "AvroSink");
STATIC_ASSERT(sizeof(AvroProgress) == 56, "AvroProgress");
STATIC_ASSERT(sizeof(AvroNeed) == 24, "AvroNeed");
STATIC_ASSERT(sizeof(AvroBlockHeader) == 24, "AvroBlockHeader");
STATIC_ASSERT(sizeof(AvroAttr) == 40, "AvroAttr");
STATIC_ASSERT(sizeof(AvroNodeInfo) == 72, "AvroNodeInfo");

#define CHECK(cond) \
    do \
    { \
        if (!(cond)) \
        { \
            fprintf(stderr, "%s:%d: check failed: %s (err: %s)\n", __FILE__, __LINE__, #cond, err); \
            return 1; \
        } \
    } while (0)

int main(void)
{
    char err[256] = {0};
    const char * json
        = "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
          "{\"name\":\"id\",\"type\":\"long\",\"field-id\":42},"
          "{\"name\":\"s\",\"type\":[\"null\",\"string\"]}]}";

    AvroSchema * schema = NULL;
    CHECK(avro_schema_parse((const uint8_t *)json, strlen(json), 256, &schema, err, sizeof(err)) == AVRO_OK);
    uint32_t root = avro_schema_root(schema);

    AvroAttr attr;
    CHECK(avro_schema_attr(schema, root, 0, (const uint8_t *)"field-id", 8, &attr, err, sizeof(err)) == AVRO_OK);
    CHECK(attr.kind == AVRO_ATTR_INT && attr.int_value == 42);

    /// The schema text and a bounded prefix of the canonical form, before the form was ever computed.
    const uint8_t * source = NULL;
    size_t source_len = 0;
    CHECK(avro_schema_source(schema, &source, &source_len, err, sizeof(err)) == AVRO_OK);
    CHECK(source_len == strlen(json) && memcmp(source, json, source_len) == 0);
    char prefix[8];
    size_t prefix_len = 0;
    uint8_t truncated = 0;
    CHECK(avro_schema_canonical_prefix(schema, prefix, sizeof(prefix), &prefix_len, &truncated, err, sizeof(err)) == AVRO_OK);
    CHECK(prefix_len == 8 && truncated == 1 && memcmp(prefix, "{\"name\":", 8) == 0);

    uint32_t id_node = 0, union_node = 0, string_node = 0;
    CHECK(avro_schema_child(schema, root, 0, &id_node, NULL, NULL, err, sizeof(err)) == AVRO_OK);
    CHECK(avro_schema_child(schema, root, 1, &union_node, NULL, NULL, err, sizeof(err)) == AVRO_OK);
    CHECK(avro_schema_child(schema, union_node, 1, &string_node, NULL, NULL, err, sizeof(err)) == AVRO_OK);
    uint32_t null_node = 0;
    CHECK(avro_schema_child(schema, union_node, 0, &null_node, NULL, NULL, err, sizeof(err)) == AVRO_OK);

    AvroSinkDesc sinks[3];
    memset(sinks, 0, sizeof(sinks));
    sinks[0].kind = AVRO_SINK_INT64;
    sinks[1].kind = AVRO_SINK_NULL_MAP;
    sinks[2].kind = AVRO_SINK_BYTES;
    sinks[2].flags = AVRO_SINK_TERMINATED;

    AvroPlanNode plan[5];
    memset(plan, 0, sizeof(plan));
    for (int i = 0; i < 5; ++i)
        plan[i].sink = plan[i].sink2 = AVRO_NO_SINK;
    plan[0].action = AVRO_ACTION_RECORD;
    plan[0].schema_node = root;
    plan[0].n_children = 2;
    plan[1].action = AVRO_ACTION_VALUE;
    plan[1].schema_node = id_node;
    plan[1].sink = 0;
    plan[2].action = AVRO_ACTION_UNION;
    plan[2].schema_node = union_node;
    plan[2].sink = 1;
    plan[2].arg = 0; /// the null branch
    plan[2].n_children = 2;
    plan[3].action = AVRO_ACTION_SKIP;
    plan[3].schema_node = null_node;
    plan[4].action = AVRO_ACTION_VALUE;
    plan[4].schema_node = string_node;
    plan[4].sink = 2;

    AvroProgram * program = NULL;
    CHECK(avro_program_compile(schema, sinks, 3, plan, 5, &program, err, sizeof(err)) == AVRO_OK);
    CHECK(avro_program_min_row_size(program) == 2);
    CHECK(avro_program_memory_size(program) > 0);

    /// Rows: (1, "hi"), (-1, null), (2, "")
    const uint8_t input[] = {0x02, 0x02, 0x04, 'h', 'i', 0x01, 0x00, 0x04, 0x02, 0x00};
    int64_t ids[4];
    uint8_t null_map[4];
    uint8_t chars[4];
    uint64_t offsets[4];
    AvroSink s[3];
    memset(s, 0, sizeof(s));
    s[0].data = (uint8_t *)ids;
    s[0].data_size = sizeof(ids);
    s[1].data = null_map;
    s[1].data_size = sizeof(null_map);
    s[2].data = chars;
    s[2].data_size = 2; /// too small for "hi" plus the terminator: forces NEED_SPACE
    s[2].data2 = (uint8_t *)offsets;
    s[2].data2_size = sizeof(offsets);

    AvroState * state = avro_state_new(64);
    AvroProgress p;
    CHECK(avro_decode(state, program, input, sizeof(input), 3, s, 3, &p, err, sizeof(err)) == AVRO_OK);
    CHECK(p.status == AVRO_STATUS_NEED_SPACE && p.need_sink == 2 && p.need_cap == 3 && p.rows_done == 0);
    /// The list of the sinks that lack room has the one sink, with the same capacities.
    CHECK(p.need_count == 1 && p.needs != NULL && p.needs[0].sink == 2 && p.needs[0].cap == 3 && p.needs[0].cap2 == 1);
    /// The row that did not fit is paused, not rolled back: nothing is committed or consumed. The state keeps the
    /// fill levels of the other sinks (id and null map written), and the array gets back those of the sink to
    /// grow: the string is not written yet.
    CHECK(p.consumed == 0 && s[2].len == 0);
    s[2].data_size = sizeof(chars);
    uint64_t consumed = p.consumed;
    CHECK(avro_decode(state, program, input + consumed, sizeof(input) - consumed, 3, s, 3, &p, err, sizeof(err)) == AVRO_OK);
    CHECK(p.status == AVRO_STATUS_ROW_LIMIT && p.rows_done == 3 && consumed + p.consumed == sizeof(input));
    CHECK(ids[0] == 1 && ids[1] == -1 && ids[2] == 2);
    CHECK(s[1].len == 3 && null_map[0] == 0 && null_map[1] == 1 && null_map[2] == 0);
    CHECK(s[2].len == 3 + 1 && memcmp(chars, "hi\0\0", 4) == 0 && s[2].len2 == 2);
    CHECK(offsets[0] == 3 && offsets[1] == 4);

    /// Errors carry codes, and the rows completed before them stay committed:
    /// row 1 is (1, null), row 2 fails with a union index out of range.
    const uint8_t bad[] = {0x02, 0x00, 0x02, 0x05};
    s[0].len = s[1].len = s[2].len = s[2].len2 = 0;
    int32_t rc = avro_decode(state, program, bad, sizeof(bad), 2, s, 3, &p, err, sizeof(err));
    CHECK(rc == -AVRO_ERR_UNION_INDEX_OUT_OF_RANGE);
    CHECK(p.status == AVRO_STATUS_ERROR && p.rows_done == 1 && p.consumed == 2);
    CHECK(s[0].len == 1 && s[1].len == 1 && null_map[0] == 1 && s[2].len == 0);
    err[0] = 0;
    /// Three rows where row 2 fails: (1, "hi"), a bad union index, (2, null). Exactly row 1 is
    /// committed, the cursor is at the start of row 2 and no value of row 2 is counted in the sinks.
    {
        const uint8_t three[] = {0x02, 0x02, 0x04, 'h', 'i', 0x04, 0x05, 0x04, 0x00};
        s[0].len = s[1].len = s[2].len = s[2].len2 = 0;
        s[2].offset = 0;
        rc = avro_decode(state, program, three, sizeof(three), 3, s, 3, &p, err, sizeof(err));
        CHECK(rc == -AVRO_ERR_UNION_INDEX_OUT_OF_RANGE);
        CHECK(p.status == AVRO_STATUS_ERROR && p.rows_done == 1 && p.consumed == 5);
        CHECK(s[0].len == 1 && ids[0] == 1);
        CHECK(s[1].len == 1 && null_map[0] == 0);
        CHECK(s[2].len == 3 && s[2].len2 == 1 && s[2].offset == 3 && offsets[0] == 3);
        err[0] = 0;
    }
    /// A nothing-committed error and the same state still works for the next block.
    const uint8_t bad_first[] = {0x02, 0x05};
    rc = avro_decode(state, program, bad_first, sizeof(bad_first), 1, s, 3, &p, err, sizeof(err));
    CHECK(rc == -AVRO_ERR_UNION_INDEX_OUT_OF_RANGE && p.rows_done == 0 && p.consumed == 0 && s[0].len == 1);
    err[0] = 0;

    /// A container file.
    const uint8_t file[] = {'O', 'b', 'j', 1, 0x02, 0x16, 'a', 'v', 'r', 'o', '.', 's', 'c', 'h', 'e', 'm', 'a', 0x0c, '"', 'l', 'o', 'n', 'g', '"', 0,
                            /// the 16 byte sync marker
                            1,   1,   1,   1, 1,    1,    1,   1,   1,   1,   1,   1,   1,   1,   1,   1};
    AvroOcfHeader * header = NULL;
    uint64_t used = 0, needed = 0;
    CHECK(avro_ocf_header_parse(file, 10, 0, 0, &header, &used, &needed, err, sizeof(err)) == AVRO_NEED_MORE);
    CHECK(needed > 10);
    CHECK(avro_ocf_header_parse(file, sizeof(file), 1, 0, &header, &used, &needed, err, sizeof(err)) == AVRO_OK);
    CHECK(used == sizeof(file) && avro_ocf_header_codec(header) == AVRO_CODEC_NULL);
    const uint8_t * schema_json = NULL;
    size_t schema_len = 0;
    CHECK(avro_ocf_header_schema(header, &schema_json, &schema_len, err, sizeof(err)) == AVRO_OK && schema_len == 6);
    avro_ocf_header_free(header);

    avro_state_free(state);
    avro_program_free(program);
    avro_schema_free(schema);
    printf("c abi smoke ok\n");
    return 0;
}
