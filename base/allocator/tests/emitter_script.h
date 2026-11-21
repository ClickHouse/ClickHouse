/* Scripted emitter operations, shared between `emitter.cpp`, `emitter_oracle.cpp` (C++ `Emitter`) and
 * `emitter_oracle_ref.c` (jemalloc's `emitter.h`). The numeric values of output modes, justifications and types are
 * those of the jemalloc enums (and of the C++ enum classes). */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

enum EmitterOpCode
{
    EMITTER_BEGIN,
    EMITTER_END,
    EMITTER_JSON_KEY,
    EMITTER_JSON_VALUE,
    EMITTER_JSON_KEY_VALUE,
    EMITTER_JSON_ARRAY_BEGIN,
    EMITTER_JSON_ARRAY_KEY_VALUE_BEGIN,
    EMITTER_JSON_ARRAY_END,
    EMITTER_JSON_OBJECT_BEGIN,
    EMITTER_JSON_OBJECT_KEY_VALUE_BEGIN,
    EMITTER_JSON_OBJECT_END,
    EMITTER_TABLE_DICT_BEGIN,
    EMITTER_TABLE_DICT_END,
    EMITTER_TABLE_KEY_VALUE_NOTE,
    EMITTER_TABLE_KEY_VALUE,
    EMITTER_TABLE_PRINTF, /* key is the format, no arguments */
    EMITTER_TABLE_PRINTF_S, /* key is the format, key2 the argument */
    EMITTER_TABLE_PRINTF_U64, /* key is the format, v.u64 the argument */
    EMITTER_KEY_VALUE_NOTE,
    EMITTER_KEY_VALUE,
    EMITTER_DICT_BEGIN,
    EMITTER_DICT_END,
    EMITTER_ROW_INIT, /* row */
    EMITTER_COLUMN_INIT, /* row, col, justify, width */
    EMITTER_COLUMN_SET, /* col, v */
    EMITTER_TABLE_ROW, /* row */
};

/* Value types (emitter_type_t). */
enum
{
    EMITTER_TYPE_BOOL,
    EMITTER_TYPE_INT,
    EMITTER_TYPE_INT64,
    EMITTER_TYPE_UNSIGNED,
    EMITTER_TYPE_UINT32,
    EMITTER_TYPE_UINT64,
    EMITTER_TYPE_SIZE,
    EMITTER_TYPE_SSIZE,
    EMITTER_TYPE_STRING,
    EMITTER_TYPE_TITLE,
};

/* Justification (emitter_justify_t). */
enum
{
    EMITTER_JUSTIFY_LEFT,
    EMITTER_JUSTIFY_RIGHT,
};

/* Output modes (emitter_output_t). */
enum
{
    EMITTER_OUTPUT_JSON,
    EMITTER_OUTPUT_JSON_COMPACT,
    EMITTER_OUTPUT_TABLE,
};

#define EMITTER_MAX_ROWS 8
#define EMITTER_MAX_COLUMNS 64

struct EmitterValue
{
    int type;
    union
    {
        bool b;
        int i;
        int64_t i64;
        unsigned u;
        uint32_t u32;
        uint64_t u64;
        size_t zu;
        ssize_t zd;
        const char * s;
    } value;
};

struct EmitterOp
{
    int op;
    const char * key;
    const char * key2;
    struct EmitterValue v;
    const char * note_key;
    struct EmitterValue note;
    int row;
    int column;
    int justify;
    int width;
};

typedef void EmitterWriteCallback(void * opaque, const char * s);

#ifdef __cplusplus
extern "C" {
#endif

void ref_emitter_run(int output, const struct EmitterOp * ops, size_t num_ops, EmitterWriteCallback * write_callback, void * opaque);

#ifdef __cplusplus
}

#include <allocator/Emitter.h>

#include <cstring>

/// The same interpreter for the C++ `Emitter`.
inline void newEmitterRun(int output, const EmitterOp * ops, size_t num_ops, EmitterWriteCallback * write_callback, void * opaque)
{
    using namespace jemalloc;
    Emitter emitter(static_cast<EmitterOutput>(output), write_callback, opaque);
    EmitterRow rows[EMITTER_MAX_ROWS];
    EmitterColumn columns[EMITTER_MAX_COLUMNS];
    auto type = [](const EmitterValue & v) { return static_cast<EmitterType>(v.type); };

    for (size_t i = 0; i < num_ops; ++i)
    {
        const EmitterOp & op = ops[i];
        switch (op.op)
        {
            case EMITTER_BEGIN: emitter.begin(); break;
            case EMITTER_END: emitter.end(); break;
            case EMITTER_JSON_KEY: emitter.jsonKey(op.key); break;
            case EMITTER_JSON_VALUE: emitter.jsonValue(type(op.v), &op.v.value); break;
            case EMITTER_JSON_KEY_VALUE: emitter.jsonKeyValue(op.key, type(op.v), &op.v.value); break;
            case EMITTER_JSON_ARRAY_BEGIN: emitter.jsonArrayBegin(); break;
            case EMITTER_JSON_ARRAY_KEY_VALUE_BEGIN: emitter.jsonArrayKeyValueBegin(op.key); break;
            case EMITTER_JSON_ARRAY_END: emitter.jsonArrayEnd(); break;
            case EMITTER_JSON_OBJECT_BEGIN: emitter.jsonObjectBegin(); break;
            case EMITTER_JSON_OBJECT_KEY_VALUE_BEGIN: emitter.jsonObjectKeyValueBegin(op.key); break;
            case EMITTER_JSON_OBJECT_END: emitter.jsonObjectEnd(); break;
            case EMITTER_TABLE_DICT_BEGIN: emitter.tableDictBegin(op.key); break;
            case EMITTER_TABLE_DICT_END: emitter.tableDictEnd(); break;
            case EMITTER_TABLE_KEY_VALUE_NOTE:
                emitter.tableKeyValueNote(op.key, type(op.v), &op.v.value, op.note_key, type(op.note), &op.note.value);
                break;
            case EMITTER_TABLE_KEY_VALUE: emitter.tableKeyValue(op.key, type(op.v), &op.v.value); break;
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wformat-nonliteral"
#pragma clang diagnostic ignored "-Wformat-security"
            case EMITTER_TABLE_PRINTF: emitter.tablePrintf(op.key); break;
            case EMITTER_TABLE_PRINTF_S: emitter.tablePrintf(op.key, op.key2); break;
            case EMITTER_TABLE_PRINTF_U64: emitter.tablePrintf(op.key, op.v.value.u64); break;
#pragma clang diagnostic pop
            case EMITTER_KEY_VALUE_NOTE:
                emitter.keyValueNote(op.key, op.key2, type(op.v), &op.v.value, op.note_key, type(op.note), &op.note.value);
                break;
            case EMITTER_KEY_VALUE: emitter.keyValue(op.key, op.key2, type(op.v), &op.v.value); break;
            case EMITTER_DICT_BEGIN: emitter.dictBegin(op.key, op.key2); break;
            case EMITTER_DICT_END: emitter.dictEnd(); break;
            case EMITTER_ROW_INIT: rows[op.row].init(); break;
            case EMITTER_COLUMN_INIT:
                columns[op.column].justify = static_cast<EmitterJustify>(op.justify);
                columns[op.column].width = op.width;
                columns[op.column].init(rows[op.row]);
                break;
            case EMITTER_COLUMN_SET:
                columns[op.column].type = type(op.v);
                std::memcpy(&columns[op.column].bool_value, &op.v.value, sizeof(op.v.value));
                break;
            case EMITTER_TABLE_ROW: emitter.tableRow(rows[op.row]); break;
            default: break;
        }
    }
}

/// Builders for scripts.
namespace emitter
{

inline EmitterValue boolValue(bool x)
{
    EmitterValue v{};
    v.type = EMITTER_TYPE_BOOL;
    v.value.b = x;
    return v;
}
inline EmitterValue intValue(int x)
{
    EmitterValue v{};
    v.type = EMITTER_TYPE_INT;
    v.value.i = x;
    return v;
}
inline EmitterValue int64Value(int64_t x)
{
    EmitterValue v{};
    v.type = EMITTER_TYPE_INT64;
    v.value.i64 = x;
    return v;
}
inline EmitterValue unsignedValue(unsigned x)
{
    EmitterValue v{};
    v.type = EMITTER_TYPE_UNSIGNED;
    v.value.u = x;
    return v;
}
inline EmitterValue uint32Value(uint32_t x)
{
    EmitterValue v{};
    v.type = EMITTER_TYPE_UINT32;
    v.value.u32 = x;
    return v;
}
inline EmitterValue uint64Value(uint64_t x)
{
    EmitterValue v{};
    v.type = EMITTER_TYPE_UINT64;
    v.value.u64 = x;
    return v;
}
inline EmitterValue sizeValue(size_t x)
{
    EmitterValue v{};
    v.type = EMITTER_TYPE_SIZE;
    v.value.zu = x;
    return v;
}
inline EmitterValue ssizeValue(ssize_t x)
{
    EmitterValue v{};
    v.type = EMITTER_TYPE_SSIZE;
    v.value.zd = x;
    return v;
}
inline EmitterValue stringValue(const char * x)
{
    EmitterValue v{};
    v.type = EMITTER_TYPE_STRING;
    v.value.s = x;
    return v;
}
inline EmitterValue titleValue(const char * x)
{
    EmitterValue v{};
    v.type = EMITTER_TYPE_TITLE;
    v.value.s = x;
    return v;
}

inline EmitterOp op(int code)
{
    EmitterOp o{};
    o.op = code;
    return o;
}
inline EmitterOp begin()
{
    return op(EMITTER_BEGIN);
}
inline EmitterOp end()
{
    return op(EMITTER_END);
}
inline EmitterOp jsonKey(const char * k)
{
    EmitterOp o = op(EMITTER_JSON_KEY);
    o.key = k;
    return o;
}
inline EmitterOp jsonValue(EmitterValue v)
{
    EmitterOp o = op(EMITTER_JSON_VALUE);
    o.v = v;
    return o;
}
inline EmitterOp jsonKeyValue(const char * k, EmitterValue v)
{
    EmitterOp o = op(EMITTER_JSON_KEY_VALUE);
    o.key = k;
    o.v = v;
    return o;
}
inline EmitterOp jsonArrayBegin()
{
    return op(EMITTER_JSON_ARRAY_BEGIN);
}
inline EmitterOp jsonArrayKeyValueBegin(const char * k)
{
    EmitterOp o = op(EMITTER_JSON_ARRAY_KEY_VALUE_BEGIN);
    o.key = k;
    return o;
}
inline EmitterOp jsonArrayEnd()
{
    return op(EMITTER_JSON_ARRAY_END);
}
inline EmitterOp jsonObjectBegin()
{
    return op(EMITTER_JSON_OBJECT_BEGIN);
}
inline EmitterOp jsonObjectKeyValueBegin(const char * k)
{
    EmitterOp o = op(EMITTER_JSON_OBJECT_KEY_VALUE_BEGIN);
    o.key = k;
    return o;
}
inline EmitterOp jsonObjectEnd()
{
    return op(EMITTER_JSON_OBJECT_END);
}
inline EmitterOp tableDictBegin(const char * k)
{
    EmitterOp o = op(EMITTER_TABLE_DICT_BEGIN);
    o.key = k;
    return o;
}
inline EmitterOp tableDictEnd()
{
    return op(EMITTER_TABLE_DICT_END);
}
inline EmitterOp tableKeyValueNote(const char * k, EmitterValue v, const char * note_key, EmitterValue note_value)
{
    EmitterOp o = op(EMITTER_TABLE_KEY_VALUE_NOTE);
    o.key = k;
    o.v = v;
    o.note_key = note_key;
    o.note = note_value;
    return o;
}
inline EmitterOp tableKeyValue(const char * k, EmitterValue v)
{
    EmitterOp o = op(EMITTER_TABLE_KEY_VALUE);
    o.key = k;
    o.v = v;
    return o;
}
inline EmitterOp tablePrintf(const char * format_string)
{
    EmitterOp o = op(EMITTER_TABLE_PRINTF);
    o.key = format_string;
    return o;
}
inline EmitterOp tablePrintfS(const char * format_string, const char * s)
{
    EmitterOp o = op(EMITTER_TABLE_PRINTF_S);
    o.key = format_string;
    o.key2 = s;
    return o;
}
inline EmitterOp tablePrintfU64(const char * format_string, uint64_t x)
{
    EmitterOp o = op(EMITTER_TABLE_PRINTF_U64);
    o.key = format_string;
    o.v = uint64Value(x);
    return o;
}
inline EmitterOp keyValueNote(const char * json_key, const char * table_key, EmitterValue v, const char * note_key, EmitterValue note_value)
{
    EmitterOp o = op(EMITTER_KEY_VALUE_NOTE);
    o.key = json_key;
    o.key2 = table_key;
    o.v = v;
    o.note_key = note_key;
    o.note = note_value;
    return o;
}
inline EmitterOp keyValue(const char * json_key, const char * table_key, EmitterValue v)
{
    EmitterOp o = op(EMITTER_KEY_VALUE);
    o.key = json_key;
    o.key2 = table_key;
    o.v = v;
    return o;
}
inline EmitterOp dictBegin(const char * json_key, const char * table_header)
{
    EmitterOp o = op(EMITTER_DICT_BEGIN);
    o.key = json_key;
    o.key2 = table_header;
    return o;
}
inline EmitterOp dictEnd()
{
    return op(EMITTER_DICT_END);
}
inline EmitterOp rowInit(int row)
{
    EmitterOp o = op(EMITTER_ROW_INIT);
    o.row = row;
    return o;
}
inline EmitterOp columnInit(int row, int column, int justify, int width)
{
    EmitterOp o = op(EMITTER_COLUMN_INIT);
    o.row = row;
    o.column = column;
    o.justify = justify;
    o.width = width;
    return o;
}
inline EmitterOp columnSet(int column, EmitterValue v)
{
    EmitterOp o = op(EMITTER_COLUMN_SET);
    o.column = column;
    o.v = v;
    return o;
}
inline EmitterOp tableRow(int row)
{
    EmitterOp o = op(EMITTER_TABLE_ROW);
    o.row = row;
    return o;
}

/// The scripts of jemalloc's test/unit/emitter.c.
inline const char * const long_str = "abcdefghijklmnopqrstuvwxyz "
                                     "abcdefghijklmnopqrstuvwxyz "
                                     "abcdefghijklmnopqrstuvwxyz "
                                     "abcdefghijklmnopqrstuvwxyz "
                                     "abcdefghijklmnopqrstuvwxyz "
                                     "abcdefghijklmnopqrstuvwxyz "
                                     "abcdefghijklmnopqrstuvwxyz "
                                     "abcdefghijklmnopqrstuvwxyz "
                                     "abcdefghijklmnopqrstuvwxyz "
                                     "abcdefghijklmnopqrstuvwxyz";

inline const EmitterOp script_dict[] = {
    begin(),
    dictBegin("foo", "This is the foo table:"),
    keyValue("abc", "ABC", boolValue(false)),
    keyValue("def", "DEF", boolValue(true)),
    keyValueNote("ghi", "GHI", intValue(123), "note_key1", stringValue("a string")),
    keyValueNote("jkl", "JKL", stringValue("a string"), "note_key2", boolValue(false)),
    dictEnd(),
    end(),
};

inline const EmitterOp script_table_printf[] = {
    begin(),
    tablePrintf("Table note 1\n"),
    tablePrintfS("Table note 2 %s\n", "with format string"),
    end(),
};

inline const EmitterOp script_nested_dict[] = {
    begin(),
    dictBegin("json1", "Dict 1"),
    dictBegin("json2", "Dict 2"),
    keyValue("primitive", "A primitive", intValue(123)),
    dictEnd(),
    dictBegin("json3", "Dict 3"),
    dictEnd(),
    dictEnd(),
    dictBegin("json4", "Dict 4"),
    keyValue("primitive", "Another primitive", intValue(123)),
    dictEnd(),
    end(),
};

inline const EmitterOp script_types[] = {
    begin(),
    keyValue("k1", "K1", boolValue(false)),
    keyValue("k2", "K2", intValue(-123)),
    keyValue("k3", "K3", unsignedValue(123)),
    keyValue("k4", "K4", ssizeValue(-456)),
    keyValue("k5", "K5", sizeValue(456)),
    keyValue("k6", "K6", stringValue("string")),
    keyValue("k7", "K7", stringValue(long_str)),
    keyValue("k8", "K8", uint32Value(789)),
    keyValue("k9", "K9", uint64Value(10000000000ULL)),
    end(),
};

inline const EmitterOp script_modal[] = {
    begin(),
    dictBegin("j0", "T0"),
    jsonKey("j1"),
    jsonObjectBegin(),
    keyValue("i1", "I1", intValue(123)),
    jsonKeyValue("i2", intValue(123)),
    tableKeyValue("I3", intValue(123)),
    tableDictBegin("T1"),
    keyValue("i4", "I4", intValue(123)),
    jsonObjectEnd(),
    keyValue("i5", "I5", intValue(123)),
    tableDictEnd(),
    keyValue("i6", "I6", intValue(123)),
    dictEnd(),
    end(),
};

inline const EmitterOp script_json_array[] = {
    begin(),
    jsonKey("dict"),
    jsonObjectBegin(),
    jsonKey("arr"),
    jsonArrayBegin(),
    jsonObjectBegin(),
    jsonKeyValue("foo", intValue(123)),
    jsonObjectEnd(),
    jsonValue(intValue(123)),
    jsonValue(intValue(123)),
    jsonObjectBegin(),
    jsonKeyValue("bar", intValue(123)),
    jsonKeyValue("baz", intValue(123)),
    jsonObjectEnd(),
    jsonArrayEnd(),
    jsonObjectEnd(),
    end(),
};

inline const EmitterOp script_json_nested_array[] = {
    begin(),
    jsonArrayBegin(),
    jsonArrayBegin(),
    jsonValue(intValue(123)),
    jsonValue(stringValue("foo")),
    jsonValue(intValue(123)),
    jsonValue(stringValue("foo")),
    jsonArrayEnd(),
    jsonArrayBegin(),
    jsonValue(intValue(123)),
    jsonArrayEnd(),
    jsonArrayBegin(),
    jsonValue(stringValue("foo")),
    jsonValue(intValue(123)),
    jsonArrayEnd(),
    jsonArrayBegin(),
    jsonArrayEnd(),
    jsonArrayEnd(),
    end(),
};

inline const EmitterOp script_table_row[] = {
    begin(),
    rowInit(0),
    columnSet(0, titleValue("ABC title")),
    columnSet(1, titleValue("DEF title")),
    columnSet(2, titleValue("GHI")),
    columnInit(0, 0, EMITTER_JUSTIFY_LEFT, 10),
    columnInit(0, 1, EMITTER_JUSTIFY_RIGHT, 15),
    columnInit(0, 2, EMITTER_JUSTIFY_RIGHT, 5),
    tableRow(0),
    columnSet(0, intValue(123)),
    columnSet(1, boolValue(true)),
    columnSet(2, intValue(456)),
    tableRow(0),
    columnSet(0, intValue(789)),
    columnSet(1, boolValue(false)),
    columnSet(2, intValue(1011)),
    tableRow(0),
    columnSet(0, stringValue("a string")),
    columnSet(1, boolValue(false)),
    columnSet(2, titleValue("ghi")),
    tableRow(0),
    end(),
};

}

#endif
