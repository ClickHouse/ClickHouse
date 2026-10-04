/* Scripted emitter operations, shared between `emitter.cpp`, `emitter_oracle.cpp` (C++ `Emitter`) and
 * `emitter_oracle_ref.c` (jemalloc's `emitter.h`). The numeric values of output modes, justifications and types are
 * those of the jemalloc enums (and of the C++ enum classes). */

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

enum EmOpCode
{
    EM_BEGIN,
    EM_END,
    EM_JSON_KEY,
    EM_JSON_VALUE,
    EM_JSON_KV,
    EM_JSON_ARRAY_BEGIN,
    EM_JSON_ARRAY_KV_BEGIN,
    EM_JSON_ARRAY_END,
    EM_JSON_OBJECT_BEGIN,
    EM_JSON_OBJECT_KV_BEGIN,
    EM_JSON_OBJECT_END,
    EM_TABLE_DICT_BEGIN,
    EM_TABLE_DICT_END,
    EM_TABLE_KV_NOTE,
    EM_TABLE_KV,
    EM_TABLE_PRINTF,     /* key is the format, no arguments */
    EM_TABLE_PRINTF_S,   /* key is the format, key2 the argument */
    EM_TABLE_PRINTF_U64, /* key is the format, v.u64 the argument */
    EM_KV_NOTE,
    EM_KV,
    EM_DICT_BEGIN,
    EM_DICT_END,
    EM_ROW_INIT,  /* row */
    EM_COL_INIT,  /* row, col, justify, width */
    EM_COL_SET,   /* col, v */
    EM_TABLE_ROW, /* row */
};

/* Value types (emitter_type_t). */
enum
{
    EM_T_BOOL,
    EM_T_INT,
    EM_T_INT64,
    EM_T_UNSIGNED,
    EM_T_UINT32,
    EM_T_UINT64,
    EM_T_SIZE,
    EM_T_SSIZE,
    EM_T_STRING,
    EM_T_TITLE,
};

/* Justification (emitter_justify_t). */
enum
{
    EM_J_LEFT,
    EM_J_RIGHT,
};

/* Output modes (emitter_output_t). */
enum
{
    EM_OUT_JSON,
    EM_OUT_JSON_COMPACT,
    EM_OUT_TABLE,
};

#define EM_MAX_ROWS 8
#define EM_MAX_COLS 64

struct EmValue
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
    } val;
};

struct EmOp
{
    int op;
    const char * key;
    const char * key2;
    struct EmValue v;
    const char * note_key;
    struct EmValue note;
    int row;
    int col;
    int justify;
    int width;
};

typedef void em_write_cb_t(void * opaque, const char * s);

#ifdef __cplusplus
extern "C"
{
#endif

void ref_emitter_run(int output, const struct EmOp * ops, size_t nops, em_write_cb_t * write_cb, void * opaque);

#ifdef __cplusplus
}

#    include <allocator/Emitter.h>

#    include <cstring>

/// The same interpreter for the C++ `Emitter`.
inline void newEmitterRun(int output, const EmOp * ops, size_t nops, em_write_cb_t * write_cb, void * opaque)
{
    using namespace jemalloc;
    Emitter emitter(static_cast<EmitterOutput>(output), write_cb, opaque);
    EmitterRow rows[EM_MAX_ROWS];
    EmitterCol cols[EM_MAX_COLS];
    auto type = [](const EmValue & v) { return static_cast<EmitterType>(v.type); };

    for (size_t i = 0; i < nops; ++i)
    {
        const EmOp & op = ops[i];
        switch (op.op)
        {
            case EM_BEGIN: emitter.begin(); break;
            case EM_END: emitter.end(); break;
            case EM_JSON_KEY: emitter.jsonKey(op.key); break;
            case EM_JSON_VALUE: emitter.jsonValue(type(op.v), &op.v.val); break;
            case EM_JSON_KV: emitter.jsonKv(op.key, type(op.v), &op.v.val); break;
            case EM_JSON_ARRAY_BEGIN: emitter.jsonArrayBegin(); break;
            case EM_JSON_ARRAY_KV_BEGIN: emitter.jsonArrayKvBegin(op.key); break;
            case EM_JSON_ARRAY_END: emitter.jsonArrayEnd(); break;
            case EM_JSON_OBJECT_BEGIN: emitter.jsonObjectBegin(); break;
            case EM_JSON_OBJECT_KV_BEGIN: emitter.jsonObjectKvBegin(op.key); break;
            case EM_JSON_OBJECT_END: emitter.jsonObjectEnd(); break;
            case EM_TABLE_DICT_BEGIN: emitter.tableDictBegin(op.key); break;
            case EM_TABLE_DICT_END: emitter.tableDictEnd(); break;
            case EM_TABLE_KV_NOTE:
                emitter.tableKvNote(op.key, type(op.v), &op.v.val, op.note_key, type(op.note), &op.note.val);
                break;
            case EM_TABLE_KV: emitter.tableKv(op.key, type(op.v), &op.v.val); break;
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wformat-nonliteral"
#    pragma clang diagnostic ignored "-Wformat-security"
            case EM_TABLE_PRINTF: emitter.tablePrintf(op.key); break;
            case EM_TABLE_PRINTF_S: emitter.tablePrintf(op.key, op.key2); break;
            case EM_TABLE_PRINTF_U64: emitter.tablePrintf(op.key, op.v.val.u64); break;
#    pragma clang diagnostic pop
            case EM_KV_NOTE:
                emitter.kvNote(op.key, op.key2, type(op.v), &op.v.val, op.note_key, type(op.note), &op.note.val);
                break;
            case EM_KV: emitter.kv(op.key, op.key2, type(op.v), &op.v.val); break;
            case EM_DICT_BEGIN: emitter.dictBegin(op.key, op.key2); break;
            case EM_DICT_END: emitter.dictEnd(); break;
            case EM_ROW_INIT: rows[op.row].init(); break;
            case EM_COL_INIT:
                cols[op.col].justify = static_cast<EmitterJustify>(op.justify);
                cols[op.col].width = op.width;
                cols[op.col].init(rows[op.row]);
                break;
            case EM_COL_SET:
                cols[op.col].type = type(op.v);
                std::memcpy(&cols[op.col].bool_val, &op.v.val, sizeof(op.v.val));
                break;
            case EM_TABLE_ROW: emitter.tableRow(rows[op.row]); break;
            default: break;
        }
    }
}

/// Builders for scripts.
namespace em
{

inline EmValue vBool(bool x) { EmValue v{}; v.type = EM_T_BOOL; v.val.b = x; return v; }
inline EmValue vInt(int x) { EmValue v{}; v.type = EM_T_INT; v.val.i = x; return v; }
inline EmValue vInt64(int64_t x) { EmValue v{}; v.type = EM_T_INT64; v.val.i64 = x; return v; }
inline EmValue vUnsigned(unsigned x) { EmValue v{}; v.type = EM_T_UNSIGNED; v.val.u = x; return v; }
inline EmValue vUint32(uint32_t x) { EmValue v{}; v.type = EM_T_UINT32; v.val.u32 = x; return v; }
inline EmValue vUint64(uint64_t x) { EmValue v{}; v.type = EM_T_UINT64; v.val.u64 = x; return v; }
inline EmValue vSize(size_t x) { EmValue v{}; v.type = EM_T_SIZE; v.val.zu = x; return v; }
inline EmValue vSsize(ssize_t x) { EmValue v{}; v.type = EM_T_SSIZE; v.val.zd = x; return v; }
inline EmValue vString(const char * x) { EmValue v{}; v.type = EM_T_STRING; v.val.s = x; return v; }
inline EmValue vTitle(const char * x) { EmValue v{}; v.type = EM_T_TITLE; v.val.s = x; return v; }

inline EmOp op(int code) { EmOp o{}; o.op = code; return o; }
inline EmOp begin() { return op(EM_BEGIN); }
inline EmOp end() { return op(EM_END); }
inline EmOp jsonKey(const char * k) { EmOp o = op(EM_JSON_KEY); o.key = k; return o; }
inline EmOp jsonValue(EmValue v) { EmOp o = op(EM_JSON_VALUE); o.v = v; return o; }
inline EmOp jsonKv(const char * k, EmValue v) { EmOp o = op(EM_JSON_KV); o.key = k; o.v = v; return o; }
inline EmOp jsonArrayBegin() { return op(EM_JSON_ARRAY_BEGIN); }
inline EmOp jsonArrayKvBegin(const char * k) { EmOp o = op(EM_JSON_ARRAY_KV_BEGIN); o.key = k; return o; }
inline EmOp jsonArrayEnd() { return op(EM_JSON_ARRAY_END); }
inline EmOp jsonObjectBegin() { return op(EM_JSON_OBJECT_BEGIN); }
inline EmOp jsonObjectKvBegin(const char * k) { EmOp o = op(EM_JSON_OBJECT_KV_BEGIN); o.key = k; return o; }
inline EmOp jsonObjectEnd() { return op(EM_JSON_OBJECT_END); }
inline EmOp tableDictBegin(const char * k) { EmOp o = op(EM_TABLE_DICT_BEGIN); o.key = k; return o; }
inline EmOp tableDictEnd() { return op(EM_TABLE_DICT_END); }
inline EmOp tableKvNote(const char * k, EmValue v, const char * nk, EmValue nv)
{
    EmOp o = op(EM_TABLE_KV_NOTE); o.key = k; o.v = v; o.note_key = nk; o.note = nv; return o;
}
inline EmOp tableKv(const char * k, EmValue v) { EmOp o = op(EM_TABLE_KV); o.key = k; o.v = v; return o; }
inline EmOp tablePrintf(const char * fmt) { EmOp o = op(EM_TABLE_PRINTF); o.key = fmt; return o; }
inline EmOp tablePrintfS(const char * fmt, const char * s) { EmOp o = op(EM_TABLE_PRINTF_S); o.key = fmt; o.key2 = s; return o; }
inline EmOp tablePrintfU64(const char * fmt, uint64_t x) { EmOp o = op(EM_TABLE_PRINTF_U64); o.key = fmt; o.v = vUint64(x); return o; }
inline EmOp kvNote(const char * jk, const char * tk, EmValue v, const char * nk, EmValue nv)
{
    EmOp o = op(EM_KV_NOTE); o.key = jk; o.key2 = tk; o.v = v; o.note_key = nk; o.note = nv; return o;
}
inline EmOp kv(const char * jk, const char * tk, EmValue v) { EmOp o = op(EM_KV); o.key = jk; o.key2 = tk; o.v = v; return o; }
inline EmOp dictBegin(const char * jk, const char * th) { EmOp o = op(EM_DICT_BEGIN); o.key = jk; o.key2 = th; return o; }
inline EmOp dictEnd() { return op(EM_DICT_END); }
inline EmOp rowInit(int row) { EmOp o = op(EM_ROW_INIT); o.row = row; return o; }
inline EmOp colInit(int row, int col, int justify, int width)
{
    EmOp o = op(EM_COL_INIT); o.row = row; o.col = col; o.justify = justify; o.width = width; return o;
}
inline EmOp colSet(int col, EmValue v) { EmOp o = op(EM_COL_SET); o.col = col; o.v = v; return o; }
inline EmOp tableRow(int row) { EmOp o = op(EM_TABLE_ROW); o.row = row; return o; }

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

inline const EmOp script_dict[] = {
    begin(),
    dictBegin("foo", "This is the foo table:"),
    kv("abc", "ABC", vBool(false)),
    kv("def", "DEF", vBool(true)),
    kvNote("ghi", "GHI", vInt(123), "note_key1", vString("a string")),
    kvNote("jkl", "JKL", vString("a string"), "note_key2", vBool(false)),
    dictEnd(),
    end(),
};

inline const EmOp script_table_printf[] = {
    begin(),
    tablePrintf("Table note 1\n"),
    tablePrintfS("Table note 2 %s\n", "with format string"),
    end(),
};

inline const EmOp script_nested_dict[] = {
    begin(),
    dictBegin("json1", "Dict 1"),
    dictBegin("json2", "Dict 2"),
    kv("primitive", "A primitive", vInt(123)),
    dictEnd(),
    dictBegin("json3", "Dict 3"),
    dictEnd(),
    dictEnd(),
    dictBegin("json4", "Dict 4"),
    kv("primitive", "Another primitive", vInt(123)),
    dictEnd(),
    end(),
};

inline const EmOp script_types[] = {
    begin(),
    kv("k1", "K1", vBool(false)),
    kv("k2", "K2", vInt(-123)),
    kv("k3", "K3", vUnsigned(123)),
    kv("k4", "K4", vSsize(-456)),
    kv("k5", "K5", vSize(456)),
    kv("k6", "K6", vString("string")),
    kv("k7", "K7", vString(long_str)),
    kv("k8", "K8", vUint32(789)),
    kv("k9", "K9", vUint64(10000000000ULL)),
    end(),
};

inline const EmOp script_modal[] = {
    begin(),
    dictBegin("j0", "T0"),
    jsonKey("j1"),
    jsonObjectBegin(),
    kv("i1", "I1", vInt(123)),
    jsonKv("i2", vInt(123)),
    tableKv("I3", vInt(123)),
    tableDictBegin("T1"),
    kv("i4", "I4", vInt(123)),
    jsonObjectEnd(),
    kv("i5", "I5", vInt(123)),
    tableDictEnd(),
    kv("i6", "I6", vInt(123)),
    dictEnd(),
    end(),
};

inline const EmOp script_json_array[] = {
    begin(),
    jsonKey("dict"),
    jsonObjectBegin(),
    jsonKey("arr"),
    jsonArrayBegin(),
    jsonObjectBegin(),
    jsonKv("foo", vInt(123)),
    jsonObjectEnd(),
    jsonValue(vInt(123)),
    jsonValue(vInt(123)),
    jsonObjectBegin(),
    jsonKv("bar", vInt(123)),
    jsonKv("baz", vInt(123)),
    jsonObjectEnd(),
    jsonArrayEnd(),
    jsonObjectEnd(),
    end(),
};

inline const EmOp script_json_nested_array[] = {
    begin(),
    jsonArrayBegin(),
    jsonArrayBegin(),
    jsonValue(vInt(123)),
    jsonValue(vString("foo")),
    jsonValue(vInt(123)),
    jsonValue(vString("foo")),
    jsonArrayEnd(),
    jsonArrayBegin(),
    jsonValue(vInt(123)),
    jsonArrayEnd(),
    jsonArrayBegin(),
    jsonValue(vString("foo")),
    jsonValue(vInt(123)),
    jsonArrayEnd(),
    jsonArrayBegin(),
    jsonArrayEnd(),
    jsonArrayEnd(),
    end(),
};

inline const EmOp script_table_row[] = {
    begin(),
    rowInit(0),
    colSet(0, vTitle("ABC title")),
    colSet(1, vTitle("DEF title")),
    colSet(2, vTitle("GHI")),
    colInit(0, 0, EM_J_LEFT, 10),
    colInit(0, 1, EM_J_RIGHT, 15),
    colInit(0, 2, EM_J_RIGHT, 5),
    tableRow(0),
    colSet(0, vInt(123)),
    colSet(1, vBool(true)),
    colSet(2, vInt(456)),
    tableRow(0),
    colSet(0, vInt(789)),
    colSet(1, vBool(false)),
    colSet(2, vInt(1011)),
    tableRow(0),
    colSet(0, vString("a string")),
    colSet(1, vBool(false)),
    colSet(2, vTitle("ghi")),
    tableRow(0),
    end(),
};

}

#endif
