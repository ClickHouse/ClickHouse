/* Runs scripted operations on jemalloc's header-only emitter (`emitter.h`) for `emitter_oracle.cpp`. */

#ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#endif

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/jemalloc_internal_includes.h"

#include "jemalloc/internal/emitter.h"

#include "emitter_script.h"

#pragma clang diagnostic ignored "-Wformat-nonliteral"
#pragma clang diagnostic ignored "-Wformat-security"

void ref_emitter_run(int output, const struct EmOp * ops, size_t nops, em_write_cb_t * write_cb, void * opaque)
{
    emitter_t emitter;
    emitter_row_t rows[EM_MAX_ROWS];
    emitter_col_t cols[EM_MAX_COLS];
    memset(rows, 0, sizeof(rows));
    memset(cols, 0, sizeof(cols));
    emitter_init(&emitter, (emitter_output_t)output, write_cb, opaque);

    for (size_t i = 0; i < nops; i++)
    {
        const struct EmOp * op = &ops[i];
        switch (op->op)
        {
            case EM_BEGIN: emitter_begin(&emitter); break;
            case EM_END: emitter_end(&emitter); break;
            case EM_JSON_KEY: emitter_json_key(&emitter, op->key); break;
            case EM_JSON_VALUE: emitter_json_value(&emitter, (emitter_type_t)op->v.type, &op->v.val); break;
            case EM_JSON_KV: emitter_json_kv(&emitter, op->key, (emitter_type_t)op->v.type, &op->v.val); break;
            case EM_JSON_ARRAY_BEGIN: emitter_json_array_begin(&emitter); break;
            case EM_JSON_ARRAY_KV_BEGIN: emitter_json_array_kv_begin(&emitter, op->key); break;
            case EM_JSON_ARRAY_END: emitter_json_array_end(&emitter); break;
            case EM_JSON_OBJECT_BEGIN: emitter_json_object_begin(&emitter); break;
            case EM_JSON_OBJECT_KV_BEGIN: emitter_json_object_kv_begin(&emitter, op->key); break;
            case EM_JSON_OBJECT_END: emitter_json_object_end(&emitter); break;
            case EM_TABLE_DICT_BEGIN: emitter_table_dict_begin(&emitter, op->key); break;
            case EM_TABLE_DICT_END: emitter_table_dict_end(&emitter); break;
            case EM_TABLE_KV_NOTE:
                emitter_table_kv_note(&emitter, op->key, (emitter_type_t)op->v.type, &op->v.val, op->note_key,
                    (emitter_type_t)op->note.type, &op->note.val);
                break;
            case EM_TABLE_KV: emitter_table_kv(&emitter, op->key, (emitter_type_t)op->v.type, &op->v.val); break;
            case EM_TABLE_PRINTF: emitter_table_printf(&emitter, op->key); break;
            case EM_TABLE_PRINTF_S: emitter_table_printf(&emitter, op->key, op->key2); break;
            case EM_TABLE_PRINTF_U64: emitter_table_printf(&emitter, op->key, op->v.val.u64); break;
            case EM_KV_NOTE:
                emitter_kv_note(&emitter, op->key, op->key2, (emitter_type_t)op->v.type, &op->v.val, op->note_key,
                    (emitter_type_t)op->note.type, &op->note.val);
                break;
            case EM_KV: emitter_kv(&emitter, op->key, op->key2, (emitter_type_t)op->v.type, &op->v.val); break;
            case EM_DICT_BEGIN: emitter_dict_begin(&emitter, op->key, op->key2); break;
            case EM_DICT_END: emitter_dict_end(&emitter); break;
            case EM_ROW_INIT: emitter_row_init(&rows[op->row]); break;
            case EM_COL_INIT:
                cols[op->col].justify = (emitter_justify_t)op->justify;
                cols[op->col].width = op->width;
                emitter_col_init(&cols[op->col], &rows[op->row]);
                break;
            case EM_COL_SET:
                cols[op->col].type = (emitter_type_t)op->v.type;
                memcpy(&cols[op->col].bool_val, &op->v.val, sizeof(op->v.val));
                break;
            case EM_TABLE_ROW: emitter_table_row(&emitter, &rows[op->row]); break;
            default: break;
        }
    }
}
