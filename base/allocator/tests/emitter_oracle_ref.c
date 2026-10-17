/* Runs scripted operations on jemalloc's header-only emitter (`emitter.h`) for `emitter_oracle.cpp`. */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "jemalloc/internal/jemalloc_internal_includes.h"
#include "jemalloc/internal/jemalloc_preamble.h"

#include "jemalloc/internal/emitter.h"

#include "emitter_script.h"

#pragma clang diagnostic ignored "-Wformat-nonliteral"
#pragma clang diagnostic ignored "-Wformat-security"

void ref_emitter_run(int output, const struct EmitterOp * ops, size_t num_ops, EmitterWriteCallback * write_cb, void * opaque)
{
    emitter_t emitter;
    emitter_row_t rows[EMITTER_MAX_ROWS];
    emitter_col_t columns[EMITTER_MAX_COLUMNS];
    memset(rows, 0, sizeof(rows));
    memset(columns, 0, sizeof(columns));
    emitter_init(&emitter, (emitter_output_t)output, write_cb, opaque);

    for (size_t i = 0; i < num_ops; i++)
    {
        const struct EmitterOp * op = &ops[i];
        switch (op->op)
        {
            case EMITTER_BEGIN: emitter_begin(&emitter); break;
            case EMITTER_END: emitter_end(&emitter); break;
            case EMITTER_JSON_KEY: emitter_json_key(&emitter, op->key); break;
            case EMITTER_JSON_VALUE: emitter_json_value(&emitter, (emitter_type_t)op->v.type, &op->v.value); break;
            case EMITTER_JSON_KEY_VALUE: emitter_json_kv(&emitter, op->key, (emitter_type_t)op->v.type, &op->v.value); break;
            case EMITTER_JSON_ARRAY_BEGIN: emitter_json_array_begin(&emitter); break;
            case EMITTER_JSON_ARRAY_KEY_VALUE_BEGIN: emitter_json_array_kv_begin(&emitter, op->key); break;
            case EMITTER_JSON_ARRAY_END: emitter_json_array_end(&emitter); break;
            case EMITTER_JSON_OBJECT_BEGIN: emitter_json_object_begin(&emitter); break;
            case EMITTER_JSON_OBJECT_KEY_VALUE_BEGIN: emitter_json_object_kv_begin(&emitter, op->key); break;
            case EMITTER_JSON_OBJECT_END: emitter_json_object_end(&emitter); break;
            case EMITTER_TABLE_DICT_BEGIN: emitter_table_dict_begin(&emitter, op->key); break;
            case EMITTER_TABLE_DICT_END: emitter_table_dict_end(&emitter); break;
            case EMITTER_TABLE_KEY_VALUE_NOTE:
                emitter_table_kv_note(
                    &emitter,
                    op->key,
                    (emitter_type_t)op->v.type,
                    &op->v.value,
                    op->note_key,
                    (emitter_type_t)op->note.type,
                    &op->note.value);
                break;
            case EMITTER_TABLE_KEY_VALUE: emitter_table_kv(&emitter, op->key, (emitter_type_t)op->v.type, &op->v.value); break;
            case EMITTER_TABLE_PRINTF: emitter_table_printf(&emitter, op->key); break;
            case EMITTER_TABLE_PRINTF_S: emitter_table_printf(&emitter, op->key, op->key2); break;
            case EMITTER_TABLE_PRINTF_U64: emitter_table_printf(&emitter, op->key, op->v.value.u64); break;
            case EMITTER_KEY_VALUE_NOTE:
                emitter_kv_note(
                    &emitter,
                    op->key,
                    op->key2,
                    (emitter_type_t)op->v.type,
                    &op->v.value,
                    op->note_key,
                    (emitter_type_t)op->note.type,
                    &op->note.value);
                break;
            case EMITTER_KEY_VALUE: emitter_kv(&emitter, op->key, op->key2, (emitter_type_t)op->v.type, &op->v.value); break;
            case EMITTER_DICT_BEGIN: emitter_dict_begin(&emitter, op->key, op->key2); break;
            case EMITTER_DICT_END: emitter_dict_end(&emitter); break;
            case EMITTER_ROW_INIT: emitter_row_init(&rows[op->row]); break;
            case EMITTER_COLUMN_INIT:
                columns[op->column].justify = (emitter_justify_t)op->justify;
                columns[op->column].width = op->width;
                emitter_col_init(&columns[op->column], &rows[op->row]);
                break;
            case EMITTER_COLUMN_SET:
                columns[op->column].type = (emitter_type_t)op->v.type;
                memcpy(&columns[op->column].bool_val, &op->v.value, sizeof(op->v.value));
                break;
            case EMITTER_TABLE_ROW: emitter_table_row(&emitter, &rows[op->row]); break;
            default: break;
        }
    }
}
