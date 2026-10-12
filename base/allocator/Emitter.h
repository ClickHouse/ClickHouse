#pragma once

/// Structured output of the statistics: pretty JSON, compact JSON or a human-readable table.
/// jemalloc: `emitter.h`.
///
/// Every print goes through `printToCallbackV` (`malloc_vcprintf`): it formats into a `MALLOC_PRINTF_BUF_SIZE` stack
/// buffer (truncating longer output of a single call) and calls `write_callback` once per call; the sequence of callback
/// invocations is the same as in jemalloc. Strings are always quoted, without escaping.

#include <allocator/Common.h>
#include <allocator/Format.h>
#include <allocator/IntrusiveList.h>

#include <cstdarg>
#include <cstdint>
#include <sys/types.h>

namespace jemalloc
{

/// jemalloc: emitter_output_t
enum class EmitterOutput
{
    JSON,
    JSONCompact,
    Table,
};

/// jemalloc: emitter_justify_t
enum class EmitterJustify
{
    Left,
    Right,
    /// Not for users; just to pass to internal functions.
    None,
};

/// jemalloc: emitter_type_t
enum class EmitterType
{
    Bool,
    Int,
    Int64,
    Unsigned,
    Uint32,
    Uint64,
    Size,
    Ssize,
    String,
    /// A title is a column title in a table; it's just a string, but it's not quoted.
    Title,
};

struct EmitterRow;

/// A column of a table row. Columns are linked into their row (no allocations) and printed in insertion order.
/// jemalloc: emitter_col_t
struct EmitterColumn
{
    /// Filled in by the user.
    EmitterJustify justify = EmitterJustify::Left;
    int width = 0;
    EmitterType type = EmitterType::Bool;
    union
    {
        bool bool_value;
        int int_value;
        unsigned unsigned_value;
        uint32_t uint32_value;
        uint32_t uint32_t_value;
        uint64_t uint64_value;
        uint64_t uint64_t_value;
        size_t size_value;
        ssize_t ssize_value;
        const char * str_value;
    };

    /// Filled in by initialization.
    RingLink<EmitterColumn> link;

    EmitterColumn()
        : uint64_value(0)
        , link{nullptr, nullptr}
    {
    }

    EmitterColumn(const EmitterColumn &) = delete;
    EmitterColumn & operator=(const EmitterColumn &) = delete;

    /// Appends the column to `row`.
    /// jemalloc: emitter_col_init
    inline void init(EmitterRow & row);
};

/// jemalloc: emitter_row_t
struct EmitterRow
{
    IntrusiveList<EmitterColumn, &EmitterColumn::link> columns;

    /// jemalloc: emitter_row_init
    void init() { columns.init(); }
};

inline void EmitterColumn::init(EmitterRow & row)
{
    IntrusiveList<EmitterColumn, &EmitterColumn::link>::elementInit(this);
    row.columns.tailInsert(this);
}

/// jemalloc: emitter_t
class Emitter
{
public:
    /// jemalloc: emitter_init
    Emitter(EmitterOutput output_, WriteCallback * write_callback_, void * callback_argument_)
        : output(output_)
        , write_callback(write_callback_)
        , callback_argument(callback_argument_)
    {
    }

    Emitter(const Emitter &) = delete;
    Emitter & operator=(const Emitter &) = delete;

    /// jemalloc: emitter_init
    void init(EmitterOutput output_, WriteCallback * write_callback_, void * callback_argument_)
    {
        output = output_;
        write_callback = write_callback_;
        callback_argument = callback_argument_;
        item_at_depth = false;
        emitted_key = false;
        nesting_depth = 0;
    }

    /// jemalloc: emitter_outputs_json
    bool outputsJSON() const { return output == EmitterOutput::JSON || output == EmitterOutput::JSONCompact; }

    EmitterOutput getOutput() const { return output; }

    /// --- JSON public API ----------------------------------------------------------------------------------------

    /// Emits a key (e.g. as appears in an object). The next json entity emitted will be the corresponding value.
    /// jemalloc: emitter_json_key
    void jsonKey(const char * json_key)
    {
        if (outputsJSON())
        {
            jsonKeyPrefix();
            print("\"%s\":%s", json_key, output == EmitterOutput::JSONCompact ? "" : " ");
            emitted_key = true;
        }
    }

    /// jemalloc: emitter_json_value
    void jsonValue(EmitterType value_type, const void * value)
    {
        if (outputsJSON())
        {
            jsonKeyPrefix();
            printValue(EmitterJustify::None, -1, value_type, value);
            item_at_depth = true;
        }
    }

    /// Shorthand for calling `jsonKey` and then `jsonValue`.
    /// jemalloc: emitter_json_kv
    void jsonKeyValue(const char * json_key, EmitterType value_type, const void * value)
    {
        jsonKey(json_key);
        jsonValue(value_type, value);
    }

    /// jemalloc: emitter_json_array_begin
    void jsonArrayBegin()
    {
        if (outputsJSON())
        {
            jsonKeyPrefix();
            print("[");
            nestIncrement();
        }
    }

    /// Shorthand for calling `jsonKey` and then `jsonArrayBegin`.
    /// jemalloc: emitter_json_array_kv_begin
    void jsonArrayKeyValueBegin(const char * json_key)
    {
        jsonKey(json_key);
        jsonArrayBegin();
    }

    /// jemalloc: emitter_json_array_end
    void jsonArrayEnd()
    {
        if (outputsJSON())
        {
            ALLOCATOR_ASSERT(nesting_depth > 0);
            nestDecrement();
            if (output != EmitterOutput::JSONCompact)
            {
                print("\n");
                indent();
            }
            print("]");
        }
    }

    /// jemalloc: emitter_json_object_begin
    void jsonObjectBegin()
    {
        if (outputsJSON())
        {
            jsonKeyPrefix();
            print("{");
            nestIncrement();
        }
    }

    /// Shorthand for calling `jsonKey` and then `jsonObjectBegin`.
    /// jemalloc: emitter_json_object_kv_begin
    void jsonObjectKeyValueBegin(const char * json_key)
    {
        jsonKey(json_key);
        jsonObjectBegin();
    }

    /// jemalloc: emitter_json_object_end
    void jsonObjectEnd()
    {
        if (outputsJSON())
        {
            ALLOCATOR_ASSERT(nesting_depth > 0);
            nestDecrement();
            if (output != EmitterOutput::JSONCompact)
            {
                print("\n");
                indent();
            }
            print("}");
        }
    }

    /// --- Table public API ---------------------------------------------------------------------------------------

    /// jemalloc: emitter_table_dict_begin
    void tableDictBegin(const char * table_key)
    {
        if (output == EmitterOutput::Table)
        {
            indent();
            print("%s\n", table_key);
            nestIncrement();
        }
    }

    /// jemalloc: emitter_table_dict_end
    void tableDictEnd()
    {
        if (output == EmitterOutput::Table)
            nestDecrement();
    }

    /// jemalloc: emitter_table_kv_note
    void tableKeyValueNote(
        const char * table_key,
        EmitterType value_type,
        const void * value,
        const char * table_note_key,
        EmitterType table_note_value_type,
        const void * table_note_value)
    {
        if (output == EmitterOutput::Table)
        {
            indent();
            print("%s: ", table_key);
            printValue(EmitterJustify::None, -1, value_type, value);
            if (table_note_key != nullptr)
            {
                print(" (%s: ", table_note_key);
                printValue(EmitterJustify::None, -1, table_note_value_type, table_note_value);
                print(")");
            }
            print("\n");
        }
        item_at_depth = true;
    }

    /// jemalloc: emitter_table_kv
    void tableKeyValue(const char * table_key, EmitterType value_type, const void * value)
    {
        tableKeyValueNote(table_key, value_type, value, nullptr, EmitterType::Bool, nullptr);
    }

    /// Write to the emitter the given string, but only in table mode.
    /// jemalloc: emitter_table_printf
    void tablePrintf(const char * format_string, ...) ALLOCATOR_FORMAT_PRINTF(2, 3)
    {
        if (output == EmitterOutput::Table)
        {
            va_list args;
            va_start(args, format_string);
            printToCallbackV(write_callback, callback_argument, format_string, args);
            va_end(args);
        }
    }

    /// jemalloc: emitter_table_row
    void tableRow(const EmitterRow & row)
    {
        if (output != EmitterOutput::Table)
            return;
        for (EmitterColumn * column : row.columns)
            printValue(column->justify, column->width, column->type, static_cast<const void *>(&column->bool_value));
        tablePrintf("\n");
    }

    /// --- Generalized public API: emits using either JSON or table, according to the output mode. -----------------

    /// Note emits a different kv pair as well, but only in table mode. Omits the note if `table_note_key` is null.
    /// jemalloc: emitter_kv_note
    void keyValueNote(
        const char * json_key,
        const char * table_key,
        EmitterType value_type,
        const void * value,
        const char * table_note_key,
        EmitterType table_note_value_type,
        const void * table_note_value)
    {
        if (outputsJSON())
        {
            jsonKey(json_key);
            jsonValue(value_type, value);
        }
        else
        {
            tableKeyValueNote(table_key, value_type, value, table_note_key, table_note_value_type, table_note_value);
        }
        item_at_depth = true;
    }

    /// jemalloc: emitter_kv
    void keyValue(const char * json_key, const char * table_key, EmitterType value_type, const void * value)
    {
        keyValueNote(json_key, table_key, value_type, value, nullptr, EmitterType::Bool, nullptr);
    }

    /// jemalloc: emitter_dict_begin
    void dictBegin(const char * json_key, const char * table_header)
    {
        if (outputsJSON())
        {
            jsonKey(json_key);
            jsonObjectBegin();
        }
        else
        {
            tableDictBegin(table_header);
        }
    }

    /// jemalloc: emitter_dict_end
    void dictEnd()
    {
        if (outputsJSON())
            jsonObjectEnd();
        else
            tableDictEnd();
    }

    /// jemalloc: emitter_begin
    void begin()
    {
        if (outputsJSON())
        {
            ALLOCATOR_ASSERT(nesting_depth == 0);
            print("{");
            nestIncrement();
        }
        else
        {
            /// This guarantees that we always call write_callback at least once. This is useful if some invariant is
            /// established by each call to write_callback, but doesn't hold initially: e.g., some buffer holds a
            /// null-terminated string.
            print("%s", "");
        }
    }

    /// jemalloc: emitter_end
    void end()
    {
        if (outputsJSON())
        {
            ALLOCATOR_ASSERT(nesting_depth == 1);
            nestDecrement();
            print("%s", output == EmitterOutput::JSONCompact ? "}" : "\n}\n");
        }
    }

private:
    EmitterOutput output;
    /// The output information.
    WriteCallback * write_callback;
    void * callback_argument;
    int nesting_depth = 0;
    /// True if we've already emitted a value at the given depth.
    bool item_at_depth = false;
    /// True if we emitted a key and will emit corresponding value next.
    bool emitted_key = false;

    /// Write to the emitter the given string.
    /// jemalloc: emitter_printf
    void print(const char * format_string, ...) ALLOCATOR_FORMAT_PRINTF(2, 3)
    {
        va_list args;
        va_start(args, format_string);
        printToCallbackV(write_callback, callback_argument, format_string, args);
        va_end(args);
    }

    /// Builds a format string from `format_specifier` (e.g. `"%zu"`) with the given justification and width.
    /// jemalloc: emitter_gen_fmt
    static const char *
    generateFormatString(char * out_format_string, size_t out_size, const char * format_specifier, EmitterJustify justify, int width)
    {
        [[maybe_unused]] size_t written;
        format_specifier++;
        if (justify == EmitterJustify::None)
            written = format(out_format_string, out_size, "%%%s", format_specifier);
        else if (justify == EmitterJustify::Left)
            written = format(out_format_string, out_size, "%%-%d%s", width, format_specifier);
        else
            written = format(out_format_string, out_size, "%%%d%s", width, format_specifier);
        /// Only happens in case of bad format string, which *we* choose.
        ALLOCATOR_ASSERT(written < out_size);
        return out_format_string;
    }

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wformat-nonliteral"

    /// jemalloc: emitter_emit_str
    void emitStr(EmitterJustify justify, int width, char * format_string, size_t format_size, const char * str)
    {
        static constexpr size_t BUF_SIZE = 256;
        char buf[BUF_SIZE];
        size_t str_written = format(buf, BUF_SIZE, "\"%s\"", str);
        print(generateFormatString(format_string, format_size, "%s", justify, width), buf);
        if (str_written < BUF_SIZE)
            return;
        /// There is no support for long string justification at the moment as we output them partially with
        /// multiple `format` calls and justification will work correctly only within one call. Fortunately this is
        /// not a big concern as we don't use justification with long strings right now.
        ///
        /// We emitted leading quotation mark and trailing '\0', hence need to exclude extra characters from str
        /// shift.
        str += BUF_SIZE - 2;
        do
        {
            str_written = format(buf, BUF_SIZE, "%s\"", str);
            str += str_written >= BUF_SIZE ? BUF_SIZE - 1 : str_written;
            print(generateFormatString(format_string, format_size, "%s", justify, width), buf);
        } while (str_written >= BUF_SIZE);
    }

    /// Emit the given value type in the relevant encoding (so that the bool true gets mapped to json "true", but the
    /// string "true" gets mapped to json "\"true\"", for instance). Width is ignored if justify is `None`.
    /// jemalloc: emitter_print_value
    void printValue(EmitterJustify justify, int width, EmitterType value_type, const void * value)
    {
        static constexpr size_t FORMAT_SIZE = 10;
        /// We dynamically generate a format string to emit, to let us use the snprintf machinery.
        char format_string[FORMAT_SIZE];

        switch (value_type)
        {
            case EmitterType::Bool:
                print(
                    generateFormatString(format_string, FORMAT_SIZE, "%s", justify, width),
                    *static_cast<const bool *>(value) ? "true" : "false");
                break;
            case EmitterType::Int:
                print(generateFormatString(format_string, FORMAT_SIZE, "%d", justify, width), *static_cast<const int *>(value));
                break;
            case EmitterType::Int64:
                print(
                    generateFormatString(format_string, FORMAT_SIZE, "%" FORMAT_D64, justify, width), *static_cast<const int64_t *>(value));
                break;
            case EmitterType::Unsigned:
                print(generateFormatString(format_string, FORMAT_SIZE, "%u", justify, width), *static_cast<const unsigned *>(value));
                break;
            case EmitterType::Ssize:
                print(generateFormatString(format_string, FORMAT_SIZE, "%zd", justify, width), *static_cast<const ssize_t *>(value));
                break;
            case EmitterType::Size:
                print(generateFormatString(format_string, FORMAT_SIZE, "%zu", justify, width), *static_cast<const size_t *>(value));
                break;
            case EmitterType::String: emitStr(justify, width, format_string, FORMAT_SIZE, *static_cast<const char * const *>(value)); break;
            case EmitterType::Uint32:
                print(
                    generateFormatString(format_string, FORMAT_SIZE, "%" FORMAT_U32, justify, width),
                    *static_cast<const uint32_t *>(value));
                break;
            case EmitterType::Uint64:
                print(
                    generateFormatString(format_string, FORMAT_SIZE, "%" FORMAT_U64, justify, width),
                    *static_cast<const uint64_t *>(value));
                break;
            case EmitterType::Title:
                print(generateFormatString(format_string, FORMAT_SIZE, "%s", justify, width), *static_cast<const char * const *>(value));
                break;
        }
    }

#pragma clang diagnostic pop

    /// In json mode, tracks nesting state.
    /// jemalloc: emitter_nest_inc
    void nestIncrement()
    {
        nesting_depth++;
        item_at_depth = false;
    }

    /// jemalloc: emitter_nest_dec
    void nestDecrement()
    {
        nesting_depth--;
        item_at_depth = true;
    }

    /// jemalloc: emitter_indent
    void indent()
    {
        int amount = nesting_depth;
        const char * indent_str;
        ALLOCATOR_ASSERT(output != EmitterOutput::JSONCompact);
        if (output == EmitterOutput::JSON)
        {
            indent_str = "\t";
        }
        else
        {
            amount *= 2;
            indent_str = " ";
        }
        for (int i = 0; i < amount; i++)
            print("%s", indent_str);
    }

    /// jemalloc: emitter_json_key_prefix
    void jsonKeyPrefix()
    {
        ALLOCATOR_ASSERT(outputsJSON());
        if (emitted_key)
        {
            emitted_key = false;
            return;
        }
        if (item_at_depth)
            print(",");
        if (output != EmitterOutput::JSONCompact)
        {
            print("\n");
            indent();
        }
    }
};

}
