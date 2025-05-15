#pragma once

#include <cstddef>
#include <cstdint>

extern "C" {

enum class RustLanceColumnType : uint8_t
{
    Unknown = 0,
    Int8 = 1,
    UInt8 = 2,
    Int16 = 3,
    UInt16 = 4,
    Int32 = 5,
    UInt32 = 6,
    Int64 = 7,
    UInt64 = 8,
    Float32 = 9,
    Float64 = 10,
    String = 11,
};

typedef struct RustLanceDatabase RustLanceDatabase;
typedef struct RustLanceTable RustLanceTable;
typedef struct RustLanceReader RustLanceReader;
typedef struct RustLanceBatch RustLanceBatch;

typedef struct RustLanceColumnDescription
{
    const char * name;
    RustLanceColumnType data_type;
    bool is_nullable;
} RustLanceColumnDescription;


typedef struct RustLanceSchema
{
    RustLanceColumnDescription * data;
    size_t len;
    size_t capacity;
} RustLanceSchema;

typedef struct RustLanceColumn
{
    RustLanceColumnType data_type;
    void * data;
    size_t len;
    size_t capacity;
} RustLanceColumn;

typedef struct NullableRustLanceColumn
{
    RustLanceColumn column;
    bool * nulls;
} NullableRustLanceColumn;

RustLanceDatabase * rust_connect_to_database(const char * database_path);
void rust_free_lance_database(RustLanceDatabase * database);

RustLanceTable * rust_try_open_table(RustLanceDatabase * database, const char * table_name);
RustLanceTable * rust_create_table_with_schema(RustLanceDatabase * database, const char * table_name, RustLanceSchema schema);
bool rust_drop_lance_table(RustLanceDatabase * database, const char * table_name);

void rust_free_lance_table(RustLanceTable * table);

RustLanceSchema rust_read_lance_schema(RustLanceTable * table);
void rust_free_lance_schema(RustLanceSchema schema);

RustLanceReader * rust_create_lance_reader(RustLanceTable * table);
void rust_free_lance_reader(RustLanceReader * reader);

bool rust_read_next_batch(RustLanceReader * reader);
size_t rust_get_rows_in_current_batch(RustLanceReader * reader);
RustLanceColumn rust_get_lance_column(RustLanceReader * reader, size_t col);
NullableRustLanceColumn rust_get_nullable_lance_column(RustLanceReader * reader, size_t col);
void rust_free_lance_column(RustLanceColumn column);
void rust_free_nullable_lance_column(NullableRustLanceColumn column);

RustLanceBatch * rust_create_lance_batch();
void rust_free_lance_batch(RustLanceBatch * batch);

void rust_set_schema_for_lance_batch(RustLanceBatch * batch, RustLanceSchema schema);
void rust_append_column_to_lance_batch(RustLanceBatch * batch, RustLanceColumn column);
void rust_append_nullable_column_to_lance_batch(RustLanceBatch * batch, NullableRustLanceColumn column);
void rust_write_batch_to_lance_table(RustLanceTable * table, RustLanceBatch * batch);

void rust_rename_column(RustLanceTable * table, const char * old_name, const char * new_name);
void rust_drop_column(RustLanceTable * table, const char * column_name);
} // extern "C"


namespace lance
{

using ColumnType = RustLanceColumnType;
using Database = RustLanceDatabase;
using Table = RustLanceTable;
using Reader = RustLanceReader;
using ColumnDescription = RustLanceColumnDescription;
using Schema = RustLanceSchema;
using Column = RustLanceColumn;
using NullableColumn = NullableRustLanceColumn;
using Batch = RustLanceBatch;
using ColumnType = RustLanceColumnType;

inline Database * connect_to_database(const char * database_path)
{
    return rust_connect_to_database(database_path);
}

inline void free_database(Database * database)
{
    rust_free_lance_database(database);
}

inline Table * try_open_table(Database * database, const char * table_name)
{
    return rust_try_open_table(database, table_name);
}

inline Table * create_table_with_schema(Database * database, const char * table_name, const Schema & schema)
{
    return rust_create_table_with_schema(database, table_name, schema);
}

inline bool drop_table(Database * database, const char * table_name)
{
    return rust_drop_lance_table(database, table_name);
}

inline void free_table(Table * table)
{
    rust_free_lance_table(table);
}

inline Schema read_schema(Table * table)
{
    return rust_read_lance_schema(table);
}

inline void free_schema(const Schema & schema)
{
    rust_free_lance_schema(schema);
}

inline Reader * create_reader(Table * table)
{
    return rust_create_lance_reader(table);
}

inline void free_reader(Reader * reader)
{
    rust_free_lance_reader(reader);
}

inline bool read_next_batch(Reader * reader)
{
    return rust_read_next_batch(reader);
}

inline size_t get_rows_in_current_batch(Reader * reader)
{
    return rust_get_rows_in_current_batch(reader);
}

inline Column get_column(Reader * reader, size_t col)
{
    return rust_get_lance_column(reader, col);
}

inline NullableColumn get_nullable_column(Reader * reader, size_t col)
{
    return rust_get_nullable_lance_column(reader, col);
}

inline void free_column(const Column & column)
{
    rust_free_lance_column(column);
}

inline void free_nullable_column(const NullableColumn & column)
{
    rust_free_nullable_lance_column(column);
}

inline Batch * create_batch()
{
    return rust_create_lance_batch();
}

inline void free_batch(Batch * batch)
{
    rust_free_lance_batch(batch);
}

inline void set_schema_for_batch(Batch * batch, const Schema & schema)
{
    rust_set_schema_for_lance_batch(batch, schema);
}

inline void append_column_to_batch(Batch * batch, const Column & column)
{
    rust_append_column_to_lance_batch(batch, column);
}

inline void append_nullable_column_to_batch(Batch * batch, const NullableColumn & column)
{
    rust_append_nullable_column_to_lance_batch(batch, column);
}

inline void write_batch_to_table(Table * table, Batch * batch)
{
    rust_write_batch_to_lance_table(table, batch);
}

inline void rename_column(Table * table, const char * old_name, const char * new_name)
{
    rust_rename_column(table, old_name, new_name);
}

inline void drop_column(Table * table, const char * column_name)
{
    rust_drop_column(table, column_name);
}

} // namespace lance
