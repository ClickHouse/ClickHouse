#pragma once

#include "config.h"

#if USE_ARROW

#include <Columns/IColumn_fwd.h>
#include <Processors/Formats/Impl/ArrowIPC/RecordBatchDecoder.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Core/Names.h>
#include <DataTypes/IDataType.h>

#include <cstdint>

/// The Arrow C data interface, as specified in https://arrow.apache.org/docs/format/CDataInterface.html.
/// The guard is the one the specification prescribes, so that these declarations coexist with any other
/// copy of them (e.g. in the headers generated for the Rust libraries).
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

extern "C"
{

struct ArrowSchema
{
    const char * format;
    const char * name;
    const char * metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema ** children;
    struct ArrowSchema * dictionary;
    void (*release)(struct ArrowSchema *);
    void * private_data;
};

struct ArrowArray
{
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void ** buffers;
    struct ArrowArray ** children;
    struct ArrowArray * dictionary;
    void (*release)(struct ArrowArray *);
    void * private_data;
};

}

#endif

namespace DB
{
struct FormatSettings;
}

namespace DB::ArrowIPC
{
struct ArrowSchema;
}

namespace DB::ArrowIPC
{

/// Exports the schema of a record batch with the columns `names` of types `types`, as a struct whose
/// fields are the columns. The Arrow types are the ones the native `Arrow` output format writes with
/// `settings`, except that `LowCardinality` is always written as its plain values.
/// `out` must be released by the consumer with its `release` callback.
void exportSchemaToC(const Names & names, const DataTypes & types, const FormatSettings & settings, ::ArrowSchema & out);

/// Exports rows [begin, end) of `columns` as a record batch: a struct array whose children are the
/// columns, laid out as `exportSchemaToC` describes. The array owns a copy of the data, so the columns
/// may be destroyed while it is alive. `out` must be released by the consumer with its `release` callback.
/// Throws `TOO_LARGE_ARRAY_SIZE` when the range does not fit one Arrow record batch (32-bit offsets).
void exportRecordBatchToC(
    const Columns & columns,
    const DataTypes & types,
    const FormatSettings & settings,
    size_t begin,
    size_t end,
    ::ArrowArray & out);

/// Parses the schema of a record batch exported to the Arrow C data interface (a struct whose fields are the
/// columns) into the Arrow-free description of the native reader. `schema` is not released.
ArrowIPC::ArrowSchema importSchemaFromC(const ::ArrowSchema & schema);

/// Decodes a record batch of `schema` exported to the Arrow C data interface (a struct array whose children are
/// the columns) into ClickHouse columns, as the native `Arrow` format decodes a record batch message. Takes
/// ownership of `array`, which is released before returning, and marks it released. The buffers are not copied
/// before decoding. Dictionary-encoded fields are not supported.
RecordBatchDecoder::DecodedColumns importRecordBatchFromC(
    ::ArrowArray & array,
    const ArrowIPC::ArrowSchema & schema,
    const FormatSettings & settings,
    const RequestedTypes * target_types = nullptr);

}

#endif
