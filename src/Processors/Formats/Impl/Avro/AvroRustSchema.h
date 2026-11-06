#pragma once

#include "config.h"

#if USE_AVRO_RUST

#include <DataTypes/IDataType.h>
#include <base/types.h>

#include <avro_rust.h>

#include <optional>
#include <string>
#include <string_view>

namespace DB::AvroRust
{

/// Capacity of the error message buffers passed to the Rust ABI.
constexpr size_t ERROR_BUFFER_SIZE = 512;

/// Maps the status returned by a function of the Rust ABI to an exception.
/// Non-negative statuses (`AVRO_OK`, `AVRO_NEED_MORE`, `AVRO_END`, `AVRO_NOT_FOUND`) are returned as is,
/// the caller decides what they mean. `err` is the NUL terminated message the function wrote (may be null).
///
///  * `AVRO_ERR_ENUM_INDEX_OUT_OF_RANGE`, `AVRO_ERR_UNION_INDEX_OUT_OF_RANGE`: `INCORRECT_DATA`.
///  * `AVRO_ERR_DATA_TOO_DEEP`: `TOO_DEEP_RECURSION`.
///  * everything else: `AVRO_EXCEPTION`, the message is prefixed with `avro::Exception: `.
Int32 check(Int32 status, const char * err);

/// Name of an Avro type as in the Avro specification (`avro::toString` of the legacy library).
const char * kindName(UInt8 kind);

class Schema;

/// A node of the schema graph: a thin handle (schema pointer and node id), valid while the `Schema` is alive.
/// Named types referenced by name are the same node as their definition, so a node may be reached by several
/// paths and may be its own descendant (recursive record). Strings and attributes returned by a node point
/// into the schema and stay valid until the schema is destroyed.
class Node
{
public:
    Node(const Schema & schema_, UInt32 id_) : schema(&schema_), node_id(id_) {}

    UInt32 id() const { return node_id; }
    const Schema & getSchema() const { return *schema; }

    AvroNodeInfo info() const;

    AvroKind kind() const { return static_cast<AvroKind>(info().kind); }
    AvroLogical logical() const { return static_cast<AvroLogical>(info().logical); }

    /// Fields of a record, branches of a union, 1 for an array and a map (the items / the values).
    UInt32 childCount() const { return info().child_count; }

    /// The `i`-th field type of a record, branch of a union, the items / values of an array / map (`i == 0`).
    Node child(UInt32 i) const;

    /// Name of the `i`-th field of a record.
    std::string_view fieldName(UInt32 i) const;

    /// The `i`-th symbol of an enum.
    std::string_view symbol(UInt32 i) const;

    bool hasName() const { return info().name != nullptr; }
    /// Full name (`namespace.name`) of a record, enum or fixed, empty for other types.
    std::string_view name() const;
    /// Name without the namespace, empty for types without a name.
    std::string_view simpleName() const;

    /// Attribute the schema compiler does not interpret. `field < 0`: of this node, otherwise of its `field`-th field.
    std::optional<AvroAttr> attr(std::string_view key, Int64 field = -1) const;

private:
    const Schema * schema;
    UInt32 node_id;
};

/// Owns a compiled `AvroSchema` of the Rust decoder. Not copyable and not movable: nodes point at it.
class Schema
{
public:
    /// Parses and compiles `json`. `max_depth` limits the nesting of the JSON and of the schema (0: only the internal hard cap).
    explicit Schema(std::string_view json, UInt32 max_depth = 256);
    ~Schema();

    Schema(const Schema &) = delete;
    Schema & operator=(const Schema &) = delete;
    Schema(Schema &&) = delete;
    Schema & operator=(Schema &&) = delete;

    /// The raw handle, for the ABI functions that take a schema (program compiler, OCF).
    const AvroSchema * get() const { return schema; }

    Node root() const { return Node(*this, root_id); }
    Node node(UInt32 id) const;
    /// Valid node ids are `[0, nodeCount())`.
    UInt32 nodeCount() const { return node_count; }

    /// Parsing Canonical Form, computed by the first call. It is not bounded by the size of the schema (a named type
    /// that is referenced many times is printed in full each time), so it is neither a cache key nor fit for a message:
    /// see `source` and `canonicalPrefix`.
    std::string_view canonical() const;

    /// At most `max_bytes` of the Parsing Canonical Form, followed by `...` if it is longer. Nothing beyond them is generated.
    std::string canonicalPrefix(size_t max_bytes) const;

    /// The JSON text the schema was parsed from. The schema is a function of these bytes alone, so they are an exact
    /// identity of it (the key of a cache), and their size is the size of the input.
    std::string_view source() const;

private:
    AvroSchema * schema = nullptr;
    UInt32 root_id = 0;
    UInt32 node_count = 0;
};

/// The ClickHouse type of the Avro type `node`, as the `Avro` schema inference does it:
/// `union[T]` is `T`, `union[T, null]` is `Nullable(T)` (`T` for a tuple if `!allow_nullable_tuple_type`),
/// a wider union is `Variant` (the `null` branch is implicit), a record is a named `Tuple`, a map has `String` keys.
/// Throws `BAD_ARGUMENTS` for a recursive record, `ILLEGAL_COLUMN` for a type with no ClickHouse counterpart and
/// `TOO_DEEP_RECURSION` if the types nest deeper than `MAX_AVRO_SCHEMA_DEPTH` (named types are followed by
/// reference, so a schema whose JSON is shallow can still nest arbitrarily deep).
DataTypePtr inferType(Node node, bool allow_nullable_tuple_type = true);

}

#endif
