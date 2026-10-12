#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace Poco::JSON { class Object; }

namespace DB
{

/// Depth limit enforced during JSON AST deserialization: the thread-local limit configured by
/// createFromJSON(json, max_depth, max_elements), or a stack-safety ceiling when that limit is 0
/// (`max_ast_depth = 0` disables the semantic AST depth check, not the recursion bound).
/// Never returns 0. Helpers that perform their own recursive parsing (e.g. `Field::restoreFromDump`
/// over `Array_/Tuple_/Map_` payloads) consult this value to enforce the same depth bound on
/// hostile input.
size_t getJSONDeserializationMaxDepth();

/// Count one structured `Field` element (an `Array`/`Tuple`/`Map` entry deserialized by
/// `readFieldFromObject`) against the same element-count budget that `createFromJSON` enforces
/// for AST nodes. Such elements live inside a single `Literal` AST node and would otherwise let a
/// hostile literal payload bypass `max_ast_elements`. Throws `TOO_BIG_AST` when the budget is exceeded.
void countJSONDeserializationElement();

/// Remaining element budget for JSON AST deserialization (`max_ast_elements` minus the elements
/// counted so far). Returns `SIZE_MAX` when no limit is active. Used to bound up-front container
/// reserves (`children` arrays, structured `Field` Array/Tuple/Map) against the configured limit so
/// an untrusted array length cannot force a large allocation before the per-element budget check runs.
size_t getJSONDeserializationRemainingElements();

/// Returns true when the buffer [begin, end) starts with a `SET` token (case-insensitive,
/// followed by whitespace or end-of-input). Used as an escape hatch when
/// `dialect = clickhouse_json` is active so users can still send `SET dialect = ...`
/// queries in plain SQL to switch back to another dialect, instead of being locked
/// into JSON-only input.
bool isClickHouseJSONSetEscape(const char * begin, const char * end, size_t max_query_size);

}
