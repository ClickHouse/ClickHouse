#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace Poco::JSON { class Object; }

namespace DB
{

/// Current thread-local depth limit configured by createFromJSON(json, max_depth, max_elements).
/// Returns 0 when no limit is active. Helpers that perform their own recursive parsing
/// (e.g. `Field::restoreFromDump` over `Array_/Tuple_/Map_` payloads) consult this value
/// to enforce the same depth bound on hostile input.
size_t getJSONDeserializationMaxDepth();

/// Returns true when the buffer [begin, end) starts with a `SET` token (case-insensitive,
/// followed by whitespace or end-of-input). Used as an escape hatch when
/// `dialect = clickhouse_json` is active so users can still send `SET dialect = ...`
/// queries in plain SQL to switch back to another dialect, instead of being locked
/// into JSON-only input.
bool isClickHouseJSONSetEscape(const char * begin, const char * end);

}
