#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include <rapidjson/document.h>

#include <Parsers/IAST_fwd.h>

namespace DB
{

namespace Mongo
{

/** Translates a Mongo constant into a ClickHouse expression.
  *
  * A constant is either a plain JSON scalar or an Extended JSON wrapper such as
  * `{"$numberLong": "435090932899640449"}` or `{"$date": "2013-07-01"}`, which the Mongo drivers
  * and `mongosh` emit for the types JSON cannot represent. Returns `nullptr` when the value is
  * not a constant - an operator document, an array, or a nested document.
  */
ASTPtr tryParseMongoConstant(const rapidjson::Value & value);

/** The pattern of a Mongo regular expression, already translated into the RE2 syntax
  * ClickHouse uses: the Extended JSON form `{"$regularExpression": {"pattern": ..., "options": ...}}`
  * and the query form `{"$regex": ..., "$options": ...}` are both accepted, and the options
  * become the inline flags RE2 understands. Returns nullopt when the value is not a regular
  * expression.
  */
std::optional<std::string> tryParseMongoRegularExpression(const rapidjson::Value & value);

/** `pattern` with the Mongo regular expression `options` applied to it as the inline flags RE2
  * understands, e.g. `^a` and `i` become `(?i)^a`. An option RE2 has no counterpart for is an
  * error rather than a silently different match.
  */
std::string applyMongoRegularExpressionOptions(std::string_view pattern, std::string_view options);

/** Tells whether an object is an Extended JSON scalar wrapper, such as `{"$date": ...}` or
  * `{"$oid": "..."}`: the serialization of a BSON-only type, which is a value rather than a
  * subdocument. Mongo forbids `$` at the start of a stored field name, so no real subdocument
  * looks like this.
  */
bool isMongoExtendedJSONWrapper(const rapidjson::Value & value);

/** Converts an Extended JSON scalar wrapper into the type of the column that holds it and the
  * value to store there. A wrapper of a BSON type that has no ClickHouse counterpart is rejected
  * rather than descended into, which would turn the field into bogus `<field>.$<wrapper>` columns.
  * `field_name` only names the field in the error message and may be empty.
  */
std::pair<std::string, rapidjson::Value> convertMongoExtendedJSONWrapper(
    const rapidjson::Value & wrapper, std::string_view field_name, rapidjson::Document::AllocatorType & allocator);

/** Replaces every Extended JSON wrapper inside a value with the value it wraps, so that a wrapper
  * never reaches a stored document as a field whose name starts with `$`. The type the wrapper
  * named is dropped: a value this deep lands in a `JSON` or a `Dynamic` column, which keeps the
  * serialized form.
  */
rapidjson::Value convertMongoExtendedJSONWrappersDeep(
    const rapidjson::Value & value, std::string_view field_name, rapidjson::Document::AllocatorType & allocator);

/** An embedded document as a value of the `JSON` type: `CAST('{...}', 'JSON')`. This is how an
  * embedded document that is a value rather than a set of paths - an element of an array, or the
  * document `$push` appends - is written, and it matches the `JSON` column the wire insert path
  * infers for the same shape. The nested Extended JSON wrappers are converted the way the wire
  * path converts them, so that both surfaces store one and the same document.
  */
ASTPtr makeMongoJSONValue(const rapidjson::Value & value, std::string_view field_name = {});

}

}
