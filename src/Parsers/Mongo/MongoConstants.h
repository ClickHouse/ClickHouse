#pragma once

#include <optional>
#include <string>
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

}

}
