#pragma once

#include <string>
#include <vector>

#include <rapidjson/document.h>

#include <Parsers/IAST_fwd.h>

namespace DB
{

namespace Mongo
{

/** Translates an expression of the Mongo aggregation language into a ClickHouse expression.
  *
  * An expression is a field path (`"$Referer"`, `"$_id.RegionID"`), a constant, an array, or an
  * operator document (`{"$toString": "$SearchEngineID"}`). A field path names the column of the
  * same name: the Mongo dialect maps a nested document field onto a column whose name contains
  * the dot, so `"$a.b"` is the column `a.b`.
  *
  * Throws when the expression uses an operator the dialect does not support - a silently dropped
  * operator would return a wrong result instead of an error.
  */
ASTPtr parseMongoAggregateExpression(const rapidjson::Value & value);

/** Translates an accumulator of a `$group` stage: `{"$sum": 1}`, `{"$avg": "$ResolutionWidth"}`,
  * `{"$addToSet": "$UserID"}` and so on.
  */
ASTPtr parseMongoAccumulator(const rapidjson::Value & value);

/** A field of a document produced by a stage: the expression and the name of the column it is
  * aliased to. One member of a `$project`, `$set` or `$group` document can expand into several
  * of these when its value is a nested document, because the dialect flattens a nested document
  * into columns whose names contain the dot.
  */
struct MongoProjectedField
{
    std::string name;
    ASTPtr expression;
};

/** Expands one member of a `$project` or `$set` document into the columns it produces.
  *
  * A nested document becomes one column per leaf (`{"a": {"b": "$X"}}` produces the column `a.b`),
  * and `$regexFind` becomes the `match`, `idx` and `captures` fields of its result document.
  */
void expandMongoProjectedField(const std::string & name, const rapidjson::Value & value, std::vector<MongoProjectedField> & result);

}

}
