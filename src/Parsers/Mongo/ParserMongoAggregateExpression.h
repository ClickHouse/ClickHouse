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

/** The order of the documents a `$group` stage consumes, when a preceding `$sort` established one.
  *
  * Mongo defines the value of the accumulators `$first`, `$last`, `$push`, `$firstN` and `$lastN`
  * by the order of the documents of the group, so a `$sort` right before a `$group` is how a
  * pipeline asks for the earliest or the latest document of each key. A ClickHouse aggregate
  * function reads its input in whatever order the query happens to produce it in, so those
  * accumulators are lowered through the sort keys instead - `any` would answer with an arbitrary
  * row of the group.
  */
struct MongoGroupOrder
{
    /// The keys of the most recent `$sort`, in order, each with its direction: 1 or -1.
    std::vector<std::pair<ASTPtr, int>> keys;

    /** Whether those keys are still columns of the stream the `$group` reads. A stage that builds
      * new documents - a `$project`, a `$group` - may leave the key out of them, and then the
      * order of the stream, which Mongo keeps, cannot be named in the translated query.
      */
    bool keys_in_scope = true;

    bool empty() const { return keys.empty(); }
};

/** Translates an accumulator of a `$group` stage: `{"$sum": 1}`, `{"$avg": "$ResolutionWidth"}`,
  * `{"$addToSet": "$UserID"}` and so on. `order` is the order of the documents of the group, which
  * the order-sensitive accumulators are lowered through.
  */
ASTPtr parseMongoAccumulator(const rapidjson::Value & value, const MongoGroupOrder & order = {});

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
