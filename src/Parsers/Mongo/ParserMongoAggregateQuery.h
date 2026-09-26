#pragma once

#include <Parsers/Mongo/ParserMongoQuery.h>

namespace DB
{

namespace Mongo
{

/** Translates a Mongo aggregation pipeline into a `SELECT` query.
  *
  * `db.<collection>.aggregate([<stage>, <stage>, ...])` becomes a chain of selects: every stage
  * fills a clause of the select being built, and a stage that needs a clause already filled wraps
  * that select into a subquery and continues on top of it. The order of the stages is therefore
  * preserved exactly, while the common pipelines - a `$match` followed by a `$group`, a `$sort`
  * and a `$limit` - still translate into a single flat select.
  */
class ParserMongoAggregateQuery : public IMongoParser
{
public:
    explicit ParserMongoAggregateQuery(rapidjson::Value data_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoParser(std::move(data_), metadata_, edge_name_)
    {
    }

    bool parseImpl(ASTPtr & node) override;
};

}

}
