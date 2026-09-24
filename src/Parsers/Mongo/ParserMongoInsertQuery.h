#pragma once

#include <Parsers/Mongo/ParserMongoQuery.h>

namespace DB
{

namespace Mongo
{

class ParserMongoInsertManyQuery : public IMongoParser
{
public:
    explicit ParserMongoInsertManyQuery(rapidjson::Value data_, std::shared_ptr<QueryMetadata> metadata_)
        : IMongoParser(std::move(data_), metadata_, "")
    {
    }

    bool parseImpl(ASTPtr & node) override;

    ~ParserMongoInsertManyQuery() override = default;
};

class ParserMongoInsertOneQuery : public IMongoParser
{
public:
    explicit ParserMongoInsertOneQuery(rapidjson::Value data_, std::shared_ptr<QueryMetadata> metadata_)
        : IMongoParser(std::move(data_), metadata_, "")
    {
    }

    bool parseImpl(ASTPtr & node) override;

    ~ParserMongoInsertOneQuery() override = default;
};

}

}
