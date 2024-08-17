#pragma once

#include <memory>
#include <Parsers/IParserBase.h>
#include <Parsers/Mongo/Metadata.h>
#include <Parsers/Mongo/ParserMongoQuery.h>

#include <Poco/JSON/Object.h>

namespace DB
{

namespace Mongo
{

class ParserMongoSelectQuery : public IMongoParser
{
public:
    explicit ParserMongoSelectQuery(rapidjson::Value data_, std::shared_ptr<QueryMetadata> metadata_)
        : IMongoParser(std::move(data_), metadata_, "")
    {
    }

    bool parseImpl(ASTPtr & node) override;

    ~ParserMongoSelectQuery() override = default;
};

}

}
