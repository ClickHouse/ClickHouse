#pragma once

#include <rapidjson/document.h>

#include <Parsers/IParserBase.h>

#include <Parsers/Mongo/Metadata.h>
#include <Parsers/Mongo/ParserMongoQuery.h>

namespace DB
{

namespace Mongo
{

class ParserMongoProjection : public IMongoParser
{
public:
    explicit ParserMongoProjection(rapidjson::Value data_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoParser(std::move(data_), metadata_, edge_name_)
    {
    }

    bool parseImpl(ASTPtr & node) override;

    ~ParserMongoProjection() override = default;
};

}

}
