#pragma once

#include <rapidjson/document.h>

#include <Parsers/IParserBase.h>

#include <Parsers/Mongo/Metadata.h>
#include <Parsers/Mongo/ParserMongoQuery.h>

namespace DB
{

namespace Mongo
{

class ParserMongoFilter : public IMongoParser
{
public:
    explicit ParserMongoFilter(rapidjson::Value data_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoParser(std::move(data_), metadata_, edge_name_)
    {
    }

    bool parseImpl(ASTPtr & node) override;

    ~ParserMongoFilter() override = default;
};

}

}
