#pragma once

#include <string>
#include <vector>

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

private:
    /// Adds one member of the projection to `result`, or to `excluded` when it removes a field.
    /// `name` is the dotted path of the member, which is longer than its key in the nested form.
    bool addMember(const std::string & name, const rapidjson::Value & value, ASTPtr & result, std::vector<std::string> & excluded);
};

}

}
