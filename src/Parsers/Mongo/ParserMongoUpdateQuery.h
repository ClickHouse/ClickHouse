#pragma once

#include <memory>

#include <Parsers/IParserBase.h>

#include <Parsers/Mongo/Metadata.h>
#include <Parsers/Mongo/ParserMongoQuery.h>

namespace DB
{

namespace Mongo
{

/** Translates the update statement of an `update`, a document of update operators such as
  * `{"$set": {"a": 1}, "$inc": {"b": 2}}`, into the assignments of an `ALTER TABLE ... UPDATE`.
  */
ASTPtr parseMongoUpdateStatement(const rapidjson::Value & update);

class ParserMongoUpdateQuery : public IMongoParser
{
public:
    explicit ParserMongoUpdateQuery(rapidjson::Value data_, std::shared_ptr<QueryMetadata> metadata_)
        : IMongoParser(std::move(data_), metadata_, "")
    {
    }

    bool parseImpl(ASTPtr & node) override;

    ~ParserMongoUpdateQuery() override = default;
};

}

}
