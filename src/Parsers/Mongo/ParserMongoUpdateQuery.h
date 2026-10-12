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

/** A nested document is stored as one column per leaf, named by its dotted path. `$set` of a
  * subdocument expands into these leaves by the shape of the value it assigns, but `$unset` and
  * `$rename` name only the path, and the parser does not know the columns of the collection. So
  * once they are known, the assignments of `ast` - the `ALTER TABLE ... UPDATE` of `updateMany` -
  * that `$unset` or `$rename` made for a path that is not a column itself are rewritten into one
  * per leaf of `columns`: `{"$unset": {"profile": ""}}` unsets `profile.name` and `profile.age`,
  * and `{"$rename": {"profile": "user"}}` renames them to `user.name` and `user.age`. This is the
  * same expansion the `update` command of the wire protocol makes. Any other query is left as is.
  */
void expandMongoSubtreeAssignments(ASTPtr & ast, const std::vector<std::string> & columns);

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
