#include "ParserMongoUpdateQuery.h"

#include <rapidjson/document.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IParserBase.h>

#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/ParserMongoOrderBy.h>
#include <Parsers/Mongo/ParserMongoProjection.h>
#include <Parsers/Mongo/Utils.h>
#include "Parsers/ASTAlterQuery.h"
#include "Parsers/ASTDeleteQuery.h"


#include "Parsers/Mongo/ParserMongoQuery.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace DB
{

namespace Mongo
{

bool ParserMongoUpdateQuery::parseImpl(ASTPtr & node)
{
    auto update_query = std::make_shared<ASTAlterCommand>();
    node = update_query;

    update_query->type = ASTAlterCommand::UPDATE;

    auto * iter = data.GetArray().Begin();
    rapidjson::Value filter_json = iter->GetObject();
    ++iter;
    rapidjson::Value update_json = iter->GetObject();


    ASTPtr where_condition;

    {
        rapidjson::StringBuffer writer_buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer_filter(writer_buffer);

        filter_json.Accept(writer_filter);
    }

    {
        rapidjson::StringBuffer writer_buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer_filter(writer_buffer);

        update_json.Accept(writer_filter);
    }


    if (ParserMongoFilter(std::move(filter_json), metadata, "").parseImpl(where_condition))
    {
        update_query->children.push_back(where_condition);
        update_query->predicate = where_condition.get();
    }
    else
    {
        return false;
    }

    ASTPtr update_operation;
    if (createParser(std::move(update_json), metadata, "")->parseImpl(update_operation))
    {
        update_query->children.push_back(update_operation);
        update_query->update_assignments = update_operation.get();
    }
    else
    {
        return false;
    }

    return true;
}

}

}
