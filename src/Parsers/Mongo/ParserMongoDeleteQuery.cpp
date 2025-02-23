#include "ParserMongoDeleteQuery.h"

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
#include "Parsers/ASTDeleteQuery.h"


#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace DB
{

namespace Mongo
{

bool ParserMongoDeleteQuery::parseImpl(ASTPtr & node)
{
    auto delete_query = std::make_shared<ASTDeleteQuery>();
    node = delete_query;

    auto table_expression_ast = std::make_shared<ASTIdentifier>(metadata->getCollectionName());
    delete_query->table = table_expression_ast;

    /// Traverse data tree for WHERE operator
    ASTPtr where_condition;

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    data.Accept(writer);
    const char * json_string = buffer.GetString();

    if (ParserMongoFilter(std::move(data), metadata, "").parseImpl(where_condition))
    {
        delete_query->predicate = std::move(where_condition);
        std::cerr << "Parse OK " << json_string << '\n';
        return true;
    }
    else
    {
        std::cerr << "Parse NOT OK\n";
        return false;
    }
}

}

}
