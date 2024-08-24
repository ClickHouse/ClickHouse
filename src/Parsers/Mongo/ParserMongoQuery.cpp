#include "ParserMongoQuery.h"

#include <memory>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWithElement.h>

#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/ParserMongoFunction.h>
#include <Parsers/Mongo/ParserMongoSelectQuery.h>
#include <Parsers/Mongo/Metadata.h>
#include <Parsers/Mongo/ParserMongoInsertQuery.h>
#include <Parsers/Mongo/Utils.h>
#include <Parsers/Mongo/ParserMongoCompareFunctions.h>

namespace DB
{

namespace Mongo
{

bool ParserMongoQuery::parseImpl(Pos & /*pos*/, ASTPtr & node, Expected & /*expected*/)
{
    switch (metadata->getQueryType())
    {
        case QueryMetadata::QueryType::select:
        {
            return ParserMongoSelectQuery(std::move(data), metadata).parseImpl(node);
        }
        case QueryMetadata::QueryType::insert_many:
        {
            return ParserMongoInsertManyQuery(std::move(data), metadata).parseImpl(node);
        }
        case QueryMetadata::QueryType::insert_one:
        {
            return ParserMongoInsertOneQuery(std::move(data), metadata).parseImpl(node);
        }
    }
}

std::shared_ptr<IMongoParser>
createParser(rapidjson::Value data_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_, bool literal_as_default)
{
    if (edge_name_ == "$or")
    {
        return std::make_shared<MongoOrFunction>(std::move(data_), metadata_, edge_name_);
    }
    if (edge_name_ == "$add")
    {
        return std::make_shared<MongoSumFunction>(std::move(data_), metadata_, edge_name_);
    }
    if (edge_name_ == "$mul")
    {
        return std::make_shared<MongoMultiplyFunction>(std::move(data_), metadata_, edge_name_);
    }
    if (edge_name_ == "$div")
    {
        return std::make_shared<MongoDivideFunction>(std::move(data_), metadata_, edge_name_);
    }
    if (edge_name_ == "$sub")
    {
        return std::make_shared<MongoMinusFunction>(std::move(data_), metadata_, edge_name_);
    }
    if (edge_name_ == "")
    {
        return std::make_shared<ParserMongoFilter>(std::move(data_), metadata_, edge_name_);
    }
    if (edge_name_ == "$arithmetic_function_element")
    {
        return std::make_shared<MongoArithmeticFunctionElement>(std::move(data_), metadata_, edge_name_);
    }
    if (!literal_as_default)
    {
        return std::make_shared<MongoIdentityFunction>(std::move(data_), metadata_, edge_name_);
    }
    else
    {
        return std::make_shared<MongoLiteralFunction>(std::move(data_), metadata_, edge_name_);
    }
}

std::shared_ptr<IMongoParser>
createInversedParser(rapidjson::Value data_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
{
    if (static_cast<std::string>(data_.MemberBegin()->name.GetString()) == "$lt")
    {
        return std::make_shared<MongoLtFunction>(copyValue(data_.MemberBegin()->value), metadata_, edge_name_);
    }
    if (static_cast<std::string>(data_.MemberBegin()->name.GetString()) == "$lte")
    {
        return std::make_shared<MongoLteFunction>(copyValue(data_.MemberBegin()->value), metadata_, edge_name_);
    }
    if (static_cast<std::string>(data_.MemberBegin()->name.GetString()) == "$gt")
    {
        return std::make_shared<MongoGtFunction>(copyValue(data_.MemberBegin()->value), metadata_, edge_name_);
    }
    if (static_cast<std::string>(data_.MemberBegin()->name.GetString()) == "$gte")
    {
        return std::make_shared<MongoGteFunction>(copyValue(data_.MemberBegin()->value), metadata_, edge_name_);
    }
    if (static_cast<std::string>(data_.MemberBegin()->name.GetString()) == "$ne")
    {
        return std::make_shared<MongoNotEqualsFunction>(copyValue(data_.MemberBegin()->value), metadata_, edge_name_);
    }
    if (static_cast<std::string>(data_.MemberBegin()->name.GetString()) == "$regex")
    {
        return std::make_shared<MongoLikeFunction>(copyValue(data_.MemberBegin()->value), metadata_, edge_name_);
    }
    return nullptr;
}

}

}
