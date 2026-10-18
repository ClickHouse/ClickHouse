#include <Parsers/Mongo/ParserMongoQuery.h>

#include <memory>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>

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
#include <Parsers/IParserBase.h>

#include <Parsers/Mongo/Metadata.h>
#include <Parsers/Mongo/ParserMongoAggregateQuery.h>
#include <Parsers/Mongo/ParserMongoDeleteQuery.h>
#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/ParserMongoFunction.h>
#include <Parsers/Mongo/ParserMongoInsertQuery.h>
#include <Parsers/Mongo/ParserMongoSelectQuery.h>
#include <Parsers/Mongo/ParserMongoUpdateQuery.h>
#include <Parsers/Mongo/Utils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Mongo
{

namespace
{

/// The single argument of the call, for the operations that take exactly one.
rapidjson::Value & onlyArgument(rapidjson::Value & data, const char * operation)
{
    auto args = data.GetArray();
    if (args.Size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' takes exactly one argument, got {}", operation, args.Size());
    return *args.Begin();
}

/** The query is executed here as an AST, but a remote shard, a parallel replica or a replica of a `Replicated`
  * database receives its formatted text and reads it back with the ClickHouse parser. That parser reads a
  * non-negative whole number as `UInt64`, so a literal built here as `Int64` would give the constant a different
  * type and name on each side (`2_Int8` on the initiator, `2_UInt8` on the shard), and the initiator would not find
  * the column the shard returns. Give the literals the types the formatted query is read back with.
  */
void normalizeIntegerField(Field & field)
{
    switch (field.getType())
    {
        case Field::Types::Int64:
            if (Int64 value = field.safeGet<Int64>(); value >= 0)
                field = UInt64(value);
            break;
        case Field::Types::Array:
            for (auto & element : field.safeGet<Array>())
                normalizeIntegerField(element);
            break;
        case Field::Types::Tuple:
            for (auto & element : field.safeGet<Tuple>())
                normalizeIntegerField(element);
            break;
        case Field::Types::Map:
            for (auto & element : field.safeGet<Map>())
                normalizeIntegerField(element);
            break;
        default:
            break;
    }
}

void normalizeIntegerLiterals(IAST & ast)
{
    if (auto * literal = ast.as<ASTLiteral>())
        normalizeIntegerField(literal->value);
    for (const auto & child : ast.children)
        normalizeIntegerLiterals(*child);
}

}

bool ParserMongoQuery::parseImpl(Pos & /*pos*/, ASTPtr & node, Expected & /*expected*/)
{
    if (!parseStatement(node))
        return false;
    if (node)
        normalizeIntegerLiterals(*node);
    return true;
}

bool ParserMongoQuery::parseStatement(ASTPtr & node)
{
    switch (metadata->getQueryType())
    {
        case QueryMetadata::QueryType::select: {
            /// `find` is called positionally: an optional filter and an optional projection.
            auto args = data.GetArray();
            if (args.Size() > 2)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "'find' takes at most two arguments, a filter and a projection, got {}", args.Size());

            rapidjson::Value filter(rapidjson::kObjectType);
            if (args.Size() >= 1)
            {
                if (!args[0].IsObject())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The filter of 'find' must be a document");
                filter = std::move(args[0]);
            }
            if (args.Size() == 2)
            {
                if (!args[1].IsObject())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The projection of 'find' must be a document");
                /// An empty projection document asks for the whole document, the same as no
                /// projection at all.
                if (args[1].MemberCount() > 0)
                    filter.AddMember("$projection", std::move(args[1]), metadata->getAllocator());
            }
            return ParserMongoSelectQuery(std::move(filter), metadata).parseImpl(node);
        }
        case QueryMetadata::QueryType::insert_many: {
            return ParserMongoInsertManyQuery(std::move(onlyArgument(data, "insertMany")), metadata).parseImpl(node);
        }
        case QueryMetadata::QueryType::insert_one: {
            return ParserMongoInsertOneQuery(std::move(onlyArgument(data, "insertOne")), metadata).parseImpl(node);
        }
        case QueryMetadata::QueryType::delete_many: {
            return ParserMongoDeleteQuery(std::move(onlyArgument(data, "deleteMany")), metadata).parseImpl(node);
        }
        case QueryMetadata::QueryType::update_many: {
            return ParserMongoUpdateQuery(std::move(data), metadata).parseImpl(node);
        }
        case QueryMetadata::QueryType::aggregate: {
            return ParserMongoAggregateQuery(std::move(onlyArgument(data, "aggregate")), metadata, "").parseImpl(node);
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
    if (edge_name_ == "$and")
    {
        return std::make_shared<MongoAndFunction>(std::move(data_), metadata_, edge_name_);
    }
    if (edge_name_ == "$nor")
    {
        return std::make_shared<MongoNorFunction>(std::move(data_), metadata_, edge_name_);
    }
    if (edge_name_ == "$expr")
    {
        return std::make_shared<MongoExprFunction>(std::move(data_), metadata_, edge_name_);
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
    if (edge_name_.empty())
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

}

}
