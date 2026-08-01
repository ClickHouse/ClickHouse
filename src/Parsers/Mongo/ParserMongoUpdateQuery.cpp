#include <Parsers/Mongo/ParserMongoUpdateQuery.h>

#include <string_view>
#include <unordered_set>

#include <rapidjson/document.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_fwd.h>

#include <Parsers/Mongo/MongoConstants.h>
#include <Parsers/Mongo/ParserMongoAggregateExpression.h>
#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/ParserMongoQuery.h>
#include <Parsers/Mongo/Utils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
}

namespace Mongo
{

namespace
{

std::string_view stringView(const rapidjson::Value & value)
{
    return {value.GetString(), value.GetStringLength()};
}

ASTPtr makeAssignment(const std::string & column, ASTPtr expression)
{
    auto assignment = make_intrusive<ASTAssignment>();
    assignment->column_name = column;
    assignment->children.push_back(std::move(expression));
    return assignment;
}

/// A row always has a value for every column, so removing a field means writing the value an
/// insert that leaves the field out would have written: the default of the column type.
ASTPtr makeDefaultValue(const std::string & column)
{
    return makeASTFunction("defaultValueOfTypeName", makeASTFunction("toTypeName", make_intrusive<ASTIdentifier>(column)));
}

/// The values `$push` and `$addToSet` append, which `$each` turns into several of them.
std::vector<ASTPtr> parseAppendedValues(const rapidjson::Value & value, std::string_view operator_name)
{
    std::vector<ASTPtr> values;
    if (value.IsObject() && value.MemberCount() >= 1 && stringView(value.MemberBegin()->name) == "$each")
    {
        if (value.MemberCount() != 1)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only '$each' is supported as a modifier of '{}'", operator_name);
        const auto & each = value.MemberBegin()->value;
        if (!each.IsArray())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The '$each' of '{}' must be an array", operator_name);
        for (const auto & element : each.GetArray())
            values.push_back(parseMongoAggregateExpression(element));
        return values;
    }

    values.push_back(parseMongoAggregateExpression(value));
    return values;
}

void parseUpdateOperator(std::string_view name, const rapidjson::Value & argument, std::vector<ASTPtr> & assignments)
{
    if (!argument.IsObject())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '{}' must be a document", name);

    for (auto it = argument.MemberBegin(); it != argument.MemberEnd(); ++it)
    {
        std::string column(stringView(it->name));
        auto field = [&] { return make_intrusive<ASTIdentifier>(column); };

        if (name == "$set")
            assignments.push_back(makeAssignment(column, parseMongoAggregateExpression(it->value)));
        else if (name == "$unset")
            assignments.push_back(makeAssignment(column, makeDefaultValue(column)));
        else if (name == "$inc")
            assignments.push_back(makeAssignment(column, makeASTFunction("plus", field(), parseMongoAggregateExpression(it->value))));
        else if (name == "$mul")
            assignments.push_back(makeAssignment(column, makeASTFunction("multiply", field(), parseMongoAggregateExpression(it->value))));
        else if (name == "$min")
            assignments.push_back(makeAssignment(column, makeASTFunction("least", field(), parseMongoAggregateExpression(it->value))));
        else if (name == "$max")
            assignments.push_back(makeAssignment(column, makeASTFunction("greatest", field(), parseMongoAggregateExpression(it->value))));
        else if (name == "$currentDate")
            assignments.push_back(makeAssignment(column, makeASTFunction("now64", make_intrusive<ASTLiteral>(Field(UInt64(3))))));
        else if (name == "$rename")
        {
            /// A column cannot be renamed for one row only, so the value moves to the column of the
            /// new name and the old one goes back to its default, which is what a document without
            /// the field reads as.
            if (!it->value.IsString())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The new name of '$rename' must be a string");
            std::string renamed(stringView(it->value));
            if (renamed.empty() || column.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The source and the target field of '$rename' must be named");
            if (renamed == column)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The source and the target field of '$rename' must differ");
            assignments.push_back(makeAssignment(renamed, field()));
            assignments.push_back(makeAssignment(column, makeDefaultValue(column)));
        }
        else if (name == "$push" || name == "$addToSet")
        {
            auto values = parseAppendedValues(it->value, name);
            ASTPtr result = field();
            for (auto & value : values)
            {
                auto appended = makeASTFunction("arrayPushBack", result, value);
                /// `$addToSet` appends only what the array does not hold yet.
                result = name == "$push"
                    ? appended
                    : makeASTFunction("if", makeASTFunction("has", result->clone(), value->clone()), result->clone(), appended);
            }
            assignments.push_back(makeAssignment(column, std::move(result)));
        }
        else if (name == "$pop")
        {
            auto constant = tryParseMongoConstant(it->value);
            const auto * literal = constant ? constant->as<ASTLiteral>() : nullptr;
            if (!literal || literal->value.getType() != Field::Types::Int64
                || (literal->value.safeGet<Int64>() != 1 && literal->value.safeGet<Int64>() != -1))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$pop' must be 1 or -1");
            const bool from_the_end = literal->value.safeGet<Int64>() > 0;
            assignments.push_back(makeAssignment(column, makeASTFunction(from_the_end ? "arrayPopBack" : "arrayPopFront", field())));
        }
        else if (name == "$pull" || name == "$pullAll")
        {
            static constexpr auto element_name = "__mongo_element";
            auto element = make_intrusive<ASTIdentifier>(element_name);
            ASTPtr predicate;
            if (name == "$pull")
            {
                auto constant = tryParseMongoConstant(it->value);
                if (!constant)
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only a constant is supported as the argument of '$pull'");
                predicate = makeASTFunction("notEquals", element, constant);
            }
            else
            {
                if (!it->value.IsArray())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '$pullAll' must be an array");
                auto array = makeASTFunction("array");
                for (const auto & value : it->value.GetArray())
                {
                    auto constant = tryParseMongoConstant(value);
                    if (!constant)
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only constants are supported in the array of '$pullAll'");
                    array->arguments->children.push_back(std::move(constant));
                }
                predicate = makeASTFunction("not", makeASTFunction("has", array, element));
            }

            auto parameters = makeASTFunction("tuple", make_intrusive<ASTIdentifier>(element_name));
            auto lambda = makeASTFunction("lambda", std::move(parameters), std::move(predicate));
            assignments.push_back(makeAssignment(column, makeASTFunction("arrayFilter", std::move(lambda), field())));
        }
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The update operator '{}' is not supported", name);
    }
}

}

ASTPtr parseMongoUpdateStatement(const rapidjson::Value & update)
{
    if (!update.IsObject() || update.MemberCount() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "An update statement must be a non empty document of update operators");

    /// Every operator of the statement contributes to the same list of assignments, so a statement
    /// that both sets and increments is one `ALTER TABLE ... UPDATE` rather than two conditions.
    std::vector<ASTPtr> assignments;
    for (auto it = update.MemberBegin(); it != update.MemberEnd(); ++it)
    {
        auto name = stringView(it->name);
        if (!name.starts_with("$"))
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Replacing a whole document is not supported; the update statement must hold update operators");
        parseUpdateOperator(name, it->value, assignments);
    }

    /** Two operators of the same statement that write the same field are a conflict in Mongo, and
      * a mutation can only assign a column once, so the field is named here rather than leaving
      * the generic complaint of `ALTER TABLE ... UPDATE` to explain it.
      */
    std::unordered_set<std::string> written;
    for (const auto & assignment : assignments)
    {
        const auto & column = assignment->as<const ASTAssignment &>().column_name;
        if (!written.insert(column).second)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "The update statement writes the field '{}' more than once", column);
    }

    auto expression_list = make_intrusive<ASTExpressionList>();
    for (auto & assignment : assignments)
        expression_list->children.push_back(std::move(assignment));
    return expression_list;
}

bool ParserMongoUpdateQuery::parseImpl(ASTPtr & node)
{
    /// `updateMany` is parsed from a two element array: the filter and the update statement.
    if (!data.IsArray() || data.Size() != 2)
        return false;

    auto command = make_intrusive<ASTAlterCommand>();
    command->type = ASTAlterCommand::UPDATE;

    auto filter_json = copyValue(data[0], metadata->getAllocator());

    ASTPtr where_condition;
    if (!ParserMongoFilter(std::move(filter_json), metadata, "").parseImpl(where_condition))
        return false;

    /// An empty filter updates every row, which `ALTER TABLE ... UPDATE` spells as `WHERE 1`.
    if (!where_condition)
        where_condition = make_intrusive<ASTLiteral>(Field(UInt64(1)));

    command->children.push_back(where_condition);
    command->predicate = where_condition.get();

    auto update_operation = parseMongoUpdateStatement(data[1]);
    command->children.push_back(update_operation);
    command->update_assignments = update_operation.get();

    /** The command is not a statement on its own, so it is wrapped into the `ALTER TABLE` that
      * names the collection. Only then is the result something an interface can execute directly,
      * which is what the dialect hands to `executeQuery`.
      */
    auto command_list = make_intrusive<ASTExpressionList>();
    command_list->children.push_back(std::move(command));

    auto update_query = make_intrusive<ASTAlterQuery>();
    update_query->alter_object = ASTAlterQuery::AlterObjectType::TABLE;
    update_query->set(update_query->command_list, command_list);
    update_query->set(update_query->table, make_intrusive<ASTIdentifier>(metadata->getCollectionName()));
    if (!metadata->getDatabaseName().empty())
        update_query->set(update_query->database, make_intrusive<ASTIdentifier>(metadata->getDatabaseName()));

    if (update_query->database)
        update_query->children.push_back(update_query->database);
    update_query->children.push_back(update_query->table);

    node = update_query;
    return true;
}

}

}
