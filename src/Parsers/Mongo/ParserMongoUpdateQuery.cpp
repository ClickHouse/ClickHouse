#include <Parsers/Mongo/ParserMongoUpdateQuery.h>

#include <string_view>
#include <unordered_set>
#include <vector>

#include <rapidjson/document.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_fwd.h>

#include <Parsers/Mongo/MongoConstants.h>
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

/** The value an update operator writes. An update statement is data, not an aggregation pipeline:
  * the value of `{"$set": {"name": "$other"}}` is the string `$other` rather than a reference to
  * the column `other`, and `{"$inc": {"n": 1}}` adds the number one. So the value is a constant -
  * every scalar and the Extended JSON wrappers a driver sends for the types JSON cannot represent -
  * or an array of them; a document at the top level of a `$set` is a subdocument and names columns
  * (see `flattenAssignedDocument`).
  *
  * An embedded document that is a value rather than a set of paths - an element of an array, or
  * the document `$push` and `$addToSet` append - becomes a `JSON` value, which is the shape the
  * insert paths write into an `Array(JSON)` column. The arithmetic operators take no document:
  * `documents_are_values` keeps their error message about the value they do accept.
  */
ASTPtr parseUpdateValue(const rapidjson::Value & value, std::string_view operator_name, bool documents_are_values)
{
    if (value.IsArray())
    {
        auto array = makeASTFunction("array");
        for (const auto & element : value.GetArray())
            array->arguments->children.push_back(parseUpdateValue(element, operator_name, documents_are_values));
        return array;
    }
    if (auto constant = tryParseMongoConstant(value))
        return constant;
    if (documents_are_values && value.IsObject())
        return makeMongoJSONValue(value);
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "The value of '{}' must be a scalar, an array, or an Extended JSON wrapper of them",
        operator_name);
}

/** A subdocument assigned by `$set` becomes one assignment per leaf, with the dotted path as the
  * column name - the same mapping of a nested document onto columns the insert paths make, so
  * `{"$set": {"profile": {"name": "x"}}}` writes the column `profile.name`.
  */
void flattenAssignedDocument(const std::string & prefix, const rapidjson::Value & document, std::vector<ASTPtr> & assignments);

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
            values.push_back(parseUpdateValue(element, operator_name, /* documents_are_values = */ true));
        return values;
    }

    values.push_back(parseUpdateValue(value, operator_name, /* documents_are_values = */ true));
    return values;
}

void parseUpdateOperator(std::string_view name, const rapidjson::Value & argument, std::vector<ASTPtr> & assignments)
{
    if (!argument.IsObject())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The argument of '{}' must be a document", name);

    for (auto it = argument.MemberBegin(); it != argument.MemberEnd(); ++it)
    {
        std::string column(stringView(it->name));
        /// An empty name is no column, and an `ASTIdentifier` cannot even hold it.
        if (column.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "A field name of the update operator '{}' must not be empty", name);
        auto field = [&] { return make_intrusive<ASTIdentifier>(column); };

        if (name == "$set")
        {
            /// A document that is not an Extended JSON wrapper holds fields of its own, and every
            /// one of them names a column.
            if (it->value.IsObject() && !tryParseMongoConstant(it->value))
                flattenAssignedDocument(column, it->value, assignments);
            else
                assignments.push_back(makeAssignment(column, parseUpdateValue(it->value, name, /* documents_are_values = */ true)));
        }
        else if (name == "$unset")
            assignments.push_back(makeAssignment(column, makeDefaultValue(column)));
        /// The arithmetic operators take a number, so an embedded document is not a value they can
        /// write: they pass `documents_are_values = false` and keep the error message about what
        /// they do accept.
        else if (name == "$inc")
            assignments.push_back(makeAssignment(column, makeASTFunction("plus", field(), parseUpdateValue(it->value, name, false))));
        else if (name == "$mul")
            assignments.push_back(makeAssignment(column, makeASTFunction("multiply", field(), parseUpdateValue(it->value, name, false))));
        else if (name == "$min")
            assignments.push_back(makeAssignment(column, makeASTFunction("least", field(), parseUpdateValue(it->value, name, false))));
        else if (name == "$max")
            assignments.push_back(makeAssignment(column, makeASTFunction("greatest", field(), parseUpdateValue(it->value, name, false))));
        else if (name == "$currentDate")
        {
            /// The legal forms are `true` and `{"$type": "date"}`. `{"$type": "timestamp"}` asks
            /// for the BSON timestamp - an internal type with no counterpart here - so it is
            /// rejected as unsupported rather than silently written as a date, and everything
            /// else, such as `false`, is an error in Mongo as well.
            bool is_date = (it->value.IsBool() && it->value.GetBool())
                || (it->value.IsObject() && it->value.MemberCount() == 1
                    && stringView(it->value.MemberBegin()->name) == "$type"
                    && it->value.MemberBegin()->value.IsString()
                    && stringView(it->value.MemberBegin()->value) == "date");
            if (!is_date)
            {
                if (it->value.IsObject() && it->value.MemberCount() == 1
                    && stringView(it->value.MemberBegin()->name) == "$type"
                    && it->value.MemberBegin()->value.IsString()
                    && stringView(it->value.MemberBegin()->value) == "timestamp")
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The 'timestamp' type of '$currentDate' is not supported, only 'date' is");
                throw Exception(ErrorCodes::BAD_ARGUMENTS, R"(The argument of '$currentDate' must be true or {{"$type": "date"}})");
            }
            assignments.push_back(makeAssignment(column, makeASTFunction("now64", make_intrusive<ASTLiteral>(Field(UInt64(3))))));
        }
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

/** Two operators of the same statement that write the same field are a conflict in Mongo, and
  * a mutation can only assign a column once, so the field is named here rather than leaving
  * the generic complaint of `ALTER TABLE ... UPDATE` to explain it.
  */
void checkFieldsWrittenOnce(const std::vector<ASTPtr> & assignments)
{
    std::unordered_set<std::string> written;
    for (const auto & assignment : assignments)
    {
        const auto & column = assignment->as<const ASTAssignment &>().column_name;
        if (!written.insert(column).second)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "The update statement writes the field '{}' more than once", column);
    }
}

/// Whether `expression` is the default value of the column `column`, as `makeDefaultValue` builds it.
bool isDefaultValueOf(const ASTPtr & expression, const std::string & column)
{
    const auto * function = expression->as<ASTFunction>();
    if (!function || function->name != "defaultValueOfTypeName" || !function->arguments || function->arguments->children.size() != 1)
        return false;
    const auto * type_name = function->arguments->children[0]->as<ASTFunction>();
    if (!type_name || type_name->name != "toTypeName" || !type_name->arguments || type_name->arguments->children.size() != 1)
        return false;
    const auto * identifier = type_name->arguments->children[0]->as<ASTIdentifier>();
    return identifier && identifier->name() == column;
}

void flattenAssignedDocument(const std::string & prefix, const rapidjson::Value & document, std::vector<ASTPtr> & assignments)
{
    if (document.MemberCount() == 0)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Cannot assign the empty document to the field '{}': a document is stored as one column per field, so an empty one names "
            "no column",
            prefix);

    for (auto it = document.MemberBegin(); it != document.MemberEnd(); ++it)
    {
        std::string name(stringView(it->name));
        if (name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "A field name of an assigned document must not be empty");
        std::string path = prefix + "." + name;
        if (it->value.IsObject() && !tryParseMongoConstant(it->value))
            flattenAssignedDocument(path, it->value, assignments);
        else
            assignments.push_back(makeAssignment(path, parseUpdateValue(it->value, "$set", /* documents_are_values = */ true)));
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

    checkFieldsWrittenOnce(assignments);

    auto expression_list = make_intrusive<ASTExpressionList>();
    for (auto & assignment : assignments)
        expression_list->children.push_back(std::move(assignment));
    return expression_list;
}

void expandMongoSubtreeAssignments(ASTPtr & ast, const std::vector<std::string> & columns)
{
    auto * alter = ast ? ast->as<ASTAlterQuery>() : nullptr;
    if (!alter || !alter->command_list)
        return;

    const std::unordered_set<std::string_view> column_set(columns.begin(), columns.end());
    /// The leaves of a path that is not a column itself, with the suffix that follows the path.
    auto leaves = [&](const std::string & path)
    {
        std::vector<std::string> suffixes;
        if (path.empty() || column_set.contains(path))
            return suffixes;
        const std::string prefix = path + ".";
        for (const auto & column : columns)
            if (column.starts_with(prefix))
                suffixes.push_back(column.substr(path.size()));
        return suffixes;
    };

    for (const auto & child : alter->command_list->children)
    {
        auto * command = child->as<ASTAlterCommand>();
        if (!command || command->type != ASTAlterCommand::UPDATE || !command->update_assignments)
            continue;

        bool expanded_any = false;
        std::vector<ASTPtr> expanded;
        for (const auto & child_assignment : command->update_assignments->children)
        {
            const auto & assignment = child_assignment->as<const ASTAssignment &>();
            const auto & expression = assignment.children.at(0);

            /// `$unset`, and the source of `$rename`, write the default of the field.
            if (isDefaultValueOf(expression, assignment.column_name))
            {
                if (auto suffixes = leaves(assignment.column_name); !suffixes.empty())
                {
                    for (const auto & suffix : suffixes)
                        expanded.push_back(makeAssignment(assignment.column_name + suffix, makeDefaultValue(assignment.column_name + suffix)));
                    expanded_any = true;
                    continue;
                }
            }
            /// The target of `$rename` is the only assignment whose value is a bare field.
            else if (const auto * source = expression->as<ASTIdentifier>())
            {
                if (auto suffixes = leaves(source->name()); !suffixes.empty())
                {
                    for (const auto & suffix : suffixes)
                        expanded.push_back(makeAssignment(assignment.column_name + suffix, make_intrusive<ASTIdentifier>(source->name() + suffix)));
                    expanded_any = true;
                    continue;
                }
            }
            expanded.push_back(child_assignment);
        }

        if (!expanded_any)
            continue;

        /// A leaf can now be written twice, e.g. by `$unset` of `profile` and `$set` of `profile.name`.
        checkFieldsWrittenOnce(expanded);
        command->update_assignments->children.assign(expanded.begin(), expanded.end());
    }
}

bool ParserMongoUpdateQuery::parseImpl(ASTPtr & node)
{
    /// `updateMany` is parsed from an array of the filter, the update statement and optionally the
    /// options document.
    if (!data.IsArray() || data.Size() < 2 || data.Size() > 3)
        return false;

    /** The options are standard Mongo syntax, so they are an error of their own rather than the
      * generic one of a query that does not parse. None of them is implemented: `upsert: true`
      * would insert a document that is not written, and `arrayFilters`, `collation`, `hint` or
      * `let` would change which documents and elements are updated, so the only option accepted
      * is an `upsert` that is false - the same as the `update` command of the wire protocol.
      */
    if (data.Size() == 3)
    {
        const auto & options = data[2];
        if (!options.IsObject())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The options of 'updateMany' must be a document");
        for (auto it = options.MemberBegin(); it != options.MemberEnd(); ++it)
        {
            auto name = stringView(it->name);
            if (name == "upsert")
            {
                if (!it->value.IsBool())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 'upsert' option of 'updateMany' must be a boolean");
                if (it->value.GetBool())
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The 'upsert: true' option of 'updateMany' is not supported");
            }
            else
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The option '{}' of 'updateMany' is not supported", name);
        }
    }

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
