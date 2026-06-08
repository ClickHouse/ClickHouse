#include <Storages/MergeTree/JSONSchemaHints.h>

#include <Columns/ColumnObject.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/Context.h>

#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/Dynamic/Var.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

JSONSchemaHints parseJSONSchemaHints(const String & hints_json)
{
    JSONSchemaHints result;

    if (hints_json.empty())
        return result;

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var parsed;
    try
    {
        parsed = parser.parse(hints_json);
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "json_schema_hints: invalid JSON: {}", e.displayText());
    }

    auto root = parsed.extract<Poco::JSON::Object::Ptr>();
    if (!root)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "json_schema_hints must be a JSON object");

    for (auto & [column_name_var, column_value] : *root)
    {
        String column_name = column_name_var;
        auto rules_arr = column_value.extract<Poco::JSON::Array::Ptr>();
        if (!rules_arr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "json_schema_hints: value for column '{}' must be an array of rules", column_name);

        JSONSchemaHintRules rules;
        for (unsigned int i = 0; i < rules_arr->size(); ++i)
        {
            auto rule_obj = rules_arr->getObject(i);
            if (!rule_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "json_schema_hints: each rule must be a JSON object");

            JSONSchemaHintRule rule;

            if (rule_obj->has("when"))
                rule.when_expression = rule_obj->getValue<String>("when");

            if (!rule_obj->has("paths"))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "json_schema_hints: each rule must have a 'paths' field");

            auto paths_obj = rule_obj->getObject("paths");
            if (!paths_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "json_schema_hints: 'paths' must be a JSON object");

            for (auto & [path_name_var, path_value] : *paths_obj)
            {
                String path_name = path_name_var;
                String type_name = path_value.convert<String>();

                /// Validate the type name.
                DataTypeFactory::instance().get(type_name);

                rule.paths[path_name] = type_name;
            }

            rules.push_back(std::move(rule));
        }

        result[column_name] = std::move(rules);
    }

    return result;
}


void validateJSONSchemaHints(
    const JSONSchemaHints & hints,
    const StorageMetadataPtr & metadata_snapshot)
{
    if (hints.empty())
        return;

    /// Validate that hint columns exist and are JSON type.
    const auto & columns = metadata_snapshot->getColumns();
    for (const auto & [column_name, rules] : hints)
    {
        if (!columns.has(column_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "json_schema_hints: column '{}' does not exist in the table", column_name);

        auto column_type = columns.get(column_name).type;
        if (column_type->getTypeId() != TypeIndex::Object)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "json_schema_hints: column '{}' is not a JSON column (type: {})",
                column_name, column_type->getName());
    }

    /// Collect partition key column names.
    std::unordered_set<String> partition_key_columns;
    if (metadata_snapshot->hasPartitionKey())
    {
        const auto & partition_key = metadata_snapshot->getPartitionKey();
        for (const auto & col : partition_key.sample_block.getColumnsWithTypeAndName())
            partition_key_columns.insert(col.name);
    }

    for (const auto & [column_name, rules] : hints)
    {
        for (const auto & rule : rules)
        {
            if (rule.when_expression.empty())
                continue;

            /// Parse the when expression and extract all referenced identifiers.
            ParserExpressionWithOptionalAlias parser(false);
            auto ast = parseQuery(parser, rule.when_expression, 0, 0, 0);

            IdentifierNameSet identifiers;
            ast->collectIdentifierNames(identifiers);

            for (const auto & id : identifiers)
            {
                if (!partition_key_columns.contains(id))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "json_schema_hints: `when` expression '{}' references column '{}' "
                        "which is not part of the partition key. "
                        "`when` expressions may only reference partition key columns.",
                        rule.when_expression, id);
            }
        }
    }
}


static bool evaluateWhenExpression(
    const String & when_expr,
    const MergeTreePartition & partition,
    const StorageMetadataPtr & metadata_snapshot)
{
    if (when_expr.empty())
        return true;

    const auto & partition_key = metadata_snapshot->getPartitionKey();
    const auto & partition_key_columns = partition_key.sample_block.getColumnsWithTypeAndName();
    const auto & partition_value = partition.value;

    if (partition_value.size() != partition_key_columns.size())
        return false;

    Block eval_block;
    for (size_t i = 0; i < partition_key_columns.size(); ++i)
    {
        auto col = partition_key_columns[i].type->createColumn();
        col->insert(partition_value[i]);
        eval_block.insert({std::move(col), partition_key_columns[i].type, partition_key_columns[i].name});
    }

    ParserExpressionWithOptionalAlias parser(false);
    auto ast = parseQuery(parser, when_expr, 0, 0, 0);

    auto syntax = TreeRewriter(Context::getGlobalContextInstance()).analyze(ast, eval_block.getNamesAndTypesList());
    auto actions = ExpressionAnalyzer(ast, syntax, Context::getGlobalContextInstance()).getActions(true);
    actions->execute(eval_block);

    const auto & result_col = eval_block.getByPosition(eval_block.columns() - 1);
    if (result_col.column->size() != 1)
        return false;

    return result_col.column->getBool(0);
}


void applyJSONSchemaHints(
    Block & block,
    const MergeTreePartition & partition,
    const JSONSchemaHints & hints,
    const StorageMetadataPtr & metadata_snapshot)
{
    for (const auto & [column_name, rules] : hints)
    {
        if (!block.has(column_name))
            continue;

        auto & column_with_type = block.getByName(column_name);
        auto * column_object = typeid_cast<ColumnObject *>(column_with_type.column->assumeMutable().get());
        if (!column_object)
            continue;

        for (const auto & rule : rules)
        {
            if (!evaluateWhenExpression(rule.when_expression, partition, metadata_snapshot))
                continue;

            /// Pre-create Dynamic paths for all hinted paths.
            /// This guarantees each hinted path gets its own ColumnDynamic
            /// subcolumn (won't be squeezed into shared data).
            /// Layer 1 auto-narrowing will then store these single-typed
            /// Dynamic paths as plain typed columns on disk.
            for (const auto & [path, _type_name] : rule.paths)
            {
                if (column_object->getTypedPaths().contains(path))
                    continue;
                if (column_object->getDynamicPathsPtrs().contains(path))
                    continue;

                /// tryToAddNewDynamicPath may fail if max_dynamic_paths is reached.
                /// In that case the path will fall into shared data — acceptable
                /// degradation, not an error.
                column_object->tryToAddNewDynamicPath(path);
            }

            break; /// First matching rule wins.
        }
    }
}

}
