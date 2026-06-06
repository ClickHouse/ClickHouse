#include <Storages/MergeTree/JsonSchemaHints.h>

#include <Columns/ColumnObject.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ExpressionListParsers.h>
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

JsonSchemaHints parseJsonSchemaHints(const String & hints_json)
{
    JsonSchemaHints result;

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

    for (auto col_it = root->begin(); col_it != root->end(); ++col_it)
    {
        String column_name = col_it->first;
        auto rules_arr = col_it->second.extract<Poco::JSON::Array::Ptr>();
        if (!rules_arr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "json_schema_hints: value for column '{}' must be an array of rules", column_name);

        JsonSchemaHintRules rules;
        for (unsigned int i = 0; i < rules_arr->size(); ++i)
        {
            auto rule_obj = rules_arr->getObject(i);
            if (!rule_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "json_schema_hints: each rule must be a JSON object");

            JsonSchemaHintRule rule;

            if (rule_obj->has("when"))
                rule.when_expression = rule_obj->getValue<String>("when");

            if (!rule_obj->has("paths"))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "json_schema_hints: each rule must have a 'paths' field");

            auto paths_obj = rule_obj->getObject("paths");
            if (!paths_obj)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "json_schema_hints: 'paths' must be a JSON object");

            for (auto path_it = paths_obj->begin(); path_it != paths_obj->end(); ++path_it)
            {
                String path_name = path_it->first;
                String type_name = path_it->second.convert<String>();

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


void applyJsonSchemaHints(
    Block & block,
    const MergeTreePartition & partition,
    const JsonSchemaHints & hints,
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

            for (const auto & [path, _type_name] : rule.paths)
            {
                if (column_object->getTypedPaths().contains(path))
                    continue;
                if (column_object->getDynamicPathsPtrs().contains(path))
                    continue;

                column_object->tryToAddNewDynamicPath(path);
            }

            break;
        }
    }
}

}
