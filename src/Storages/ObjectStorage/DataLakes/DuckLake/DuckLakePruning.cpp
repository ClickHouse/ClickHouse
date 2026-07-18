#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakePruning.h>

#if USE_PARQUET

#include <Databases/DataLake/DuckLakeCatalog.h>

#include <Core/Range.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <Functions/FunctionFactory.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>

#include <Common/logger_useful.h>
#include <Common/quoteString.h>

#include <Poco/String.h>

#include <fmt/format.h>

namespace DB
{

namespace DuckLake
{

std::optional<Field> parseStatsValue(const String & value, const DataTypePtr & type_)
{
    const auto type = removeNullable(type_);
    const WhichDataType which(type);

    if (which.isString() || which.isFixedString())
        return Field(value);

    /// DuckDB serializes booleans as 'true'/'false'; booleans are UInt8 in ClickHouse.
    if (which.isUInt8() && (value == "true" || value == "false"))
        return Field(UInt64(value == "true" ? 1 : 0));

    if (!type->canBeInsideNullable())
        return std::nullopt;

    try
    {
        auto column = type->createColumn();
        ReadBufferFromString buf(value);
        FormatSettings format_settings;
        type->getDefaultSerialization()->deserializeWholeText(*column, buf, format_settings);
        if (!buf.eof() || column->empty())
            return std::nullopt;
        return column->operator[](0);
    }
    catch (...)
    {
        /// Unparseable for this type (e.g. timestamptz offsets): do not prune on it.
        return std::nullopt;
    }
}

namespace
{

bool isScalarType(const DataTypePtr & type)
{
    const auto nested = removeNullable(type);
    return !isTuple(nested) && !isArray(nested) && !isMap(nested);
}

String statsColumnName(Int64 column_id)
{
    return backQuote(toString(column_id));
}

/// Rename every filter input that resolves to a visible column to its backquoted column id,
/// so per-column KeyConditions can be evaluated against per-file stats. Inputs that do not
/// resolve are passed through unchanged.
std::unique_ptr<ActionsDAG> transformFilterDag(
    const ActionsDAG & filter_dag,
    const std::unordered_map<String, Int64> & field_id_map,
    const std::unordered_map<Int64, NameAndTypePair> & column_types,
    std::vector<std::pair<Int64, DataTypePtr>> & used_columns)
{
    ActionsDAG renames;
    for (const auto * input : filter_dag.getInputs())
    {
        if (input->type != ActionsDAG::ActionType::INPUT)
            continue;

        String new_name = input->result_name;
        auto id_it = field_id_map.find(input->result_name);
        if (id_it != field_id_map.end())
        {
            auto type_it = column_types.find(id_it->second);
            if (type_it != column_types.end() && isScalarType(type_it->second.type))
            {
                new_name = statsColumnName(id_it->second);
                used_columns.emplace_back(id_it->second, removeNullable(type_it->second.type));
            }
        }
        const auto * node = &renames.addInput(new_name, input->result_type);
        node = &renames.addAlias(*node, input->result_name);
        renames.getOutputs().push_back(node);
    }

    auto result = std::make_unique<ActionsDAG>(ActionsDAG::merge(std::move(renames), filter_dag.clone()));
    result->removeUnusedActions();
    return result;
}

bool sameSpec(const std::vector<DuckLakePartitionField> & lhs, const std::vector<DuckLakePartitionField> & rhs)
{
    if (lhs.size() != rhs.size())
        return false;
    for (size_t i = 0; i < lhs.size(); ++i)
    {
        if (lhs[i].partition_key_index != rhs[i].partition_key_index
            || lhs[i].column_id != rhs[i].column_id
            || lhs[i].transform != rhs[i].transform)
            return false;
    }
    return true;
}

}

FilePruner::FilePruner(
    const ActionsDAG * filter_dag_,
    const std::unordered_map<String, Int64> & field_id_map,
    const std::unordered_map<Int64, NameAndTypePair> & column_types,
    ContextPtr context_)
    : filter_dag(filter_dag_)
    , column_types_by_id(column_types)
    , context(std::move(context_))
{
    if (!filter_dag)
        return;

    std::vector<std::pair<Int64, DataTypePtr>> used_columns;
    auto transformed_dag = transformFilterDag(*filter_dag, field_id_map, column_types, used_columns);

    ActionsDAGWithInversionPushDown inverted_dag(transformed_dag->getOutputs().front(), context, /* boolean_context */ true);
    for (const auto & [column_id, type] : used_columns)
    {
        NameAndTypePair key_column(statsColumnName(column_id), type);
        auto expression = std::make_shared<ExpressionActions>(ActionsDAG({key_column}), ExpressionActionsSettings(context));
        min_max_conditions.push_back(MinMaxCondition{
            .column_id = column_id,
            .type = type,
            .condition = KeyCondition(inverted_dag, context, {key_column.name}, expression),
        });
    }
}

const FilePruner::PartitionKeyCondition * FilePruner::getPartitionCondition(const std::vector<DuckLakePartitionField> & spec) const
{
    if (cached_spec && sameSpec(*cached_spec, spec))
        return cached_partition_condition ? &*cached_partition_condition : nullptr;

    cached_spec = spec;
    cached_partition_condition.reset();

    if (spec.empty() || !filter_dag)
        return nullptr;

    ActionsDAG key_dag;
    Names key_column_names;
    DataTypes key_data_types;
    std::unordered_map<String, const ActionsDAG::Node *> inputs;

    for (const auto & field : spec)
    {
        const auto column_it = column_types_by_id.find(field.column_id);
        if (column_it == column_types_by_id.end())
            return nullptr;
        const String & column_name = column_it->second.name;
        const DataTypePtr column_type = column_it->second.type;

        auto [input_it, inserted] = inputs.try_emplace(column_name, nullptr);
        if (inserted)
            input_it->second = &key_dag.addInput(column_name, column_type);
        const ActionsDAG::Node * node = input_it->second;

        const String transform = Poco::toLower(field.transform);
        DataTypePtr key_type;
        if (transform == "identity")
        {
            key_type = removeNullable(column_type);
        }
        else
        {
            String function_name;
            if (transform == "year")
            {
                function_name = "toYear";
                key_type = std::make_shared<DataTypeUInt16>();
            }
            else if (transform == "month")
            {
                function_name = "toMonth";
                key_type = std::make_shared<DataTypeUInt8>();
            }
            else if (transform == "day")
            {
                function_name = "toDayOfMonth";
                key_type = std::make_shared<DataTypeUInt8>();
            }
            else if (transform == "hour")
            {
                function_name = "toHour";
                key_type = std::make_shared<DataTypeUInt8>();
            }
            else
            {
                /// bucket(N) and unknown transforms cannot be pruned.
                return nullptr;
            }

            /// DuckLake's year/month/day/hour transforms are DuckDB's extraction functions,
            /// which match the ClickHouse counterparts (year number, month 1-12, day 1-31, hour 0-23).
            node = &key_dag.addFunction(FunctionFactory::instance().get(function_name, context), {node}, {});
        }

        key_column_names.push_back(fmt::format("__ducklake_partition_key_{}", field.partition_key_index));
        node = &key_dag.addAlias(*node, key_column_names.back());
        key_dag.getOutputs().push_back(node);
        key_data_types.push_back(key_type);
    }

    auto expression = std::make_shared<ExpressionActions>(std::move(key_dag), ExpressionActionsSettings(context));
    ActionsDAGWithInversionPushDown inverted_dag(filter_dag->getOutputs().front(), context, /* boolean_context */ true);
    cached_partition_condition.emplace(PartitionKeyCondition{
        .key_data_types = key_data_types,
        .condition = KeyCondition(inverted_dag, context, key_column_names, expression, /* single_point */ true),
    });
    return &*cached_partition_condition;
}

bool FilePruner::canBePruned(
    const DuckLakeDataFileEntry & file,
    const std::vector<DuckLakePartitionField> * partition_spec) const
{
    for (const auto & condition : min_max_conditions)
    {
        const DuckLakeFileColumnStats * stats = nullptr;
        for (const auto & column_stats : file.column_stats)
        {
            if (column_stats.column_id == condition.column_id)
            {
                stats = &column_stats;
                break;
            }
        }
        if (!stats || !stats->min_value.has_value() || !stats->max_value.has_value())
            continue;
        /// With NULLs in the file the [min, max] range does not represent all values; like
        /// Iceberg manifest pruning, only prune NULL-free files. NaN breaks float ordering.
        if (stats->null_count > 0 || stats->contains_nan)
            continue;

        auto min_value = parseStatsValue(*stats->min_value, condition.type);
        auto max_value = parseStatsValue(*stats->max_value, condition.type);
        if (!min_value.has_value() || !max_value.has_value())
            continue;

        FieldRef left(*min_value);
        FieldRef right(*max_value);
        if (!condition.condition.mayBeTrueInRange(1, &left, &right, {condition.type}))
            return true;
    }

    if (partition_spec && !partition_spec->empty() && file.partition_values.size() >= partition_spec->size())
    {
        if (const auto * condition = getPartitionCondition(*partition_spec))
        {
            std::vector<FieldRef> point;
            point.reserve(partition_spec->size());
            bool usable = true;
            for (size_t i = 0; i < partition_spec->size(); ++i)
            {
                if (!file.partition_values[i].has_value())
                {
                    /// NULL partition values sort after everything (NULL_LAST).
                    point.emplace_back(POSITIVE_INFINITY);
                    continue;
                }
                auto value = parseStatsValue(*file.partition_values[i], condition->key_data_types[i]);
                if (!value.has_value())
                {
                    usable = false;
                    break;
                }
                point.emplace_back(std::move(*value));
            }
            if (usable && !condition->condition.mayBeTrueInRange(
                    point.size(), point.data(), point.data(), condition->key_data_types))
                return true;
        }
    }

    return false;
}

}

}

#endif
