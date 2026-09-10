#include <optional>
#include "config.h"

#if USE_AVRO

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsDateTime.h>
#include <Common/DateLUTImpl.h>
#include <Common/DateLUT.h>
#include <Core/DecimalFunctions.h>
#include <base/arithmeticOverflow.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Common/logger_useful.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <IO/ReadHelpers.h>
#include <Common/quoteString.h>
#include <fmt/ranges.h>

#include <Interpreters/ExpressionActions.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFileIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

using namespace DB;

namespace DB::Iceberg
{

DB::ASTPtr getASTFromTransform(const String & transform_name_src, const String & column_name)
{
    auto transform_and_argument = parseTransformAndArgument(transform_name_src);
    if (!transform_and_argument)
    {
        LOG_WARNING(&Poco::Logger::get("Iceberg Partition Pruning"), "Cannot parse iceberg transform name: {}.", transform_name_src);
        return nullptr;
    }

    std::string transform_name = Poco::toLower(transform_name_src);
    if (transform_name == "identity")
        return make_intrusive<ASTIdentifier>(column_name);

    if (transform_name == "void")
        return makeASTOperator("tuple");

    if (transform_and_argument->argument.has_value())
    {
        return makeASTFunction(
                transform_and_argument->transform_name, make_intrusive<ASTLiteral>(*transform_and_argument->argument), make_intrusive<ASTIdentifier>(column_name));
    }
    return makeASTFunction(transform_and_argument->transform_name, make_intrusive<ASTIdentifier>(column_name));
}

std::unique_ptr<DB::ActionsDAG> ManifestFilesPruner::transformFilterDagForManifest(const DB::ActionsDAG * source_dag, std::vector<Int32> & used_columns_in_filter) const
{
    const auto & inputs = source_dag->getInputs();

    for (const auto & input : inputs)
    {
        if (input->type == ActionsDAG::ActionType::INPUT)
        {
            std::string input_name = input->result_name;
            std::optional<Int32> input_id = schema_processor.tryGetColumnIDByName(current_schema_id, input_name);
            if (input_id)
                used_columns_in_filter.push_back(*input_id);
        }
    }

    ActionsDAG dag_with_renames;
    for (const auto column_id : used_columns_in_filter)
    {
        auto column = schema_processor.tryGetFieldCharacteristics(current_schema_id, column_id);

        /// Columns which we dropped and don't exist in current schema
        /// cannot be queried in WHERE expression.
        if (!column.has_value())
            continue;

        /// We take data type from manifest schema, not latest type
        auto column_from_manifest = schema_processor.tryGetFieldCharacteristics(initial_schema_id, column_id);
        if (!column_from_manifest.has_value())
            continue;

        auto numeric_column_name = DB::backQuote(DB::toString(column_id));
        const auto * node = &dag_with_renames.addInput(numeric_column_name, column_from_manifest->type);
        node = &dag_with_renames.addAlias(*node, column->name);
        dag_with_renames.getOutputs().push_back(node);
    }
    auto result = std::make_unique<DB::ActionsDAG>(DB::ActionsDAG::merge(std::move(dag_with_renames), source_dag->clone()));
    result->removeUnusedActions();
    return result;
}


ManifestFilesPruner::ManifestFilesPruner(
    const IcebergSchemaProcessor & schema_processor_,
    Int32 current_schema_id_,
    Int32 initial_schema_id_,
    const DB::ActionsDAG * filter_dag,
    const ManifestFileIterator & manifest_file,
    DB::ContextPtr context)
    : schema_processor(schema_processor_)
    , current_schema_id(current_schema_id_)
    , initial_schema_id(initial_schema_id_)
{
    if (filter_dag == nullptr)
    {
        return;
    }

    std::unique_ptr<ActionsDAG> transformed_dag;
    std::vector<Int32> used_columns_in_filter;
    transformed_dag = transformFilterDagForManifest(filter_dag, used_columns_in_filter);
    chassert(transformed_dag != nullptr);

    if (manifest_file.hasPartitionKey())
    {
        partition_key = &manifest_file.getPartitionKeyDescription();
        ActionsDAGWithInversionPushDown inverted_dag(transformed_dag->getOutputs().front(), context, /* boolean_context */ true);
        partition_key_condition.emplace(
            inverted_dag, context, partition_key->column_names, partition_key->expression, true /* single_point */);
    }

    for (Int32 used_column_id : used_columns_in_filter)
    {
        auto name_and_type = schema_processor.tryGetFieldCharacteristics(initial_schema_id, used_column_id);
        if (!name_and_type.has_value())
            continue;

        name_and_type->name = DB::backQuote(DB::toString(used_column_id));

        ExpressionActionsPtr expression
            = std::make_shared<ExpressionActions>(ActionsDAG({name_and_type.value()}), ExpressionActionsSettings(context));

        ActionsDAGWithInversionPushDown inverted_dag(transformed_dag->getOutputs().front(), context, /* boolean_context */ true);
        min_max_key_conditions.emplace(used_column_id, KeyCondition(inverted_dag, context, {name_and_type->name}, expression));
    }
}

namespace
{

enum class PartitionTransformKind : uint8_t
{
    Day,
    Month,
    Year,
    Hour,
    NotInvertible,
};

PartitionTransformKind parsePartitionTransformKind(const String & transform_name_src)
{
    const String transform_name = Poco::toLower(transform_name_src);

    if (transform_name == "day" || transform_name == "days" || transform_name == "date" || transform_name == "dates")
        return PartitionTransformKind::Day;
    if (transform_name == "month" || transform_name == "months")
        return PartitionTransformKind::Month;
    if (transform_name == "year" || transform_name == "years")
        return PartitionTransformKind::Year;
    if (transform_name == "hour" || transform_name == "hours")
        return PartitionTransformKind::Hour;
    return PartitionTransformKind::NotInvertible;
}

/// Half-open: `[first, past_last)`. The transforms map a whole such interval to one partition value,
/// and every step below stays half-open, so a value from corrupt metadata can only fail an overflow
/// check and disable pruning, never wrap around.
struct Interval
{
    Int64 first;
    Int64 past_last;
};

/// The value covers `[v, v + 1)` of its own unit.
std::optional<Interval> unitInterval(Int64 value)
{
    Int64 past_last = 0;
    if (common::addOverflow(value, Int64{1}, past_last))
        return {};
    return Interval{value, past_last};
}

/// `[a, b)` in one unit is `[a * factor, b * factor)` in a unit that many times finer.
std::optional<Interval> refineInterval(std::optional<Interval> interval, Int64 factor)
{
    Int64 first = 0;
    Int64 past_last = 0;
    if (!interval || common::mulOverflow(interval->first, factor, first) || common::mulOverflow(interval->past_last, factor, past_last))
        return {};
    return Interval{first, past_last};
}

std::optional<Range> closedRange(std::optional<Interval> interval, std::optional<UInt32> decimal_scale)
{
    Int64 last = 0;
    if (!interval || common::subOverflow(interval->past_last, Int64{1}, last))
        return {};

    if (decimal_scale)
        return Range(
            DecimalField<Decimal64>(interval->first, *decimal_scale), true, DecimalField<Decimal64>(last, *decimal_scale), true);
    return Range(interval->first, true, last, true);
}

std::optional<Interval> dayIntervalOfPartitionValue(PartitionTransformKind kind, Int64 value)
{
    if (kind == PartitionTransformKind::Day)
        return unitInterval(value);

    auto own_unit = unitInterval(value);
    if (!own_unit)
        return {};

    const auto & utc = DateLUT::instance("UTC");
    const auto epoch = ExtendedDayNum(0);

    if (kind == PartitionTransformKind::Month)
    {
        const auto first = utc.addMonths(epoch, own_unit->first);
        const auto past_last = utc.addMonths(epoch, own_unit->past_last);
        if (utc.toMonthNumSinceEpoch(first) != own_unit->first || utc.toMonthNumSinceEpoch(past_last) != own_unit->past_last)
            return {};
        return Interval{Int64{first}, Int64{past_last}};
    }

    if (kind == PartitionTransformKind::Year)
    {
        const auto first = utc.addYears(epoch, own_unit->first);
        const auto past_last = utc.addYears(epoch, own_unit->past_last);
        if (utc.toYearSinceEpoch(first) != own_unit->first || utc.toYearSinceEpoch(past_last) != own_unit->past_last)
            return {};
        return Interval{Int64{first}, Int64{past_last}};
    }

    return {};
}

std::optional<Interval> secondIntervalOfPartitionValue(PartitionTransformKind kind, Int64 value)
{
    static constexpr Int64 seconds_per_hour = 3600;
    static constexpr Int64 seconds_per_day = 86400;

    if (kind == PartitionTransformKind::Hour)
        return refineInterval(unitInterval(value), seconds_per_hour);
    return refineInterval(dayIntervalOfPartitionValue(kind, value), seconds_per_day);
}

std::optional<Int64> partitionValueAsInt64(const Field & partition_value)
{
    if (partition_value.getType() == Field::Types::Int64)
        return partition_value.safeGet<Int64>();

    if (partition_value.getType() == Field::Types::UInt64)
    {
        const UInt64 value = partition_value.safeGet<UInt64>();
        if (value <= static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            return static_cast<Int64>(value);
    }

    return {};
}

std::optional<Range> rangeOfPartitionValue(const String & transform_name, const Field & partition_value, const IDataType & source_type)
{
    const auto value = partitionValueAsInt64(partition_value);
    if (!value)
        return {};

    const PartitionTransformKind kind = parsePartitionTransformKind(transform_name);
    const WhichDataType which(source_type);

    if (which.isDateOrDate32())
        return closedRange(dayIntervalOfPartitionValue(kind, *value), std::nullopt);

    if (which.isDateTime())
        return closedRange(secondIntervalOfPartitionValue(kind, *value), std::nullopt);

    if (which.isDateTime64())
    {
        const UInt32 scale = getDecimalScale(source_type);
        return closedRange(
            refineInterval(secondIntervalOfPartitionValue(kind, *value), DecimalUtils::scaleMultiplier<Int64>(scale)), scale);
    }

    return {};
}

}

PruningReturnStatus ManifestFilesPruner::canBePruned(
    const ProcessedManifestFileEntryPtr & entry, const std::unordered_map<Int32, DB::Range> & entry_hyperrectangles) const
{
    const auto & partition_value = entry->parsed_entry->partition_key_value;

    if (partition_key_condition.has_value())
    {
        std::vector<FieldRef> index_value(partition_value.begin(), partition_value.end());
        for (size_t i = 0; i < index_value.size(); ++i)
        {
            auto & field = index_value[i];
            const auto & type = partition_key->data_types.at(i);
            // NULL_LAST
            if (field.isNull())
                field = POSITIVE_INFINITY;
            else if (field.getType() == Field::Types::Int64 && WhichDataType(type).isDateTime64()) /// clickhouse used to write timestamp as simple long in avro
                field = DecimalField<Decimal64>(field.safeGet<Int64>(), getDecimalScale(*type));
        }

        bool can_be_true = partition_key_condition->mayBeTrueInRange(
            partition_value.size(), index_value.data(), index_value.data(), partition_key->data_types);

        if (!can_be_true)
        {
            return PruningReturnStatus::PARTITION_PRUNED;
        }
    }

    for (const auto & [column_id, key_condition] : min_max_key_conditions)
    {
        std::optional<NameAndTypePair> name_and_type = schema_processor.tryGetFieldCharacteristics(initial_schema_id, column_id);

        /// There is no such column in this manifest file
        if (!name_and_type.has_value())
        {
            continue;
        }

        auto info_it = entry->parsed_entry->columns_infos.find(column_id);
        bool has_no_nulls = info_it != entry->parsed_entry->columns_infos.end() && info_it->second.nulls_count.has_value()
            && *info_it->second.nulls_count == 0;

        const DataTypes data_types{name_and_type->type};

        if (entry->common_partition_specification)
        {
            for (const auto & partition_field : *entry->common_partition_specification)
            {
                if (partition_field.source_id != column_id || partition_field.tuple_index < 0
                    || static_cast<size_t>(partition_field.tuple_index) >= partition_value.size())
                    continue;

                auto range = rangeOfPartitionValue(
                    partition_field.transform_name,
                    partition_value[partition_field.tuple_index],
                    *removeNullable(name_and_type->type));

                if (range && !key_condition.mayBeTrueInRange(1, &range->left, &range->right, data_types))
                    return PruningReturnStatus::PARTITION_PRUNED;
            }
        }

        auto rect_it = entry_hyperrectangles.find(column_id);
        if (has_no_nulls && rect_it != entry_hyperrectangles.end()
            && !key_condition.mayBeTrueInRange(1, &rect_it->second.left, &rect_it->second.right, data_types))
        {
            return PruningReturnStatus::MIN_MAX_INDEX_PRUNED;
        }
    }

    return PruningReturnStatus::NOT_PRUNED;
}
}

#endif
