#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakePruning.h>

#if USE_PARQUET

#include <Databases/DataLake/DuckLakeCatalog.h>

#include <Columns/ColumnConst.h>
#include <Core/DecimalFunctions.h>
#include <Core/Range.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
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

#include <Common/DateLUTImpl.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>

#include <base/DayNum.h>

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

void intersectBucketRange(BucketRange & target, const BucketRange & other)
{
    if (other.lo.has_value() && (!target.lo.has_value() || *other.lo > *target.lo))
        target.lo = other.lo;
    if (other.hi.has_value() && (!target.hi.has_value() || *other.hi < *target.hi))
        target.hi = other.hi;
}

void intersectCalendarConstraints(CalendarConstraints & target, const CalendarConstraints & other)
{
    intersectBucketRange(target.year, other.year);
    intersectBucketRange(target.month, other.month);
    intersectBucketRange(target.day, other.day);
}

std::optional<int64_t> fieldAsInt64(const Field & field)
{
    switch (field.getType())
    {
        case Field::Types::UInt64:
            return static_cast<int64_t>(field.safeGet<UInt64>());
        case Field::Types::Int64:
            return field.safeGet<Int64>();
        default:
            return std::nullopt;
    }
}

/// How to compute calendar buckets (year/month/day numbers) and step one native unit for a
/// source column type. Buckets depend on the timezone, so only types with an unambiguous
/// timezone qualify: Date/Date32 (civil days) and DateTime/DateTime64 with an explicit
/// timezone (e.g. timestamptz mapped to UTC).
struct SourceCalendar
{
    enum class Kind
    {
        Date,
        Date32,
        DateTime,
        DateTime64
    };

    Kind kind;
    const DateLUTImpl * lut;
    UInt32 scale = 0;
};

std::optional<SourceCalendar> makeSourceCalendar(const DataTypePtr & type_)
{
    const auto type = removeNullable(type_);
    const WhichDataType which(type);
    if (which.isDate())
        return SourceCalendar{SourceCalendar::Kind::Date, &DateLUT::instance()};
    if (which.isDate32())
        return SourceCalendar{SourceCalendar::Kind::Date32, &DateLUT::instance()};
    if (which.isDateTime())
    {
        const auto & date_time = assert_cast<const DataTypeDateTime &>(*type);
        if (!date_time.hasExplicitTimeZone())
            return std::nullopt;
        return SourceCalendar{SourceCalendar::Kind::DateTime, &date_time.getTimeZone()};
    }
    if (which.isDateTime64())
    {
        const auto & date_time = assert_cast<const DataTypeDateTime64 &>(*type);
        if (!date_time.hasExplicitTimeZone())
            return std::nullopt;
        return SourceCalendar{SourceCalendar::Kind::DateTime64, &date_time.getTimeZone(), date_time.getScale()};
    }
    return std::nullopt;
}

struct YMD
{
    int32_t year;
    int32_t month;
    int32_t day;
};

/// The calendar bucket of a native range endpoint (day number / seconds / ticks).
YMD civilFromField(const Field & field, const SourceCalendar & cal)
{
    const DateLUTImpl & lut = *cal.lut;
    switch (cal.kind)
    {
        case SourceCalendar::Kind::Date:
        {
            const DayNum day(static_cast<UInt16>(field.safeGet<UInt64>()));
            return YMD{lut.toYear(day), lut.toMonth(day), lut.toDayOfMonth(day)};
        }
        case SourceCalendar::Kind::Date32:
        {
            const ExtendedDayNum day(static_cast<Int32>(field.safeGet<Int64>()));
            return YMD{lut.toYear(day), lut.toMonth(day), lut.toDayOfMonth(day)};
        }
        case SourceCalendar::Kind::DateTime:
        {
            const auto time = static_cast<time_t>(field.safeGet<UInt64>());
            return YMD{lut.toYear(time), lut.toMonth(time), lut.toDayOfMonth(time)};
        }
        case SourceCalendar::Kind::DateTime64:
        {
            const auto ticks = field.safeGet<DecimalField<DateTime64>>().getValue().value;
            const Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(cal.scale);
            const Int64 seconds = ticks >= 0 ? ticks / multiplier : -((-ticks + multiplier - 1) / multiplier);
            const auto time = static_cast<time_t>(seconds);
            return YMD{lut.toYear(time), lut.toMonth(time), lut.toDayOfMonth(time)};
        }
    }
}

/// Adjust an open range endpoint by one native unit towards the inside of the range
/// (`direction` = +1 for the left end, -1 for the right end). Returns nullopt when the
/// endpoint cannot be adjusted (type boundary): the bound is then dropped.
std::optional<Field> stepEndpoint(const Field & field, const SourceCalendar & cal, int direction)
{
    switch (cal.kind)
    {
        case SourceCalendar::Kind::Date:
        {
            const Int64 day = static_cast<Int64>(field.safeGet<UInt64>()) + direction;
            if (day < 0 || day > std::numeric_limits<UInt16>::max())
                return std::nullopt;
            return Field(static_cast<UInt64>(day));
        }
        case SourceCalendar::Kind::Date32:
        {
            const Int64 day = field.safeGet<Int64>() + direction;
            return Field(static_cast<Int64>(day));
        }
        case SourceCalendar::Kind::DateTime:
        {
            const Int64 time = static_cast<Int64>(field.safeGet<UInt64>()) + direction;
            if (time < 0 || time > std::numeric_limits<UInt32>::max())
                return std::nullopt;
            return Field(static_cast<UInt64>(time));
        }
        case SourceCalendar::Kind::DateTime64:
        {
            const auto ticks = field.safeGet<DecimalField<DateTime64>>().getValue().value + direction;
            return Field(DecimalField<DateTime64>(DateTime64(ticks), cal.scale));
        }
    }
}

struct RangeBuckets
{
    BucketRange year;
    BucketRange month;
    BucketRange day;
    bool empty = false;
};

/// Exact calendar bucket ranges for one source-column range, honoring open ends.
RangeBuckets bucketsOfRange(const Range & range, const SourceCalendar & cal)
{
    RangeBuckets result;

    std::optional<YMD> lo;
    if (!range.left.isNull())
    {
        std::optional<Field> adjusted = range.left;
        if (!range.left_included)
            adjusted = stepEndpoint(range.left, cal, +1);
        if (adjusted.has_value())
            lo = civilFromField(*adjusted, cal);
    }
    std::optional<YMD> hi;
    if (!range.right.isNull())
    {
        std::optional<Field> adjusted = range.right;
        if (!range.right_included)
            adjusted = stepEndpoint(range.right, cal, -1);
        if (adjusted.has_value())
            hi = civilFromField(*adjusted, cal);
    }

    if (lo.has_value())
        result.year.lo = lo->year;
    if (hi.has_value())
        result.year.hi = hi->year;
    if (lo.has_value() && hi.has_value())
    {
        if (lo->year > hi->year || (lo->year == hi->year && (lo->month > hi->month || (lo->month == hi->month && lo->day > hi->day))))
        {
            result.empty = true;
            return result;
        }
        if (lo->year == hi->year)
        {
            result.month.lo = lo->month;
            result.month.hi = hi->month;
            if (lo->month == hi->month)
            {
                result.day.lo = lo->day;
                result.day.hi = hi->day;
            }
        }
    }
    return result;
}

/// Envelope (union) of the bucket ranges of all source ranges. Multiple ranges arise from
/// OR predicates; the min/max envelope is a conservative superset of the union.
CalendarConstraints envelopeFromRanges(const Ranges & ranges, const SourceCalendar & cal)
{
    CalendarConstraints envelope;
    if (ranges.empty())
    {
        /// The predicate matches nothing: make the constraint unsatisfiable.
        envelope.year = BucketRange{1, 0};
        return envelope;
    }
    bool first = true;
    for (const auto & range : ranges)
    {
        if (range.isInfinite())
            continue; /// no constraint
        const RangeBuckets buckets = bucketsOfRange(range, cal);
        if (buckets.empty)
            continue;
        if (first)
        {
            envelope.year = buckets.year;
            envelope.month = buckets.month;
            envelope.day = buckets.day;
            first = false;
            continue;
        }
        const auto widen = [](BucketRange & target, const BucketRange & other)
        {
            if (target.lo.has_value() && (!other.lo.has_value() || *other.lo < *target.lo))
                target.lo = other.lo;
            if (target.hi.has_value() && (!other.hi.has_value() || *other.hi > *target.hi))
                target.hi = other.hi;
        };
        widen(envelope.year, buckets.year);
        widen(envelope.month, buckets.month);
        widen(envelope.day, buckets.day);
    }
    return envelope;
}

bool isComparisonFunction(const String & name)
{
    return name == "equals" || name == "notEquals" || name == "less" || name == "lessOrEquals"
        || name == "greater" || name == "greaterOrEquals";
}

/// Extract (transform, column, constant, flipped) from `toYear|toMonth|toDayOfMonth(col) cmp const`.
void collectCalendarComparison(
    const ActionsDAG::Node & node,
    std::unordered_map<String, CalendarConstraints> & out)
{
    const String & function_name = node.function_base->getName();
    if (node.children.size() != 2)
        return;

    for (size_t flip = 0; flip < 2; ++flip)
    {
        const ActionsDAG::Node * func_node = node.children[flip];
        const ActionsDAG::Node * const_node = node.children[1 - flip];

        if (func_node->type != ActionsDAG::ActionType::FUNCTION || func_node->children.size() != 1)
            continue;
        const String & calendar_function = func_node->function_base->getName();
        if (calendar_function != "toYear" && calendar_function != "toMonth" && calendar_function != "toDayOfMonth")
            continue;
        const ActionsDAG::Node * input = func_node->children[0];
        if (input->type != ActionsDAG::ActionType::INPUT)
            continue;
        if (const_node->type != ActionsDAG::ActionType::COLUMN || !isColumnConst(*const_node->column))
            continue;

        const auto value = fieldAsInt64((*const_node->column)[0]);
        if (!value.has_value())
            return;

        BucketRange * target = nullptr;
        if (calendar_function == "toYear")
            target = &out[input->result_name].year;
        else if (calendar_function == "toMonth")
            target = &out[input->result_name].month;
        else
            target = &out[input->result_name].day;

        BucketRange constraint;
        const int32_t v = static_cast<int32_t>(*value);
        if (function_name == "equals")
            constraint = BucketRange{v, v};
        else if (function_name == "greater")
            constraint.lo = flip ? v - 1 : v + 1;
        else if (function_name == "greaterOrEquals")
            constraint.lo = flip ? v : v;
        else if (function_name == "less")
            constraint.hi = flip ? v + 1 : v - 1;
        else if (function_name == "lessOrEquals")
            constraint.hi = flip ? v : v;
        else /// notEquals: cannot prune on it
            return;

        /// Flip swaps the comparison direction for `const op col`.
        if (flip && (function_name == "greater" || function_name == "greaterOrEquals"))
            std::swap(constraint.lo, constraint.hi);
        else if (flip && (function_name == "less" || function_name == "lessOrEquals"))
            std::swap(constraint.lo, constraint.hi);

        intersectBucketRange(*target, constraint);
        return;
    }
}

void walkCalendarConstraints(
    const ActionsDAG::Node * node,
    std::unordered_map<String, CalendarConstraints> & out)
{
    if (node->type != ActionsDAG::ActionType::FUNCTION)
        return;
    const String & function_name = node->function_base->getName();
    if (function_name == "and")
    {
        for (const auto * child : node->children)
            walkCalendarConstraints(child, out);
        return;
    }
    /// Anything else (or/not/other functions) yields no calendar constraint; the predicates
    /// are still enforced by the query, so skipping them is only conservative.
    if (isComparisonFunction(function_name))
        collectCalendarComparison(*node, out);
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

    collectFunctionFormConstraints();
}

void FilePruner::collectFunctionFormConstraints()
{
    for (const auto * output : filter_dag->getOutputs())
        walkCalendarConstraints(output, function_form_constraints);
}

const FilePruner::PartitionKeyCondition * FilePruner::getIdentityCondition(const std::vector<DuckLakePartitionField> & spec) const
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
    bool has_identity = false;

    for (const auto & field : spec)
    {
        if (Poco::toLower(field.transform) != "identity")
            continue;
        const auto column_it = column_types_by_id.find(field.column_id);
        if (column_it == column_types_by_id.end())
            continue;
        has_identity = true;
        const String & column_name = column_it->second.name;
        const DataTypePtr column_type = column_it->second.type;

        auto [input_it, inserted] = inputs.try_emplace(column_name, nullptr);
        if (inserted)
            input_it->second = &key_dag.addInput(column_name, column_type);
        const ActionsDAG::Node * node = input_it->second;

        key_column_names.push_back(fmt::format("__ducklake_partition_key_{}", field.partition_key_index));
        node = &key_dag.addAlias(*node, key_column_names.back());
        key_dag.getOutputs().push_back(node);
        key_data_types.push_back(removeNullable(column_type));
    }

    if (!has_identity)
        return nullptr;

    auto expression = std::make_shared<ExpressionActions>(std::move(key_dag), ExpressionActionsSettings(context));
    ActionsDAGWithInversionPushDown inverted_dag(filter_dag->getOutputs().front(), context, /* boolean_context */ true);
    cached_partition_condition.emplace(PartitionKeyCondition{
        .key_data_types = key_data_types,
        .condition = KeyCondition(inverted_dag, context, key_column_names, expression, /* single_point */ true),
    });
    return &*cached_partition_condition;
}

const CalendarConstraints & FilePruner::getCalendarConstraints(const String & column_name, const DataTypePtr & column_type) const
{
    const auto it = source_range_constraints.find(column_name);
    if (it != source_range_constraints.end())
        return it->second;

    CalendarConstraints constraints;
    if (const auto fn_it = function_form_constraints.find(column_name); fn_it != function_form_constraints.end())
        constraints = fn_it->second;

    if (filter_dag)
    {
        if (const auto calendar = makeSourceCalendar(column_type))
        {
            const NameAndTypePair key_column(column_name, removeNullable(column_type));
            auto expression = std::make_shared<ExpressionActions>(ActionsDAG({key_column}), ExpressionActionsSettings(context));
            ActionsDAGWithInversionPushDown inverted_dag(filter_dag->getOutputs().front(), context, /* boolean_context */ true);
            KeyCondition source_condition(inverted_dag, context, {column_name}, expression);
            Ranges ranges;
            if (source_condition.extractPlainRanges(ranges))
                intersectCalendarConstraints(constraints, envelopeFromRanges(ranges, *calendar));
        }
    }

    return source_range_constraints.emplace(column_name, std::move(constraints)).first->second;
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

    if (partition_spec && !partition_spec->empty())
    {
        /// Calendar transforms (year/month/day): exact bucket constraints from the filter.
        for (const auto & field : *partition_spec)
        {
            const String transform = Poco::toLower(field.transform);
            if (transform != "year" && transform != "month" && transform != "day")
                continue;
            if (static_cast<size_t>(field.partition_key_index) >= file.partition_values.size())
                continue;
            const auto & raw_value = file.partition_values[field.partition_key_index];
            if (!raw_value.has_value())
                continue;
            int64_t value;
            try
            {
                value = std::stoll(*raw_value);
            }
            catch (...)
            {
                continue;
            }
            const auto column_it = column_types_by_id.find(field.column_id);
            if (column_it == column_types_by_id.end())
                continue;

            const auto & constraints = getCalendarConstraints(column_it->second.name, column_it->second.type);
            const BucketRange & range = transform == "year" ? constraints.year : transform == "month" ? constraints.month : constraints.day;
            if (!range.contains(value))
                return true;
        }

        /// Identity transforms: single-point KeyCondition over the identity key columns.
        if (const auto * condition = getIdentityCondition(*partition_spec))
        {
            std::vector<FieldRef> point;
            bool usable = true;
            size_t key_index = 0;
            for (const auto & field : *partition_spec)
            {
                if (Poco::toLower(field.transform) != "identity")
                    continue;
                /// Mirror the skip in getIdentityCondition so point and key stay aligned.
                if (!column_types_by_id.contains(field.column_id))
                    continue;
                if (static_cast<size_t>(field.partition_key_index) >= file.partition_values.size())
                {
                    usable = false;
                    break;
                }
                const auto & raw_value = file.partition_values[field.partition_key_index];
                if (!raw_value.has_value())
                {
                    /// NULL partition values sort after everything (NULL_LAST).
                    point.emplace_back(POSITIVE_INFINITY);
                }
                else
                {
                    auto value = parseStatsValue(*raw_value, condition->key_data_types[key_index]);
                    if (!value.has_value())
                    {
                        usable = false;
                        break;
                    }
                    point.emplace_back(std::move(*value));
                }
                ++key_index;
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
