#include <Storages/ObjectStorage/DataLakes/DeltaLake/DataFileStatistics.h>

#if USE_DELTA_KERNEL_RS

#include <Common/assert_cast.h>
#include <Common/intExp10.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>

#include <unordered_set>

namespace DeltaLake
{

namespace
{

bool hasStatisticsType(const DB::DataTypePtr & type)
{
    switch (DB::removeNullable(type)->getTypeId())
    {
        case DB::TypeIndex::Int8:
        case DB::TypeIndex::Int16:
        case DB::TypeIndex::Int32:
        case DB::TypeIndex::Int64:
        case DB::TypeIndex::Float32:
        case DB::TypeIndex::Float64:
        case DB::TypeIndex::Date32:
        case DB::TypeIndex::DateTime64:
        case DB::TypeIndex::String:
        case DB::TypeIndex::Decimal32:
        case DB::TypeIndex::Decimal64:
        case DB::TypeIndex::Decimal128:
            return true;
        default:
            return false;
    }
}

std::optional<Int64> getInteger(const ::FileStatsValue & value)
{
    switch (value.tag)
    {
        case ::FileStatsValue::Tag::Byte: return value.byte._0;
        case ::FileStatsValue::Tag::Short: return value.short_._0;
        case ::FileStatsValue::Tag::Integer: return value.integer._0;
        case ::FileStatsValue::Tag::Long: return value.long_._0;
        default: return {};
    }
}

template <typename T>
std::optional<DB::Field> makeDecimal(Int128 value, UInt32 scale)
{
    using NativeType = typename T::NativeType;
    if (value < std::numeric_limits<NativeType>::min() || value > std::numeric_limits<NativeType>::max())
        return {};
    return DB::DecimalField<T>(static_cast<NativeType>(value), scale);
}

std::optional<DB::Field> convertValue(const ::FileStatsValue & value, const DB::IDataType & type, bool is_max)
{
    switch (type.getTypeId())
    {
        case DB::TypeIndex::Int8:
        case DB::TypeIndex::Int16:
        case DB::TypeIndex::Int32:
        case DB::TypeIndex::Int64:
            if (auto integer = getInteger(value))
                return DB::Field(*integer);
            return {};
        case DB::TypeIndex::Float32:
            if (value.tag == ::FileStatsValue::Tag::Float)
                return DB::Field(static_cast<Float64>(value.float_._0));
            return {};
        case DB::TypeIndex::Float64:
            if (value.tag == ::FileStatsValue::Tag::Float)
                return DB::Field(static_cast<Float64>(value.float_._0));
            if (value.tag == ::FileStatsValue::Tag::Double)
                return DB::Field(value.double_._0);
            return {};
        case DB::TypeIndex::Date32:
            if (value.tag == ::FileStatsValue::Tag::Date)
                return DB::Field(static_cast<Int64>(value.date._0));
            return {};
        case DB::TypeIndex::DateTime64:
        {
            Int64 micros = 0;
            if (value.tag == ::FileStatsValue::Tag::Timestamp)
                micros = value.timestamp._0;
            else if (value.tag == ::FileStatsValue::Tag::TimestampNtz)
                micros = value.timestamp_ntz._0;
            else
                return {};
            if (assert_cast<const DB::DataTypeDateTime64 &>(type).getScale() != 6)
                return {};
            if (is_max && micros <= std::numeric_limits<Int64>::max() - 999)
                micros += 999;
            return DB::DecimalField<DB::DateTime64>(micros, 6);
        }
        case DB::TypeIndex::String:
            if (value.tag == ::FileStatsValue::Tag::String)
                return DB::Field(KernelUtils::fromDeltaString(value.string._0));
            return {};
        case DB::TypeIndex::Decimal32:
        case DB::TypeIndex::Decimal64:
        case DB::TypeIndex::Decimal128:
        {
            if (value.tag != ::FileStatsValue::Tag::Decimal)
                return {};
            const auto & decimal = value.decimal._0;
            const auto scale = DB::getDecimalScale(type);
            if (decimal.scale != scale)
                return {};
            const Int128 unscaled = (static_cast<Int128>(decimal.high) << 64) | static_cast<Int128>(decimal.low);
            if (type.getTypeId() == DB::TypeIndex::Decimal32)
                return makeDecimal<DB::Decimal32>(unscaled, scale);
            if (type.getTypeId() == DB::TypeIndex::Decimal64)
                return makeDecimal<DB::Decimal64>(unscaled, scale);
            return makeDecimal<DB::Decimal128>(unscaled, scale);
        }
        default:
            return {};
    }
}

}

StatisticsColumns getStatisticsColumns(
    const DB::NamesAndTypesList & table_schema,
    const DB::Names & partition_columns,
    const DB::NameToNameMap & physical_names_map)
{
    const std::unordered_set<std::string_view> partition_columns_set(partition_columns.begin(), partition_columns.end());
    StatisticsColumns result;
    for (const auto & column : table_schema)
    {
        if (partition_columns_set.contains(column.name) || !hasStatisticsType(column.type))
            continue;
        auto physical_name = physical_names_map.empty()
            ? column.name
            : getPhysicalName(appendToLogicalPath({}, column.name), physical_names_map);
        result.push_back({column, std::move(physical_name)});
    }
    return result;
}

size_t DataFileStatistics::allocatedBytes() const
{
    size_t bytes = columns.capacity() * sizeof(ColumnStatistics);
    for (const auto & column : columns)
    {
        if (column.min && column.min->getType() == DB::Field::Types::String)
            bytes += column.min->safeGet<String>().capacity();
        if (column.max && column.max->getType() == DB::Field::Types::String)
            bytes += column.max->safeGet<String>().capacity();
    }
    return bytes;
}

DataFileStatisticsConverter::DataFileStatisticsConverter(const StatisticsColumns & columns_)
    : columns(columns_)
{
    for (size_t i = 0; i < columns.size(); ++i)
        column_index_by_physical_name.emplace(columns[i].physical_name, i);
}

DataFileStatistics DataFileStatisticsConverter::convert(const ::Stats & stats) const
{
    DataFileStatistics result;
    result.num_records = stats.num_records;
    if (stats.columns_len == 0)
        return result;

    result.columns.resize(columns.size());
    for (size_t i = 0; i < stats.columns_len; ++i)
    {
        const auto & column_stats = stats.columns[i];
        if (column_stats.path_len != 1)
            continue;
        const auto & name = column_stats.path[0];
        const auto it = column_index_by_physical_name.find(std::string_view(name.ptr, name.len));
        if (it == column_index_by_physical_name.end())
            continue;

        const auto type = DB::removeNullable(columns[it->second].column.type);
        auto & column = result.columns[it->second];
        column.min = convertValue(column_stats.min, *type, /* is_max */ false);
        column.max = convertValue(column_stats.max, *type, /* is_max */ true);
        if (column_stats.null_count >= 0)
            column.null_count = static_cast<UInt64>(column_stats.null_count);
    }
    return result;
}

StatisticsPruner::StatisticsPruner(const DB::ActionsDAG & filter_dag, const StatisticsColumns & columns, DB::ContextPtr context)
{
    std::unordered_set<std::string_view> filter_inputs;
    for (const auto * input : filter_dag.getInputs())
        filter_inputs.insert(input->result_name);

    for (size_t i = 0; i < columns.size(); ++i)
    {
        const auto & column = columns[i].column;
        if (!filter_inputs.contains(column.name))
            continue;

        auto expression = std::make_shared<DB::ExpressionActions>(DB::ActionsDAG({column}), DB::ExpressionActionsSettings(context));
        DB::ActionsDAGWithInversionPushDown inverted_dag(filter_dag.getOutputs().front(), context, /* boolean_context */ true);
        DB::KeyCondition condition(inverted_dag, context, {column.name}, expression);
        DB::DataTypes data_types{column.type};
        if (DB::KeyCondition::typeMayHideNaN(column.type))
            condition.relaxAtomsOverNaNHidingColumns(data_types);
        if (condition.alwaysUnknownOrTrue())
            continue;
        conditions.push_back({i, std::move(data_types), std::move(condition)});
    }
}

bool StatisticsPruner::canBePruned(const DataFileStatistics & statistics) const
{
    for (const auto & [column_index, data_types, condition] : conditions)
    {
        if (column_index >= statistics.columns.size())
            continue;
        const auto & column = statistics.columns[column_index];

        bool may_be_true = true;
        if (column.min && column.max)
        {
            const DB::FieldRef left(*column.min);
            const DB::FieldRef right(*column.max);
            may_be_true = condition.mayBeTrueInRange(1, &left, &right, data_types);
        }
        else if (column.null_count && statistics.num_records && *column.null_count == *statistics.num_records)
        {
            may_be_true = false;
        }

        const bool may_have_nulls = data_types.front()->isNullable() && (!column.null_count || *column.null_count > 0);
        if (!may_be_true && may_have_nulls)
        {
            const DB::FieldRef null_point(DB::POSITIVE_INFINITY);
            may_be_true = condition.mayBeTrueInRange(1, &null_point, &null_point, data_types);
        }

        if (!may_be_true)
            return true;
    }
    return false;
}

}

#endif
