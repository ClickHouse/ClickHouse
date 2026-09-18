#include <Processors/Transforms/PromQLPartialGroupMergeTransform.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Common/Exception.h>

#include <algorithm>
#include <vector>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int TOO_MANY_ROWS_OR_BYTES;
}

SharedHeader PromQLPartialGroupMergeTransform::transformHeader(const AggregateFunctionPtr & sum_function)
{
    if (!sum_function)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL partial group merge aggregate function is null");

    auto group_type = std::make_shared<DataTypeUInt64>();
    auto values_type = sum_function->getResultType();
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(group_type->createColumn(), group_type, TimeSeriesColumnNames::Group),
        ColumnWithTypeAndName(values_type->createColumn(), values_type, TimeSeriesColumnNames::Values),
    });
}

PromQLPartialGroupMergeTransform::PromQLPartialGroupMergeTransform(
    SharedHeader input_header_, AggregateFunctionPtr sum_function_, size_t max_output_groups_)
    : IAccumulatingTransform(input_header_, transformHeader(sum_function_))
    , sum_function(std::move(sum_function_))
    , max_output_groups(max_output_groups_)
{
    if (sum_function->getName() != "sumForEach")
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "PromQL partial group merge expects sumForEach, got {}", sum_function->getName());
    if (max_output_groups == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PromQL partial group merge requires a positive output group limit");

    if (!input_header_->has(TimeSeriesColumnNames::Group) || !input_header_->has(TimeSeriesColumnNames::Values))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL partial group merge expects {} and {} columns, got {}",
            TimeSeriesColumnNames::Group,
            TimeSeriesColumnNames::Values,
            input_header_->dumpStructure());

    group_position = input_header_->getPositionByName(TimeSeriesColumnNames::Group);
    values_position = input_header_->getPositionByName(TimeSeriesColumnNames::Values);

    const auto & group_type = input_header_->getByPosition(group_position).type;
    const auto expected_group_type = std::make_shared<DataTypeUInt64>();
    if (!group_type->equals(*expected_group_type))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL partial group merge expects {} to have type UInt64, got {}",
            TimeSeriesColumnNames::Group,
            group_type->getName());

    const auto & values_type = input_header_->getByPosition(values_position).type;
    if (!values_type->equals(*sum_function->getResultType()))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL partial group merge expects {} to have type {}, got {}",
            TimeSeriesColumnNames::Values,
            sum_function->getResultType()->getName(),
            values_type->getName());

    const auto & sum_arguments = sum_function->getArgumentTypes();
    if (sum_arguments.size() != 1 || !sum_arguments.front()->equals(*values_type))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "PromQL partial group merge expects sumForEach({}), got {}",
            values_type->getName(),
            sum_arguments.empty() ? String{"<missing>"} : sum_arguments.front()->getName());
}

PromQLPartialGroupMergeTransform::~PromQLPartialGroupMergeTransform()
{
    destroyStates();
}

void PromQLPartialGroupMergeTransform::consume(Chunk chunk)
{
    if (!chunk.getNumRows())
        return;

    const auto & columns = chunk.getColumns();
    const auto * group_column = typeid_cast<const ColumnUInt64 *>(columns[group_position].get());
    if (!group_column)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PromQL partial group merge expected {} to be UInt64, got {}",
            TimeSeriesColumnNames::Group,
            columns[group_position]->getName());

    const IColumn * sum_arguments[] = {columns[values_position].get()};
    for (size_t row = 0; row < chunk.getNumRows(); ++row)
    {
        AggregateDataPtr group_place = getOrCreateGroupState(group_column->getElement(row));
        sum_function->add(group_place, sum_arguments, row, &group_arena);
    }
}

AggregateDataPtr PromQLPartialGroupMergeTransform::getOrCreateGroupState(UInt64 group)
{
    if (auto it = group_states.find(group); it != group_states.end())
        return it->getMapped();

    if (group_states.size() >= max_output_groups)
        throw Exception(
            ErrorCodes::TOO_MANY_ROWS_OR_BYTES,
            "PromQL partial group merge exceeded its limit of {} output groups",
            max_output_groups);

    decltype(group_states)::LookupResult it = nullptr;
    bool inserted = false;
    group_states.emplace(group, it, inserted);
    chassert(inserted);

    try
    {
        AggregateDataPtr new_place = group_arena.alignedAlloc(sum_function->sizeOfData(), sum_function->alignOfData());
        sum_function->create(new_place);
        it->getMapped() = new_place;
    }
    catch (...)
    {
        group_states.erase(group);
        throw;
    }
    return it->getMapped();
}

Chunk PromQLPartialGroupMergeTransform::generate()
{
    if (generated)
        return {};

    generated = true;
    if (group_states.empty())
        return {};

    std::vector<UInt64> groups;
    groups.reserve(group_states.size());
    for (const auto & entry : group_states)
        groups.push_back(entry.getKey());
    std::sort(groups.begin(), groups.end());

    auto group_column = ColumnUInt64::create();
    auto values_column = sum_function->getResultType()->createColumn();
    for (UInt64 group : groups)
    {
        group_column->insertValue(group);
        sum_function->insertResultInto(group_states.find(group)->getMapped(), *values_column, &group_arena);
    }

    return Chunk(Columns{std::move(group_column), std::move(values_column)}, groups.size());
}

void PromQLPartialGroupMergeTransform::destroyStates() noexcept
{
    if (sum_function)
    {
        for (auto & entry : group_states)
        {
            if (entry.getMapped())
                sum_function->destroy(entry.getMapped());
        }
    }
    group_states.clear();
}

}
