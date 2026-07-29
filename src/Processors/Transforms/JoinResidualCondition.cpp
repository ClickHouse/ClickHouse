#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Transforms/JoinResidualCondition.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void validateResidualOutput(const ExpressionActions & actions)
{
    const auto & sample = actions.getSampleBlock();
    if (sample.columns() != 1
        || !WhichDataType(removeNullable(removeLowCardinality(sample.getByPosition(0).type))).isUInt8())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join residual condition must have a single boolean output, got {}",
            sample.dumpStructure());
}

JoinResidualCondition resolveJoinResidualCondition(
    ExpressionActionsPtr actions, const Block & left_header, const Block & right_header)
{
    validateResidualOutput(*actions);

    JoinResidualCondition condition;
    condition.actions = std::move(actions);
    for (const auto & required_column : condition.actions->getRequiredColumnsWithTypes())
    {
        bool in_left = left_header.has(required_column.name);
        bool in_right = right_header.has(required_column.name);
        if (in_left == in_right)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Join residual condition input {} must come from exactly one input, found in {}",
                required_column.name, in_left ? "both" : "neither");
        if (in_left)
            condition.inputs.push_back({.side = 0, .position = left_header.getPositionByName(required_column.name)});
        else
            condition.inputs.push_back({.side = 1, .position = right_header.getPositionByName(required_column.name)});
    }
    return condition;
}

JoinResidualConditionEvaluator::JoinResidualConditionEvaluator(
    JoinResidualCondition condition_, const Block & left_header, const Block & right_header)
    : condition(std::move(condition_))
{
    validateResidualOutput(*condition.actions);

    const auto & required_columns = condition.actions->getRequiredColumnsWithTypes();
    if (condition.inputs.size() != required_columns.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join residual condition requires {} columns, {} sources given",
            required_columns.size(), condition.inputs.size());

    size_t i = 0;
    for (const auto & required_column : required_columns)
    {
        const auto & source = condition.inputs[i++];
        const Block & header = source.side == 0 ? left_header : right_header;
        if (source.side >= 2 || source.position >= header.columns())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Join residual condition source ({}, {}) is out of range",
                source.side, source.position);

        const auto & input_column = header.getByPosition(source.position);
        if (!input_column.type->equals(*required_column.type))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Join residual condition input {} has type {}, expected {}",
                required_column.name, input_column.type->getName(), required_column.type->getName());

        input_header.insert(ColumnWithTypeAndName(nullptr, required_column.type, required_column.name));
    }
    input_positions = condition.actions->getInputPositions(input_header);
}

IColumn::Filter JoinResidualConditionEvaluator::evaluateMask(Columns columns, size_t num_rows) const
{
    Columns results = condition.actions->executeOnColumns(std::move(columns), input_header, input_positions, num_rows);
    ColumnPtr result = results.at(0)->convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();

    IColumn::Filter mask(num_rows);
    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(result.get()))
    {
        const auto & null_map = nullable->getNullMapData();
        const auto & values = assert_cast<const ColumnUInt8 &>(nullable->getNestedColumn()).getData();
        for (size_t row = 0; row < num_rows; ++row)
            mask[row] = !null_map[row] && values[row];
    }
    else
    {
        const auto & values = assert_cast<const ColumnUInt8 &>(*result).getData();
        for (size_t row = 0; row < num_rows; ++row)
            mask[row] = values[row] ? 1 : 0;
    }
    return mask;
}

}
