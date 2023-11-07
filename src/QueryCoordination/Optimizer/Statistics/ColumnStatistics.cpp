#include <QueryCoordination/Optimizer/Statistics/ColumnStatistics.h>
#include <QueryCoordination/Optimizer/Statistics/Utils.h>

namespace DB
{

ColumnStatisticsPtr ColumnStatistics::unknown()
{
    auto ret = std::make_shared<ColumnStatistics>();
    ret->is_unknown = true;
    return ret;
}

ColumnStatisticsPtr ColumnStatistics::create(Float64 value)
{
    return std::make_shared<ColumnStatistics>(value);
}

ColumnStatisticsPtr ColumnStatistics::clone()
{
    return std::make_shared<ColumnStatistics>(min_value, max_value, ndv, avg_row_size, data_type, is_unknown, histogram);
}

Float64 ColumnStatistics::calculateByValue(OP_TYPE op_type, Float64 value)
{
    if (isUnKnown())
        return 0.1; /// TODO add to settings

    /// whether the column is a number.
    bool nan = isNumber(data_type);
    return nan ? calculateForNaN(op_type) : calculateForNumber(op_type, value);
}

Float64 ColumnStatistics::calculateForNaN(OP_TYPE op_type)
{
    Float64 selectivity;
    switch (op_type)
    {
        case OP_TYPE::EQUAL:
            selectivity = 1 / ndv;
            ndv = 1.0;
            break;
        case OP_TYPE::NOT_EQUAL:
            selectivity = ndv - 1 / ndv;
            ndv = std::max(1.0, ndv - 1);
            break;
        case OP_TYPE::GREATER:
        case OP_TYPE::GREATER_OR_EQUAL:
        case OP_TYPE::LESS:
        case OP_TYPE::LESS_OR_EQUAL:
            selectivity = 0.5; /// TODO add to settings
            break;
    }
    return selectivity;
}

Float64 ColumnStatistics::calculateForNumber(OP_TYPE op_type, Float64 value)
{
    Float64 selectivity;

    auto value_in_range = value >= min_value && value <= max_value;

    auto value_at_left = value < min_value;
    auto value_at_right = value > max_value;

    switch (op_type)
    {
        case OP_TYPE::EQUAL:
            if (value_in_range)
            {
                min_value = value;
                max_value = value;
                selectivity = 1 / ndv;
            }
            else
            {
                min_value = 0.0;
                max_value = 0.0;
                selectivity = 0;
            }
            setNdv(1.0);
            break;
        case OP_TYPE::NOT_EQUAL:
            if (value_in_range)
            {
                selectivity = (ndv - 1) / ndv;
                setNdv(ndv - 1);
            }
            else
            {
                selectivity = 1.0;
            }
            break;
        case OP_TYPE::GREATER:
        case OP_TYPE::GREATER_OR_EQUAL:
            if (value_at_left)
            {
                /// select all
                selectivity = 1.0;
            }
            else if (value_at_right)
            {
                /// select nothing
                min_value = 0.0;
                max_value = 0.0;
                ndv = 1.0;
                selectivity = 0.0;
            }
            else
            {
                /// select partial
                selectivity = (max_value - value) / std::max(1.0, max_value - min_value);
                min_value = value;
                setNdv(selectivity * ndv);
            }
            break;
        case OP_TYPE::LESS:
        case OP_TYPE::LESS_OR_EQUAL:
            if (value_at_left)
            {
                /// select nothing
                min_value = 0.0;
                max_value = 0.0;
                ndv = 1.0;
                selectivity = 0;
            }
            else if (value_at_right)
            {
                /// select all
                selectivity = 1.0;
            }
            else
            {
                selectivity = (value - min_value) / std::max(1.0, max_value - min_value);
                max_value = value;
                setNdv(selectivity * ndv);
            }
            break;
    }

    return selectivity;
}

void ColumnStatistics::mergeColumnValueByUnion(ColumnStatisticsPtr other)
{
    if (this->isUnKnown())
        return;
    if (other->isUnKnown() && !isUnKnown())
    {
        *this = *unknown();
        return;
    }
    setMinValue(std::min(this->getMinValue(), other->getMinValue()));
    setMaxValue(std::max(this->getMaxValue(), other->getMaxValue()));
}

void ColumnStatistics::mergeColumnValueByIntersect(ColumnStatisticsPtr other)
{
    if (this->isUnKnown())
        return;
    if (other->isUnKnown())
    {
        *this = *unknown();
        return;
    }
    setMinValue(std::max(this->getMinValue(), other->getMinValue()));
    setMaxValue(std::min(this->getMaxValue(), other->getMaxValue()));
}

void ColumnStatistics::revertColumnValue()
{
    if (this->isUnKnown())
        return;

    if (isNumber(data_type))
    {
    }
    else
    {
        /// do nothing
    }
}

bool ColumnStatistics::isUnKnown() const
{
    return is_unknown;
}

Float64 ColumnStatistics::getNdv() const
{
    return ndv;
}

void ColumnStatistics::setNdv(Float64 new_value)
{
    ndv = std::max(1.0, new_value);
}

Float64 ColumnStatistics::getMinValue() const
{
    return min_value;
}

void ColumnStatistics::setMinValue(Float64 minValue)
{
    min_value = std::max(1.0, minValue);
}

Float64 ColumnStatistics::getMaxValue() const
{
    return max_value;
}

void ColumnStatistics::setMaxValue(Float64 maxValue)
{
    max_value = std::max(1.0, maxValue);
}

Float64 ColumnStatistics::getAvgRowSize() const
{
    return avg_row_size;
}

void ColumnStatistics::setAvgRowSize(Float64 avgRowSize)
{
    avg_row_size = std::max(1.0, avgRowSize);
}

bool ColumnStatistics::inRange(Float64 value)
{
    if (isNumeric(data_type))
        return value <= max_value && value >= min_value;
    else
        return true;
}

const DataTypePtr & ColumnStatistics::getDataType() const
{
    return data_type;
}

void ColumnStatistics::setDataType(const DataTypePtr & dataType)
{
    data_type = dataType;

    if (data_type->haveMaximumSizeOfValue())
    {
        auto fixed_row_size = data_type->getMaximumSizeOfValueInMemory();
        avg_row_size = fixed_row_size;
    }
    else
    {
        avg_row_size = 8; /// TODO add to settings
    }
}

}
