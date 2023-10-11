#include <QueryCoordination/Optimizer/Statistics/ColumnStatistics.h>

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

Float64 ColumnStatistics::calculateSelectivity(OP_TYPE, Float64 /*value*/)
{
    return 1.0; /// TODO
}

void ColumnStatistics::updateValues(OP_TYPE op_type, Float64 value)
{
    switch (op_type) /// TODO
    {
        case OP_TYPE::EQUAL:
            min_value = value;
            max_value = value;
            ndv = 1.0;
            break;
        default:
            break;
    }
}

void ColumnStatistics::mergeColumnByUnion(ColumnStatisticsPtr other)
{
    if (other->isUnKnown() && !isUnKnown())
    {
        *this = *unknown();
        return;
    }
    this->setMinValue(std::min(this->getMinValue(), other->getMinValue()));
    this->setMaxValue(std::max(this->getMaxValue(), other->getMaxValue()));
}

void ColumnStatistics::mergeColumnByIntersect(ColumnStatisticsPtr other)
{
    if (other->isUnKnown())
    {
        *this = *unknown();
        return;
    }
    this->setMinValue(std::max(this->getMinValue(), other->getMinValue()));
    this->setMaxValue(std::min(this->getMaxValue(), other->getMaxValue()));
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
    min_value = minValue;
}

Float64 ColumnStatistics::getMaxValue() const
{
    return max_value;
}

void ColumnStatistics::setMaxValue(Float64 maxValue)
{
    max_value = maxValue;
}

Float64 ColumnStatistics::getAvgRowSize() const
{
    return avg_row_size;
}

void ColumnStatistics::setAvgRowSize(Float64 avgRowSize)
{
    avg_row_size = std::max(1.0, avgRowSize);
}

}
