#include <DataTypes/DataTypeExponentialTimeDecayingFloat64.h>

namespace DB
{

void DataTypeCustomExponentialTimeDecayingFloat64::validateColumn(
    const IColumn & column, const String & operation) const
{
    validateExponentialTimeDecayingFloat64Column(column, decay_length, operation);
}

}
