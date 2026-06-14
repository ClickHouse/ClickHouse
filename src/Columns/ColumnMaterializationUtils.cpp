#include <Columns/ColumnMaterializationUtils.h>

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnSparse.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Common/assert_cast.h>

namespace DB
{

ColumnPtr convertToSerialization(const ColumnPtr & column, const IDataType & type, const ISerialization::KindStack & kind_stack)
{
    if (!ISerialization::hasKind(kind_stack, ISerialization::Kind::LOW_CARDINALITY))
        return recursiveRemoveNonNativeLowCardinality(column);

    /// The column is written with non-native LowCardinality serialization, which requires a
    /// ColumnLowCardinality in memory. Build it (the dictionary) from the full column.
    auto full = recursiveRemoveSparse(column);
    if (full->lowCardinality())
        return full;

    auto new_column = createEmptyLowCardinalityColumn(type, /*is_native=*/false);
    assert_cast<ColumnLowCardinality &>(*new_column).insertRangeFromFullColumn(*full, 0, full->size());
    return new_column;
}

void convertToSerializations(Block & block, const SerializationInfoByName & infos)
{
    for (auto & column : block)
        column.column = convertToSerialization(column.column, *column.type, infos.getKindStack(column.name));
}

}
