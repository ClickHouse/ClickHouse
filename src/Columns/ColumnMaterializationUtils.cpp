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

ColumnPtr convertToSerialization(const ColumnPtr & column, const IDataType & type, bool low_cardinality)
{
    if (!low_cardinality)
        return recursiveRemoveNonNativeLowCardinality(column);

    /// The column is written with non-native LowCardinality serialization, which requires a
    /// ColumnLowCardinality in memory. Build it (the dictionary) from the full column.
    auto full = recursiveRemoveSparse(column->convertToFullColumnIfConst());
    if (full->lowCardinality())
        return full;

    auto new_column = createEmptyLowCardinalityColumn(type, /*is_native=*/false);
    assert_cast<ColumnLowCardinality &>(*new_column).insertRangeFromFullColumn(*full, 0, full->size());
    return new_column;
}

void convertToSerializations(Block & block, const SerializationInfoByName & infos)
{
    /// This runs for every written block, so look the kinds up without copying them.
    for (auto & column : block)
    {
        auto it = infos.find(column.name);
        const bool low_cardinality
            = it != infos.end() && ISerialization::hasKind(it->second->getKindStack(), ISerialization::Kind::LOW_CARDINALITY);

        column.column = convertToSerialization(column.column, *column.type, low_cardinality);
    }
}

}
