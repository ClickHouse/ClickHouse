#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

static ASTPtr getCompressionCodecDeltaLZ4()
{
    return makeASTFunction("CODEC",
        std::make_shared<ASTIdentifier>("Delta"),
        std::make_shared<ASTIdentifier>("LZ4"));
}

const String RowExistsColumn::name = "_row_exists";
const DataTypePtr RowExistsColumn::type = std::make_shared<DataTypeUInt8>();

const String BlockNumberColumn::name = "_block_number";
const DataTypePtr BlockNumberColumn::type = std::make_shared<DataTypeUInt64>();
const ASTPtr BlockNumberColumn::codec = getCompressionCodecDeltaLZ4();

const String BlockOffsetColumn::name = "_block_offset";
const DataTypePtr BlockOffsetColumn::type = std::make_shared<DataTypeUInt64>();
const ASTPtr BlockOffsetColumn::codec = getCompressionCodecDeltaLZ4();

Field getFieldForConstVirtualColumn(const String & column_name, const IMergeTreeDataPart & part)
{
    if (column_name == RowExistsColumn::name)
        return 1ULL;

    if (column_name == BlockNumberColumn::name)
        return part.info.min_block;

    if (column_name == "_part")
        return part.name;

    if (column_name == "_part_uuid")
        return part.uuid;

    if (column_name == "_partition_id")
        return part.info.partition_id;

    if (column_name == "_part_data_version")
        return part.info.getDataVersion();

    if (column_name == "_partition_value")
        return Tuple(part.partition.value.begin(), part.partition.value.end());

    throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Unexpected const virtual column: {}", column_name);
}

}
