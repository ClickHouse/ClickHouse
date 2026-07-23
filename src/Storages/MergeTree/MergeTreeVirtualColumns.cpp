#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Core/Names.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

static ASTPtr getCompressionCodecDeltaLZ4()
{
    return makeASTFunction("CODEC",
        make_intrusive<ASTIdentifier>("Delta"),
        make_intrusive<ASTIdentifier>("LZ4"));
}

const String RowExistsColumn::name = "_row_exists";
const DataTypePtr RowExistsColumn::type = std::make_shared<DataTypeUInt8>();

bool isLightweightDeleteAssignment(const ASTAssignment & assignment)
{
    if (assignment.column_name != RowExistsColumn::name)
        return false;
    /// `DELETE FROM` rewrites to `_row_exists = 0`; only that exact literal is a delete. Any other
    /// expression (e.g. `_row_exists = 1` to resurrect rows) is a real update of the deletion mask.
    const auto * literal = assignment.expression()->as<ASTLiteral>();
    return literal && literal->value == Field(static_cast<UInt64>(0));
}

const String BlockNumberColumn::name = "_block_number";
const DataTypePtr BlockNumberColumn::type = std::make_shared<DataTypeUInt64>();
const ASTPtr BlockNumberColumn::codec = getCompressionCodecDeltaLZ4();

const String BlockOffsetColumn::name = "_block_offset";
const DataTypePtr BlockOffsetColumn::type = std::make_shared<DataTypeUInt64>();
const ASTPtr BlockOffsetColumn::codec = getCompressionCodecDeltaLZ4();

const String PartDataVersionColumn::name = "_part_data_version";
const DataTypePtr PartDataVersionColumn::type = std::make_shared<DataTypeUInt64>();

const String PartitionIdColumn::name = "_partition_id";
const DataTypePtr PartitionIdColumn::type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

const String PartitionValueColumn::name = "_partition_value";
DataTypePtr PartitionValueColumn::type(const KeyDescription * partition_key)
{
    auto partition_types = partition_key->sample_block.getDataTypes();
    return std::make_shared<DataTypeTuple>(std::move(partition_types));
}

VirtualColumnsDescription getMergeTreeVirtuals(const KeyDescription * partition_key)
{
    VirtualColumnsDescription desc;

    desc.addEphemeral("_part", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Name of part", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_part_index", std::make_shared<DataTypeUInt64>(), "Sequential index of the part in the query result", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_part_starting_offset", std::make_shared<DataTypeUInt64>(), "Cumulative starting row of the part in the query result", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_part_uuid", std::make_shared<DataTypeUUID>(), "Unique part identifier (if enabled MergeTree setting assign_part_uuids)", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral(PartitionIdColumn::name, PartitionIdColumn::type, "Name of partition", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_sample_factor", std::make_shared<DataTypeFloat64>(), "Sample factor (from the query)", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_part_offset", std::make_shared<DataTypeUInt64>(), "Number of row in the part", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_part_granule_offset", std::make_shared<DataTypeUInt64>(), "Number of granule in the part", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral(PartDataVersionColumn::name, PartDataVersionColumn::type, "Data version of part (either min block number or mutation version)", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_disk_name", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Disk name", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_distance", std::make_shared<DataTypeFloat32>(), "Pre-computed distance for vector search queries", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Reader);

    if (partition_key && partition_key->sample_block.columns() > 0)
        desc.addEphemeral(PartitionValueColumn::name, PartitionValueColumn::type(partition_key), "Value (a tuple) of a PARTITION BY expression", VirtualsMaterializationPlace::Reader);

    desc.addPersistent(RowExistsColumn::name, RowExistsColumn::type, nullptr, "Persisted mask created by lightweight delete that show whether row exists or is deleted");
    desc.addPersistent(BlockNumberColumn::name, BlockNumberColumn::type, BlockNumberColumn::codec, "Persisted original number of block that was assigned at insert");
    desc.addPersistent(BlockOffsetColumn::name, BlockOffsetColumn::type, BlockOffsetColumn::codec, "Persisted original number of row in block that was assigned at insert");

    return desc;
}

bool isVirtualColumn(const String & column_name)
{
    /// Derived from the one registry (`getMergeTreeVirtuals`) so it cannot drift from the set a
    /// MergeTree table actually exposes. Built once (partition-key-independent membership).
    /// `_partition_value` is registered only with a partition key, so add it explicitly here —
    /// it is a virtual name regardless of whether the current table has a partition key.
    static const NameSet virtual_columns = []
    {
        NameSet names;
        for (const auto & column : getMergeTreeVirtuals(nullptr))
            names.insert(column.name);
        names.insert(PartitionValueColumn::name);
        return names;
    }();
    return virtual_columns.contains(column_name);
}

Field getFieldForConstVirtualColumn(const String & column_name, const IMergeTreeDataPart & part_or_projection)
{
    const auto & part = part_or_projection.isProjectionPart() ? *part_or_projection.getParentPart() : part_or_projection;

    if (column_name == RowExistsColumn::name)
        return 1ULL;

    if (column_name == BlockNumberColumn::name)
        return part.info.min_block;

    if (column_name == "_part")
        return part.name;

    if (column_name == "_part_uuid")
        return part.uuid;

    if (column_name == "_partition_id")
        return part.info.getPartitionId();

    if (column_name == PartDataVersionColumn::name)
        return part.info.getDataVersion();

    if (column_name == "_partition_value")
        return Tuple(part.partition.value.begin(), part.partition.value.end());

    if (column_name == "_disk_name")
        return part.getDataPartStorage().getDiskName();

    throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Unexpected const virtual column: {}", column_name);
}

}
