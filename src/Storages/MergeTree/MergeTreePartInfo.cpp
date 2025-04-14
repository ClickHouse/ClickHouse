#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/ProtocolDefines.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_DATA_PART_NAME;
    extern const int INVALID_PARTITION_VALUE;
    extern const int UNKNOWN_FORMAT_VERSION;
}


MergeTreePartInfo MergeTreePartInfo::fromPartName(const String & part_name, MergeTreeDataFormatVersion format_version)
{
    if (auto part_opt = tryParsePartName(part_name, format_version))
        return *part_opt;
    throw Exception(ErrorCodes::BAD_DATA_PART_NAME, "Unexpected part name: {} for format version: {}", part_name, format_version);
}

void MergeTreePartInfo::validatePartitionID(const ASTPtr & partition_id_ast, MergeTreeDataFormatVersion format_version)
{
    std::string partition_id;
    if (auto * literal = partition_id_ast->as<ASTLiteral>(); literal != nullptr && literal->value.getType() == Field::Types::String)
        partition_id = literal->value.safeGet<String>();

    else
        throw Exception(ErrorCodes::INVALID_PARTITION_VALUE, "Partition id must be string literal");

    if (partition_id.empty())
        throw Exception(ErrorCodes::INVALID_PARTITION_VALUE, "Partition id is empty");

    if (format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        if (partition_id.size() != 6 || !std::all_of(partition_id.begin(), partition_id.end(), isNumericASCII))
            throw Exception(ErrorCodes::INVALID_PARTITION_VALUE,
                "Invalid partition format: {}. Partition should consist of 6 digits: YYYYMM",
                partition_id);
    }
    else
    {
        auto is_valid_char = [](char c) { return c == '-' || isAlphaNumericASCII(c); };
        if (!std::all_of(partition_id.begin(), partition_id.end(), is_valid_char))
            throw Exception(ErrorCodes::INVALID_PARTITION_VALUE, "Invalid partition format: {}", partition_id);
    }

}

std::optional<MergeTreePartInfo> MergeTreePartInfo::tryParsePartName(
    std::string_view part_name, MergeTreeDataFormatVersion format_version)
{
    ReadBufferFromString in(part_name);

    String partition_id;

    if (format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        UInt32 min_yyyymmdd = 0;
        UInt32 max_yyyymmdd = 0;

        if (!tryReadIntText(min_yyyymmdd, in)
            || !checkChar('_', in)
            || !tryReadIntText(max_yyyymmdd, in)
            || !checkChar('_', in))
        {
            return std::nullopt;
        }

        partition_id = toString(min_yyyymmdd / 100);
    }
    else
    {
        while (!in.eof())
        {
            char c;
            readChar(c, in);
            if (c == '_')
                break;

            partition_id.push_back(c);
        }
    }

    /// Sanity check
    if (partition_id.empty())
        return std::nullopt;

    Int64 min_block_num = 0;
    Int64 max_block_num = 0;
    UInt32 level = 0;
    UInt32 mutation = 0;

    if (!tryReadIntText(min_block_num, in)
        || !checkChar('_', in)
        || !tryReadIntText(max_block_num, in)
        || !checkChar('_', in)
        || !tryReadIntText(level, in))
    {
        return std::nullopt;
    }

    /// Sanity check
    if (min_block_num > max_block_num)
        return std::nullopt;

    if (!in.eof())
    {
        if (!checkChar('_', in)
            || !tryReadIntText(mutation, in)
            || !in.eof())
        {
            return std::nullopt;
        }
    }

    MergeTreePartInfo part_info;

    part_info.partition_id = std::move(partition_id);
    part_info.min_block = min_block_num;
    part_info.max_block = max_block_num;

    if (level == LEGACY_MAX_LEVEL)
    {
        /// We (accidentally) had two different max levels until 21.6 and it might cause logical errors like
        /// "Part 20170601_20170630_0_2_999999999 intersects 201706_0_1_4294967295".
        /// So we replace unexpected max level to make contains(...) method and comparison operators work
        /// correctly with such virtual parts. On part name serialization we will use legacy max level to keep the name unchanged.
        part_info.use_legacy_max_level = true;
        level = MAX_LEVEL;
    }

    part_info.level = level;
    part_info.mutation = mutation;

    return part_info;
}


void MergeTreePartInfo::parseMinMaxDatesFromPartName(const String & part_name, DayNum & min_date, DayNum & max_date)
{
    UInt32 min_yyyymmdd = 0;
    UInt32 max_yyyymmdd = 0;

    ReadBufferFromString in(part_name);

    if (!tryReadIntText(min_yyyymmdd, in)
        || !checkChar('_', in)
        || !tryReadIntText(max_yyyymmdd, in))
    {
        throw Exception(ErrorCodes::BAD_DATA_PART_NAME, "Unexpected part name: {}", part_name);
    }

    const auto & date_lut = DateLUT::serverTimezoneInstance();

    min_date = date_lut.YYYYMMDDToDayNum(min_yyyymmdd);
    max_date = date_lut.YYYYMMDDToDayNum(max_yyyymmdd);

    auto min_month = date_lut.toNumYYYYMM(min_date);
    auto max_month = date_lut.toNumYYYYMM(max_date);

    if (min_month != max_month)
        throw Exception(ErrorCodes::BAD_DATA_PART_NAME, "Part name {} contains different months", part_name);
}


bool MergeTreePartInfo::contains(const String & outer_part_name, const String & inner_part_name, MergeTreeDataFormatVersion format_version)
{
    MergeTreePartInfo outer = fromPartName(outer_part_name, format_version);
    MergeTreePartInfo inner = fromPartName(inner_part_name, format_version);
    return outer.contains(inner);
}


String MergeTreePartInfo::getPartNameAndCheckFormat(MergeTreeDataFormatVersion format_version) const
{
    if (format_version == MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        return getPartNameV1();

    /// We cannot just call getPartNameV0 because it requires extra arguments, but at least we can warn about it.
    chassert(false);  /// Catch it in CI. Feel free to remove this line.
    throw Exception(ErrorCodes::BAD_DATA_PART_NAME, "Trying to get part name in new format for old format version. "
                    "Either some new feature is incompatible with deprecated *MergeTree definition syntax or it's a bug.");
}


String MergeTreePartInfo::getPartNameForLogs() const
{
    /// We don't care about format version here
    return getPartNameV1();
}

String MergeTreePartInfo::getPartNameV1() const
{
    WriteBufferFromOwnString wb;

    writeString(partition_id, wb);
    writeChar('_', wb);
    writeIntText(min_block, wb);
    writeChar('_', wb);
    writeIntText(max_block, wb);
    writeChar('_', wb);
    if (use_legacy_max_level)
    {
        assert(level == MAX_LEVEL);
        writeIntText(LEGACY_MAX_LEVEL, wb);
    }
    else
    {
        writeIntText(level, wb);
    }

    if (mutation)
    {
        writeChar('_', wb);
        writeIntText(mutation, wb);
    }

    return wb.str();
}


String MergeTreePartInfo::getPartNameV0(DayNum left_date, DayNum right_date) const
{
    const auto & date_lut = DateLUT::serverTimezoneInstance();

    /// Directory name for the part has form: `YYYYMMDD_YYYYMMDD_N_N_L`.

    unsigned left_date_id = date_lut.toNumYYYYMMDD(left_date);
    unsigned right_date_id = date_lut.toNumYYYYMMDD(right_date);

    WriteBufferFromOwnString wb;

    writeIntText(left_date_id, wb);
    writeChar('_', wb);
    writeIntText(right_date_id, wb);
    writeChar('_', wb);
    writeIntText(min_block, wb);
    writeChar('_', wb);
    writeIntText(max_block, wb);
    writeChar('_', wb);
    if (use_legacy_max_level)
    {
        assert(level == MAX_LEVEL);
        writeIntText(LEGACY_MAX_LEVEL, wb);
    }
    else
    {
        writeIntText(level, wb);
    }

    if (mutation)
    {
        writeChar('_', wb);
        writeIntText(mutation, wb);
    }

    return wb.str();
}

void MergeTreePartInfo::serialize(WriteBuffer & out) const
{
    UInt64 version = DBMS_MERGE_TREE_PART_INFO_VERSION;
    /// Must be the first
    writeIntBinary(version, out);

    writeStringBinary(partition_id, out);
    writeIntBinary(min_block, out);
    writeIntBinary(max_block, out);
    writeIntBinary(level, out);
    writeIntBinary(mutation, out);
    writeBoolText(use_legacy_max_level, out);
}


String MergeTreePartInfo::describe() const
{
    return getPartNameV1();
}


void MergeTreePartInfo::deserialize(ReadBuffer & in)
{
    UInt64 version;
    readIntBinary(version, in);
    if (version != DBMS_MERGE_TREE_PART_INFO_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Version for MergeTreePart info mismatched. Got: {}, supported version: {}",
            version, DBMS_MERGE_TREE_PART_INFO_VERSION);

    readStringBinary(partition_id, in);
    readIntBinary(min_block, in);
    readIntBinary(max_block, in);
    readIntBinary(level, in);
    readIntBinary(mutation, in);
    readBoolText(use_legacy_max_level, in);
}

bool MergeTreePartInfo::areAllBlockNumbersCovered(const MergeTreePartInfo & blocks_range, std::vector<MergeTreePartInfo> candidates)
{
    if (candidates.empty())
        return false;

    std::sort(candidates.begin(), candidates.end());

    /// First doesn't cover left border
    if (candidates[0].min_block != blocks_range.min_block)
        return false;

    int64_t current_right_block = candidates[0].min_block - 1;

    for (const auto & candidate : candidates)
    {
        if (current_right_block + 1 != candidate.min_block)
            return false;

        current_right_block = candidate.max_block;
    }

    if (current_right_block != blocks_range.max_block)
        return false;

    return true;
}

DetachedPartInfo DetachedPartInfo::parseDetachedPartName(
    const DiskPtr & disk, std::string_view dir_name, MergeTreeDataFormatVersion format_version)
{
    DetachedPartInfo part_info;
    part_info.disk = disk;
    part_info.dir_name = dir_name;

    /// First, try to find known prefix and parse dir_name as <prefix>_<part_name>.
    /// Arbitrary strings are not allowed for partition_id, so known_prefix cannot be confused with partition_id.
    for (std::string_view known_prefix : DETACH_REASONS)
    {
        if (dir_name.starts_with(known_prefix)
            && known_prefix.size() < dir_name.size()
            && dir_name[known_prefix.size()] == '_')
        {
            part_info.prefix = known_prefix;

            const std::string_view part_name = dir_name.substr(known_prefix.size() + 1);

            if (auto part_opt = MergeTreePartInfo::tryParsePartName(part_name, format_version))
            {
                part_info.valid_name = true;
                part_info.addParsedPartInfo(*part_opt);
            }
            else
                part_info.valid_name = false;

            return part_info;
        }
    }

    /// Next, try to parse dir_name as <part_name>.
    if (auto part_opt = MergeTreePartInfo::tryParsePartName(dir_name, format_version))
    {
        part_info.valid_name = true;
        part_info.addParsedPartInfo(*part_opt);
        return part_info;
    }

    /// Next, as <prefix>_<partname>. Use entire name as prefix if it fails.
    part_info.prefix = dir_name;

    const size_t first_separator = dir_name.find_first_of('_');

    if (first_separator == String::npos)
    {
        part_info.valid_name = false;
        return part_info;
    }

    const std::string_view part_name = dir_name.substr(
        first_separator + 1,
        dir_name.size() - first_separator - 1);

    if (auto part_opt = MergeTreePartInfo::tryParsePartName(part_name, format_version))
    {
        part_info.valid_name = true;
        part_info.prefix = dir_name.substr(0, first_separator);
        part_info.addParsedPartInfo(*part_opt);
    }
    else
        part_info.valid_name = false;

    // TODO what if name contains "_tryN" suffix?
    return part_info;
}

void DetachedPartInfo::addParsedPartInfo(const MergeTreePartInfo & part)
{
    // Both class are aggregates so it's ok.
    static_cast<MergeTreePartInfo &>(*this) = part;
}

}
