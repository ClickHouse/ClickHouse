#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_DATA_PART_NAME;
}


MergeTreePartInfo MergeTreePartInfo::fromPartName(const String & part_name, MergeTreeDataFormatVersion format_version)
{
    MergeTreePartInfo part_info;
    if (!tryParsePartName(part_name, &part_info, format_version))
        throw Exception("Unexpected part name: " + part_name, ErrorCodes::BAD_DATA_PART_NAME);
    return part_info;
}


bool MergeTreePartInfo::tryParsePartName(const String & part_name, MergeTreePartInfo * part_info, MergeTreeDataFormatVersion format_version)
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
            return false;
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
    {
        return false;
    }

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
        return false;
    }

    /// Sanity check
    if (min_block_num > max_block_num)
    {
        return false;
    }

    if (!in.eof())
    {
        if (!checkChar('_', in)
            || !tryReadIntText(mutation, in)
            || !in.eof())
        {
            return false;
        }
    }

    if (part_info)
    {
        part_info->partition_id = std::move(partition_id);
        part_info->min_block = min_block_num;
        part_info->max_block = max_block_num;
        part_info->level = level;
        part_info->mutation = mutation;
    }

    return true;
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
        throw Exception("Unexpected part name: " + part_name, ErrorCodes::BAD_DATA_PART_NAME);
    }

    const auto & date_lut = DateLUT::instance();

    min_date = date_lut.YYYYMMDDToDayNum(min_yyyymmdd);
    max_date = date_lut.YYYYMMDDToDayNum(max_yyyymmdd);

    auto min_month = date_lut.toNumYYYYMM(min_date);
    auto max_month = date_lut.toNumYYYYMM(max_date);

    if (min_month != max_month)
        throw Exception("Part name " + part_name + " contains different months", ErrorCodes::BAD_DATA_PART_NAME);
}


bool MergeTreePartInfo::contains(const String & outer_part_name, const String & inner_part_name, MergeTreeDataFormatVersion format_version)
{
    MergeTreePartInfo outer = fromPartName(outer_part_name, format_version);
    MergeTreePartInfo inner = fromPartName(inner_part_name, format_version);
    return outer.contains(inner);
}


String MergeTreePartInfo::getPartName() const
{
    WriteBufferFromOwnString wb;

    writeString(partition_id, wb);
    writeChar('_', wb);
    writeIntText(min_block, wb);
    writeChar('_', wb);
    writeIntText(max_block, wb);
    writeChar('_', wb);
    writeIntText(level, wb);

    if (mutation)
    {
        writeChar('_', wb);
        writeIntText(mutation, wb);
    }

    return wb.str();
}


String MergeTreePartInfo::getPartNameV0(DayNum left_date, DayNum right_date) const
{
    const auto & date_lut = DateLUT::instance();

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
    writeIntText(level, wb);

    if (mutation)
    {
        writeChar('_', wb);
        writeIntText(mutation, wb);
    }

    return wb.str();
}

bool DetachedPartInfo::tryParseDetachedPartName(const String & dir_name, DetachedPartInfo & part_info,
                                                MergeTreeDataFormatVersion format_version)
{
    part_info.dir_name = dir_name;

    /// First, try to parse as <part_name>.
    // TODO what if tryParsePartName will parse prefix as partition_id? It can happen if dir_name doesn't contain mutation number at the end
    if (MergeTreePartInfo::tryParsePartName(dir_name, &part_info, format_version))
        return part_info.valid_name = true;

    /// Next, as <prefix>_<partname>. Use entire name as prefix if it fails.
    part_info.prefix = dir_name;
    const auto first_separator = dir_name.find_first_of('_');
    if (first_separator == String::npos)
        return part_info.valid_name = false;

    // TODO what if <prefix> contains '_'?
    const auto part_name = dir_name.substr(first_separator + 1,
                                           dir_name.size() - first_separator - 1);
    if (!MergeTreePartInfo::tryParsePartName(part_name, &part_info, format_version))
        return part_info.valid_name = false;

    part_info.prefix = dir_name.substr(0, first_separator);
    return part_info.valid_name = true;
}

}
