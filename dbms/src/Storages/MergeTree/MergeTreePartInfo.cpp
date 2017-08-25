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


MergeTreePartInfo MergeTreePartInfo::fromPartName(const String & dir_name, MergeTreeDataFormatVersion format_version)
{
    MergeTreePartInfo part_info;
    if (!tryParsePartName(dir_name, &part_info, format_version))
        throw Exception("Unexpected part name: " + dir_name, ErrorCodes::BAD_DATA_PART_NAME);
    return part_info;
}


bool MergeTreePartInfo::tryParsePartName(const String & dir_name, MergeTreePartInfo * part_info, MergeTreeDataFormatVersion format_version)
{
    ReadBufferFromString in(dir_name);

    String partition_id;
    if (format_version == 0)
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

    Int64 min_block_num = 0;
    Int64 max_block_num = 0;
    UInt32 level = 0;

    if (!tryReadIntText(min_block_num, in)
        || !checkChar('_', in)
        || !tryReadIntText(max_block_num, in)
        || !checkChar('_', in)
        || !tryReadIntText(level, in)
        || !in.eof())
    {
        return false;
    }

    if (part_info)
    {
        part_info->partition_id = std::move(partition_id);
        part_info->min_block = min_block_num;
        part_info->max_block = max_block_num;
        part_info->level = level;
    }

    return true;
}


void MergeTreePartInfo::parseMinMaxDatesFromPartName(const String & dir_name, DayNum_t & min_date, DayNum_t & max_date)
{
    UInt32 min_yyyymmdd = 0;
    UInt32 max_yyyymmdd = 0;

    ReadBufferFromString in(dir_name);

    if (!tryReadIntText(min_yyyymmdd, in)
        || !checkChar('_', in)
        || !tryReadIntText(max_yyyymmdd, in))
    {
        throw Exception("Unexpected part name: " + dir_name, ErrorCodes::BAD_DATA_PART_NAME);
    }

    const auto & date_lut = DateLUT::instance();

    min_date = date_lut.YYYYMMDDToDayNum(min_yyyymmdd);
    max_date = date_lut.YYYYMMDDToDayNum(max_yyyymmdd);

    DayNum_t min_month = date_lut.toFirstDayNumOfMonth(min_date);
    DayNum_t max_month = date_lut.toFirstDayNumOfMonth(max_date);

    if (min_month != max_month)
        throw Exception("Part name " + dir_name + " contains different months", ErrorCodes::BAD_DATA_PART_NAME);
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

    return wb.str();
}


String MergeTreePartInfo::getPartNameV0(DayNum_t left_date, DayNum_t right_date) const
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

    return wb.str();
}

}
