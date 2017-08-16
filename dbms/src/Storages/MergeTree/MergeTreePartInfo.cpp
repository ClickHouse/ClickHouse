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


MergeTreePartInfo MergeTreePartInfo::fromPartName(const String & dir_name)
{
    MergeTreePartInfo part_info;
    if (!tryParsePartName(dir_name, &part_info))
        throw Exception("Unexpected part name: " + dir_name, ErrorCodes::BAD_DATA_PART_NAME);
    return part_info;
}

bool MergeTreePartInfo::tryParsePartName(const String & dir_name, MergeTreePartInfo * part_info)
{
    UInt32 min_yyyymmdd = 0;
    UInt32 max_yyyymmdd = 0;
    Int64 min_block_num = 0;
    Int64 max_block_num = 0;
    UInt32 level = 0;

    ReadBufferFromString in(dir_name);

    if (!tryReadIntText(min_yyyymmdd, in)
        || !checkChar('_', in)
        || !tryReadIntText(max_yyyymmdd, in)
        || !checkChar('_', in)
        || !tryReadIntText(min_block_num, in)
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
        part_info->partition_id = dir_name.substr(0, strlen("YYYYMM"));
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


bool MergeTreePartInfo::contains(const String & outer_part_name, const String & inner_part_name)
{
    MergeTreePartInfo outer = fromPartName(outer_part_name);
    MergeTreePartInfo inner = fromPartName(inner_part_name);
    return outer.contains(inner);
}


String MergeTreePartInfo::getPartName(DayNum_t left_date, DayNum_t right_date, Int64 left_id, Int64 right_id, UInt64 level)
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
    writeIntText(left_id, wb);
    writeChar('_', wb);
    writeIntText(right_id, wb);
    writeChar('_', wb);
    writeIntText(level, wb);

    return wb.str();
}

}
