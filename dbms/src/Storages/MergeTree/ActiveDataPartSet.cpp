#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_DATA_PART_NAME;
}


ActiveDataPartSet::ActiveDataPartSet(const Strings & names)
{
    for (const auto & name : names)
        addImpl(name);
}


void ActiveDataPartSet::add(const String & name)
{
    std::lock_guard<std::mutex> lock(mutex);
    addImpl(name);
}


void ActiveDataPartSet::addImpl(const String & name)
{
    if (getContainingPartImpl(name) != "")
        return;

    Part part;
    part.name = name;
    parsePartName(name, part);

    /// Parts contained in `part` are located contiguously inside `data_parts`, overlapping with the place where the part itself would be inserted.
    Parts::iterator it = parts.lower_bound(part);

    /// Let's go left.
    while (it != parts.begin())
    {
        --it;
        if (!part.contains(*it))
        {
            ++it;
            break;
        }
        parts.erase(it++);
    }

    /// Let's go to the right.
    while (it != parts.end() && part.contains(*it))
    {
        parts.erase(it++);
    }

    parts.insert(part);
}


String ActiveDataPartSet::getContainingPart(const String & part_name) const
{
    std::lock_guard<std::mutex> lock(mutex);
    return getContainingPartImpl(part_name);
}


String ActiveDataPartSet::getContainingPartImpl(const String & part_name) const
{
    Part part;
    parsePartName(part_name, part);

    /// A part can only be covered/overlapped by the previous or next one in `parts`.
    Parts::iterator it = parts.lower_bound(part);

    if (it != parts.end())
    {
        if (it->name == part_name)
            return it->name;
        if (it->contains(part))
            return it->name;
    }

    if (it != parts.begin())
    {
        --it;
        if (it->contains(part))
            return it->name;
    }

    return "";
}


Strings ActiveDataPartSet::getParts() const
{
    std::lock_guard<std::mutex> lock(mutex);

    Strings res;
    res.reserve(parts.size());
    for (const Part & part : parts)
        res.push_back(part.name);

    return res;
}


size_t ActiveDataPartSet::size() const
{
    std::lock_guard<std::mutex> lock(mutex);
    return parts.size();
}


String ActiveDataPartSet::getPartName(DayNum_t left_date, DayNum_t right_date, Int64 left_id, Int64 right_id, UInt64 level)
{
    const auto & date_lut = DateLUT::instance();

    /// Directory name for the part has form: `YYYYMMDD_YYYYMMDD_N_N_L`.
    String res;
    {
        unsigned left_date_id = date_lut.toNumYYYYMMDD(left_date);
        unsigned right_date_id = date_lut.toNumYYYYMMDD(right_date);

        WriteBufferFromString wb(res);

        writeIntText(left_date_id, wb);
        writeChar('_', wb);
        writeIntText(right_date_id, wb);
        writeChar('_', wb);
        writeIntText(left_id, wb);
        writeChar('_', wb);
        writeIntText(right_id, wb);
        writeChar('_', wb);
        writeIntText(level, wb);
    }

    return res;
}


bool ActiveDataPartSet::isPartDirectory(const String & dir_name)
{
    return parsePartNameImpl(dir_name, nullptr);
}

bool ActiveDataPartSet::parsePartNameImpl(const String & dir_name, Part * part)
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

    if (part)
    {
        const auto & date_lut = DateLUT::instance();

        part->left_date = date_lut.YYYYMMDDToDayNum(min_yyyymmdd);
        part->right_date = date_lut.YYYYMMDDToDayNum(max_yyyymmdd);
        part->left = min_block_num;
        part->right = max_block_num;
        part->level = level;

        DayNum_t left_month = date_lut.toFirstDayNumOfMonth(part->left_date);
        DayNum_t right_month = date_lut.toFirstDayNumOfMonth(part->right_date);

        if (left_month != right_month)
            throw Exception("Part name " + dir_name + " contains different months", ErrorCodes::BAD_DATA_PART_NAME);

        part->month = left_month;
    }

    return true;
}

void ActiveDataPartSet::parsePartName(const String & dir_name, Part & part)
{
    if (!parsePartNameImpl(dir_name, &part))
        throw Exception("Unexpected part name: " + dir_name, ErrorCodes::BAD_DATA_PART_NAME);
}

bool ActiveDataPartSet::contains(const String & outer_part_name, const String & inner_part_name)
{
    Part outer;
    Part inner;
    parsePartName(outer_part_name, outer);
    parsePartName(inner_part_name, inner);
    return outer.contains(inner);
}

}
