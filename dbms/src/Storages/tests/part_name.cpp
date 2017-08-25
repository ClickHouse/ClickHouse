#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <common/LocalDateTime.h>


int main(int argc, char ** argv)
{
    DayNum_t today = DateLUT::instance().toDayNum(time(0));

    for (DayNum_t date = today; DayNum_t(date + 10) > today; --date)
    {
        DB::MergeTreePartInfo part_info("partition", 0, 0, 0);
        std::string name = part_info.getPartNameV0(date, date);
        std::cerr << name << '\n';

        time_t time = DateLUT::instance().YYYYMMDDToDate(DB::parse<UInt32>(name));
        std::cerr << LocalDateTime(time) << '\n';
    }

    return 0;
}
