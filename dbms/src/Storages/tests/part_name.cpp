#include <DB/Storages/MergeTree/ActiveDataPartSet.h>
#include <Yandex/time2str.h>
#include <mysqlxx/DateTime.h>


int main(int argc, char ** argv)
{
	DayNum_t today = DateLUT::instance().toDayNum(time(0));

	for (DayNum_t date = today; DayNum_t(date + 10) > today; --date)
	{
		std::string name = DB::ActiveDataPartSet::getPartName(date, date, 0, 0, 0);
		std::cerr << name << '\n';

		time_t time = OrderedIdentifier2Date(name);
		std::cerr << mysqlxx::DateTime(time) << '\n';
	}

	return 0;
}
