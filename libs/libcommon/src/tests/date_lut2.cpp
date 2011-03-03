#include <iostream>

#include <Yandex/DateLUT.h>
#include <Yandex/time2str.h>


void loop(time_t begin, time_t end, int step)
{
	Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();
	
	for (time_t t = begin; t < end; t += step)
		std::cout << Yandex::Time2Sql(t)
			<< ", " << Yandex::Time2Sql(date_lut.toTimeInaccurate(t))
			<< ", " << date_lut.toHourInaccurate(t)
			<< std::endl;
}


int main(int argc, char ** argv)
{
	loop(Yandex::OrderedIdentifier2Date(20101031), Yandex::OrderedIdentifier2Date(20101101), 15 * 60);
	loop(Yandex::OrderedIdentifier2Date(20100328), Yandex::OrderedIdentifier2Date(20100330), 15 * 60);

	return 0;
}
