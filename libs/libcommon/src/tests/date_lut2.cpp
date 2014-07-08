#include <iostream>

#include <Yandex/DateLUT.h>
#include <Yandex/time2str.h>


void loop(time_t begin, time_t end, int step)
{
	DateLUT & date_lut = DateLUT::instance();
	
	for (time_t t = begin; t < end; t += step)
		std::cout << Time2Sql(t)
			<< ", " << Time2Sql(date_lut.toTimeInaccurate(t))
			<< ", " << date_lut.toHourInaccurate(t)
			<< std::endl;
}


int main(int argc, char ** argv)
{
	loop(OrderedIdentifier2Date(20101031), OrderedIdentifier2Date(20101101), 15 * 60);
	loop(OrderedIdentifier2Date(20100328), OrderedIdentifier2Date(20100330), 15 * 60);

	return 0;
}
