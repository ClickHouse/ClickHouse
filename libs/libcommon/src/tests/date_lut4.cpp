#include <iostream>
#include <Yandex/DateLUT.h>


int main(int argc, char ** argv)
{
	/** В DateLUT был глюк - для времён из дня 1970-01-01, возвращался номер часа больше 23. */
	static const time_t TIME = 66130;

	DateLUT & date_lut = DateLUT::instance();

	std::cerr << date_lut.toHourInaccurate(TIME) << std::endl;
	std::cerr << date_lut.toDayNum(TIME) << std::endl;

	const DateLUT::Values * values = reinterpret_cast<const DateLUT::Values *>(&date_lut);

	std::cerr << values[0].date << ", " << time_t(values[1].date - values[0].date) << std::endl;

	return 0;
}
