#include <iostream>
#include <common/DateLUT.h>


int main(int argc, char ** argv)
{
    /** В DateLUT был глюк - для времён из дня 1970-01-01, возвращался номер часа больше 23. */
    static const time_t TIME = 66130;

    const auto & date_lut = DateLUT::instance();

    std::cerr << date_lut.toHour(TIME) << std::endl;
    std::cerr << date_lut.toDayNum(TIME) << std::endl;

    const auto * values = reinterpret_cast<const DateLUTImpl::Values *>(&date_lut);

    std::cerr << values[0].date << ", " << time_t(values[1].date - values[0].date) << std::endl;

    return 0;
}
