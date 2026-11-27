#include <Common/Wyrd/Facade/Random.h>
#include <Common/Wyrd/Strategies/Random.h>

#include <limits>

namespace DB::Wyrd
{

UInt64 randomNumber()
{
    return randomInt(0, std::numeric_limits<UInt64>::max());
}

UInt64 randomNumberBetween(UInt64 from, UInt64 to)
{
    return randomInt(from, to);
}

double randomReal(double from, double to)
{
    return randomDouble(from, to);
}

bool randomEvent(double probability)
{
    return randomBool(probability);
}

}
