#include <Common/Wyrd/Strategies/Mode.h>

namespace DB::Wyrd
{

namespace
{

Mode current_mode = Mode::Adversary;

}

Mode getMode()
{
    return current_mode;
}

void setMode(Mode value)
{
    current_mode = value;
}

}
