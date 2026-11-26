#pragma once

namespace DB::Wyrd
{

enum class Mode
{
    Adversary,
    Simulation,
};

Mode getMode();
void setMode(Mode value);

}
