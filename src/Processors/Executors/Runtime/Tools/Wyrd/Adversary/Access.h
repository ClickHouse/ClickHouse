#pragma once

#include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/Adversary.h>

namespace DB::Runtime::Wyrd
{

Adversary * adversary();
void setAdversary(AdversaryParams params);
void resetAdversary();

}
