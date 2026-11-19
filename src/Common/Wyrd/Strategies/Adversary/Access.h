#pragma once

#include <Processors/Executors/Runtime/Wyrd/Strategies/Adversary/Adversary.h>

namespace DB::Runtime::Wyrd
{

Adversary * adversary();
void setAdversary(AdversaryParams params);
void resetAdversary();

}
