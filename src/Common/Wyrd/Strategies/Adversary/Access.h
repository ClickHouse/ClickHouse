#pragma once

#include <Common/Wyrd/Strategies/Adversary/Adversary.h>

namespace DB::Wyrd::AdversaryMode
{

Adversary * adversary();
void setAdversary(AdversaryParams params);
void resetAdversary();

}
