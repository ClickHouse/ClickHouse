#pragma once

#include <Common/Wyrd/Strategies/Adversary/Adversary.h>

namespace DB::Wyrd
{

Adversary * adversary();
void setAdversary(AdversaryParams params);
void resetAdversary();

}
