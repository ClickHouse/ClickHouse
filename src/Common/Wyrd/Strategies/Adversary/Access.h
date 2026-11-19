#pragma once

#include <Common/Wyrd/Namespace.h>
#include <Common/Wyrd/Strategies/Adversary/Adversary.h>

namespace WYRD_NAMESPACE(DB::Wyrd)
{

Adversary * adversary();
void setAdversary(AdversaryParams params);
void resetAdversary();

}
