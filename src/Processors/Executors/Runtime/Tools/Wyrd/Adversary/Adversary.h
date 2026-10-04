#pragma once

#include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/IAdversary.h>

#include <string_view>

namespace DB::Runtime::Wyrd
{

IAdversary * adversary();
void setAdversary(AdversaryPtr adversary);

AdversaryPtr createAdversary(std::string_view name);

}
