#pragma once

#include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/IAdversary.h>

namespace DB::Runtime::Wyrd
{

/// Injects nothing; every method is empty and no call fails spuriously.
AdversaryPtr createNopAdversary();

}
