#pragma once

#include <Common/Wyrd/Namespace.h>

namespace WYRD_NAMESPACE(DB::Wyrd)
{

void enterThread();
void exitThread();
void enterLockFreeScope();
void leaveLockFreeScope();
void progressHint();

}
