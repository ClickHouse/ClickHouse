#pragma once

namespace DB::Runtime::Wyrd
{

void enterThread();
void exitThread();
void enterLockFreeScope();
void leaveLockFreeScope();
void progressHint();

}
