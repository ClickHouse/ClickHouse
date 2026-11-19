#pragma once

namespace DB::Wyrd
{

void enterThread();
void exitThread();
void enterLockFreeScope();
void leaveLockFreeScope();
void progressHint();

}
