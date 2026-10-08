#pragma once

#include <string>

namespace DB::Runtime::Wyrd
{

void enterThread();
void exitThread();
void enterLockFreeScope();
void leaveLockFreeScope();
void progressHint();
void trace(std::string message);

}
