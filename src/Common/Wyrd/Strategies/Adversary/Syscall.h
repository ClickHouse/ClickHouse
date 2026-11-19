#pragma once

#include <exception>

namespace DB::Wyrd
{

void enterThread();
void failThread(std::exception_ptr exception);
void exitThread();
void enterLockFreeScope();
void leaveLockFreeScope();
void progressHint();

}
