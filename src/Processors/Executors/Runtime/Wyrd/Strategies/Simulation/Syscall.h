#pragma once

#include <functional>
#include <source_location>
#include <string>

namespace DB::Runtime::Wyrd
{

void sync(const char * operation, std::source_location location = std::source_location::current());
void yield(std::source_location location = std::source_location::current());
void futexWait(void * address, std::source_location location = std::source_location::current());
void futexWake(void * address, size_t count, std::source_location location = std::source_location::current());
void spawn(std::function<void()> body, std::source_location location = std::source_location::current());
void enterLockFreeScope(std::source_location location = std::source_location::current());
void leaveLockFreeScope(std::source_location location = std::source_location::current());
void progressHint(std::source_location location = std::source_location::current());
void trace(std::string message);
void brokenInvariant(std::string message);

}
