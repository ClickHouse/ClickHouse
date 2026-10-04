#pragma once

#include <functional>
#include <source_location>

namespace DB::Runtime::Wyrd
{

void sync(const char * operation, std::source_location location = std::source_location::current());
void yield(std::source_location location = std::source_location::current());
void futexWait(void * address, std::source_location location = std::source_location::current());
void futexWake(void * address, size_t count, std::source_location location = std::source_location::current());
void spawn(std::function<void()> body, std::source_location location = std::source_location::current());
bool spuriousFailure(std::source_location location = std::source_location::current());

}
