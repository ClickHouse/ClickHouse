#include <Yandex/likely.h>
#include <Yandex/logger_useful.h>
#include <DB/Core/Exception.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Common/MemoryTracker.h>


MemoryTracker::~MemoryTracker()
{
	LOG_DEBUG(&Logger::get("MemoryTracker"), "Peak memory usage for query: " << peak << " bytes.");
}

void MemoryTracker::alloc(Int32 size)
{
	Int32 will_be = __sync_add_and_fetch(&amount, size);

	if (unlikely(limit && will_be > limit))
	{
		free(size);
		throw DB::Exception("Memory limit exceeded: would use " + DB::toString(will_be) + " bytes"
			" (attempt to allocate chunk of " + DB::toString(size) + " bytes)"
			", maximum: " + DB::toString(limit) + " bytes", DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED);
	}

	if (will_be > peak)
		peak = will_be;
}


__thread MemoryTracker * current_memory_tracker = nullptr;
