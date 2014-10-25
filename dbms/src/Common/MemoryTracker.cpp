#include <Yandex/likely.h>
#include <Yandex/logger_useful.h>
#include <DB/Core/Exception.h>
#include <DB/Common/formatReadable.h>
#include <DB/IO/WriteHelpers.h>
#include <iomanip>

#include <DB/Common/MemoryTracker.h>


MemoryTracker::~MemoryTracker()
{
	LOG_DEBUG(&Logger::get("MemoryTracker"), "Peak memory usage for query: " << formatReadableSizeWithBinarySuffix(peak) << ".");
}

void MemoryTracker::alloc(Int64 size)
{
	Int64 will_be = __sync_add_and_fetch(&amount, size);

	if (unlikely(limit && will_be > limit))
	{
		free(size);
		throw DB::Exception("Memory limit exceeded: would use " + formatReadableSizeWithBinarySuffix(will_be) + ""
			" (attempt to allocate chunk of " + DB::toString(size) + " bytes)"
			", maximum: " + formatReadableSizeWithBinarySuffix(limit), DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED);
	}

	if (will_be > peak)
		peak = will_be;
}


__thread MemoryTracker * current_memory_tracker = nullptr;
