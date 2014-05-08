#include <Yandex/likely.h>
#include <Yandex/logger_useful.h>
#include <DB/Core/Exception.h>
#include <DB/IO/WriteHelpers.h>
#include <iomanip>

#include <DB/Common/MemoryTracker.h>


static std::string formatReadableSize(double size)
{
	const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"};
	size_t i = 0;
	while (i + 1 < sizeof(units) / sizeof(units[0]) &&
			fabs(size) >= 1024)
	{
		size /= 1024;
        ++i;
    }
    std::stringstream ss;
    ss << std::fixed << std::setprecision(i) << size << units[i];
    return ss.str();
}

MemoryTracker::~MemoryTracker()
{
	LOG_DEBUG(&Logger::get("MemoryTracker"), "Peak memory usage for query: " << formatReadableSize(peak) << ".");
}

void MemoryTracker::alloc(Int64 size)
{
	Int64 will_be = __sync_add_and_fetch(&amount, size);

	if (unlikely(limit && will_be > limit))
	{
		free(size);
		throw DB::Exception("Memory limit exceeded: would use " + formatReadableSize(will_be) + ""
			" (attempt to allocate chunk of " + DB::toString(size) + " bytes)"
			", maximum: " + formatReadableSize(limit), DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED);
	}

	if (will_be > peak)
		peak = will_be;
}


__thread MemoryTracker * current_memory_tracker = nullptr;
