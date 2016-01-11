#include <common/likely.h>
#include <common/logger_useful.h>
#include <DB/Common/Exception.h>
#include <DB/Common/formatReadable.h>
#include <DB/IO/WriteHelpers.h>
#include <iomanip>

#include <DB/Common/MemoryTracker.h>


namespace DB
{
namespace ErrorCodes
{
	extern const int MEMORY_LIMIT_EXCEEDED;
}
}


MemoryTracker::~MemoryTracker()
{
	LOG_DEBUG(&Logger::get("MemoryTracker"), "Peak memory usage for query: " << formatReadableSizeWithBinarySuffix(peak) << ".");
}

void MemoryTracker::alloc(Int64 size)
{
	Int64 will_be = __sync_add_and_fetch(&amount, size);

	/// Используется непотокобезопасный генератор случайных чисел. Совместное распределение в разных потоках не будет равномерным.
	/// В данном случае, это нормально.
	if (unlikely(fault_probability && drand48() < fault_probability))
	{
		free(size);
		throw DB::Exception("Memory tracker: fault injected. Would use " + formatReadableSizeWithBinarySuffix(will_be) + ""
			" (attempt to allocate chunk of " + DB::toString(size) + " bytes)"
			", maximum: " + formatReadableSizeWithBinarySuffix(limit), DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED);
	}

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
