#include <common/likely.h>
#include <common/logger_useful.h>
#include <DB/Common/Exception.h>
#include <DB/Common/CurrentMetrics.h>
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
	if (peak)
		logPeakMemoryUsage();

	if (amount && !next)
		CurrentMetrics::sub(CurrentMetrics::MemoryTracking, amount);
}


void MemoryTracker::logPeakMemoryUsage() const
{
	LOG_DEBUG(&Logger::get("MemoryTracker"),
		"Peak memory usage" << (description ? " " + std::string(description) : "")
		<< ": " << formatReadableSizeWithBinarySuffix(peak) << ".");
}


void MemoryTracker::alloc(Int64 size)
{
	Int64 will_be = amount += size;

	if (!next)
		CurrentMetrics::add(CurrentMetrics::MemoryTracking, size);

	/// Using non-thread-safe random number generator. Joint distribution in different threads would not be uniform.
	/// In this case, it doesn't matter.
	if (unlikely(fault_probability && drand48() < fault_probability))
	{
		free(size);

		std::stringstream message;
		message << "Memory tracker";
		if (description)
			message << " " << description;
		message << ": fault injected. Would use " << formatReadableSizeWithBinarySuffix(will_be)
			<< " (attempt to allocate chunk of " << size << " bytes)"
			<< ", maximum: " << formatReadableSizeWithBinarySuffix(limit);

		throw DB::Exception(message.str(), DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED);
	}

	if (unlikely(limit && will_be > limit))
	{
		free(size);

		std::stringstream message;
		message << "Memory limit";
		if (description)
			message << " " << description;
		message << " exceeded: would use " << formatReadableSizeWithBinarySuffix(will_be)
			<< " (attempt to allocate chunk of " << size << " bytes)"
			<< ", maximum: " << formatReadableSizeWithBinarySuffix(limit);

		throw DB::Exception(message.str(), DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED);
	}

	if (will_be > peak.load(std::memory_order_relaxed))		/// Races doesn't matter. Could rewrite with CAS, but not worth.
		peak.store(will_be, std::memory_order_relaxed);

	if (next)
		next->alloc(size);
}


void MemoryTracker::free(Int64 size)
{
	amount -= size;

	if (next)
		next->free(size);
	else
		CurrentMetrics::sub(CurrentMetrics::MemoryTracking, size);
}


void MemoryTracker::reset()
{
	if (!next)
		CurrentMetrics::sub(CurrentMetrics::MemoryTracking, amount);

	amount = 0;
	peak = 0;
	limit = 0;
}


__thread MemoryTracker * current_memory_tracker = nullptr;
