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
	if (peak)
		logPeakMemoryUsage();

	if (amount && !next)
		CurrentMetrics::sub(metric, amount);
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
		CurrentMetrics::add(metric, size);

	Int64 current_limit = limit.load(std::memory_order_relaxed);

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
			<< ", maximum: " << formatReadableSizeWithBinarySuffix(current_limit);

		throw DB::Exception(message.str(), DB::ErrorCodes::MEMORY_LIMIT_EXCEEDED);
	}

	if (unlikely(current_limit && will_be > current_limit))
	{
		free(size);

		std::stringstream message;
		message << "Memory limit";
		if (description)
			message << " " << description;
		message << " exceeded: would use " << formatReadableSizeWithBinarySuffix(will_be)
			<< " (attempt to allocate chunk of " << size << " bytes)"
			<< ", maximum: " << formatReadableSizeWithBinarySuffix(current_limit);

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
		CurrentMetrics::sub(metric, size);
}


void MemoryTracker::reset()
{
	if (!next)
		CurrentMetrics::sub(metric, amount);

	amount.store(0, std::memory_order_relaxed);
	peak.store(0, std::memory_order_relaxed);
	limit.store(0, std::memory_order_relaxed);
}


void MemoryTracker::setOrRaiseLimit(Int64 value)
{
	/// This is just atomic set to maximum.
	Int64 old_value = limit.load(std::memory_order_relaxed);
	while (old_value < value && !limit.compare_exchange_weak(old_value, value))
		;
}


__thread MemoryTracker * current_memory_tracker = nullptr;
