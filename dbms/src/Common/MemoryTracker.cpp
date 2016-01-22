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
	Int64 will_be = __sync_add_and_fetch(&amount, size);

	if (!next)
		CurrentMetrics::add(CurrentMetrics::MemoryTracking, size);

	/// Используется непотокобезопасный генератор случайных чисел. Совместное распределение в разных потоках не будет равномерным.
	/// В данном случае, это нормально.
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

	if (will_be > peak)
		peak = will_be;

	if (next)
		next->alloc(size);
}


void MemoryTracker::free(Int64 size)
{
	__sync_sub_and_fetch(&amount, size);

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
