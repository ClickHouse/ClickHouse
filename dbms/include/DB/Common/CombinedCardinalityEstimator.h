#pragma once

#include <DB/Common/HashTable/SmallTable.h>
#include <DB/Common/HashTable/HashSet.h>
#include <DB/Common/HyperLogLogWithSmallSetOptimization.h>


namespace DB
{

namespace details
{

enum class ContainerType { SMALL, MEDIUM, LARGE };

ContainerType max(const ContainerType & lhs, const ContainerType & rhs)
{
	unsigned int res = std::max(static_cast<unsigned int>(lhs), static_cast<unsigned int>(rhs));
	return static_cast<ContainerType>(res);
}

}

/** Для маленького количества ключей - массив фиксированного размера "на стеке".
  * Для среднего - выделяется HashSet.
  * Для большого - выделяется HyperLogLog.
  */
template <typename Key, typename HashContainer, UInt8 small_set_size_max, UInt8 medium_set_power2_max, UInt8 K>
class CombinedCardinalityEstimator
{
public:
	using Self = CombinedCardinalityEstimator<Key, HashContainer, small_set_size_max, medium_set_power2_max, K>;

private:
	using Small = SmallSet<Key, small_set_size_max>;
	using Medium = HashContainer;
	using Large = HyperLogLogWithSmallSetOptimization<Key, small_set_size_max, K>;

public:
	~CombinedCardinalityEstimator()
	{
		if (container_type == details::ContainerType::MEDIUM)
		{
			delete medium;

			if (current_memory_tracker)
				current_memory_tracker->free(sizeof(medium));
		}
		else if (container_type == details::ContainerType::LARGE)
		{
			delete large;

			if (current_memory_tracker)
				current_memory_tracker->free(sizeof(large));
		}
	}

	void insert(Key value)
	{
		if (container_type == details::ContainerType::SMALL)
		{
			if (small.find(value) == small.end())
			{
				if (!small.full())
					small.insert(value);
				else
				{
					toMedium();
					medium->insert(value);
				}
			}
		}
		else if (container_type == details::ContainerType::MEDIUM)
		{
			if (medium->size() < medium_set_size_max)
				medium->insert(value);
			else
			{
				toLarge();
				large->insert(value);
			}
		}
		else if (container_type == details::ContainerType::LARGE)
			large->insert(value);
		else
			throw Poco::Exception("Internal error", ErrorCodes::LOGICAL_ERROR);
	}

	UInt32 size() const
	{
		if (container_type == details::ContainerType::SMALL)
			return small.size();
		else if (container_type == details::ContainerType::MEDIUM)
			return medium->size();
		else if (container_type == details::ContainerType::LARGE)
			return large->size();
		else
			throw Poco::Exception("Internal error", ErrorCodes::LOGICAL_ERROR);
	}

	void merge(const Self & rhs)
	{
		details::ContainerType max_container_type = details::max(container_type, rhs.container_type);

		if (container_type != max_container_type)
		{
			if (max_container_type == details::ContainerType::MEDIUM)
				toMedium();
			else if (max_container_type == details::ContainerType::LARGE)
				toLarge();
		}

		if (container_type == details::ContainerType::SMALL)
		{
			for (const auto & x : rhs.small)
				insert(x);
		}
		else if (container_type == details::ContainerType::MEDIUM)
		{
			if (rhs.container_type == details::ContainerType::SMALL)
			{
				for (const auto & x : rhs.small)
					insert(x);
			}
			else if (rhs.container_type == details::ContainerType::MEDIUM)
			{
				for (const auto & x : *rhs.medium)
					insert(x);
			}
		}
		else if (container_type == details::ContainerType::LARGE)
		{
			if (rhs.container_type == details::ContainerType::SMALL)
			{
				for (const auto & x : rhs.small)
					insert(x);
			}
			else if (rhs.container_type == details::ContainerType::MEDIUM)
			{
				for (const auto & x : *rhs.medium)
					insert(x);
			}
			else if (rhs.container_type == details::ContainerType::LARGE)
				large->merge(*rhs.large);
		}
		else
			throw Poco::Exception("Internal error", ErrorCodes::LOGICAL_ERROR);
	}

	/// Можно вызывать только для пустого объекта.
	void read(DB::ReadBuffer & in)
	{
		UInt8 v;
		readBinary(v, in);
		details::ContainerType t = static_cast<details::ContainerType>(v);

		if (t == details::ContainerType::SMALL)
			small.read(in);
		else if (t == details::ContainerType::MEDIUM)
		{
			toMedium();
			medium->read(in);
		}
		else if (t == details::ContainerType::LARGE)
		{
			toLarge();
			large->read(in);
		}
		else
			throw Poco::Exception("Internal error", ErrorCodes::LOGICAL_ERROR);
	}

	void readAndMerge(DB::ReadBuffer & in)
	{
		Self other;
		other.read(in);
		merge(other);
	}

	void write(DB::WriteBuffer & out) const
	{
		UInt8 v = static_cast<UInt8>(container_type);
		writeBinary(v, out);

		if (container_type == details::ContainerType::SMALL)
			small.write(out);
		else if (container_type == details::ContainerType::MEDIUM)
			medium->write(out);
		else if (container_type == details::ContainerType::LARGE)
			large->write(out);
		else
			throw Poco::Exception("Internal error", ErrorCodes::LOGICAL_ERROR);
	}

	bool isMedium() const
	{
		return container_type == details::ContainerType::MEDIUM;
	}

private:
	void toMedium()
	{
		if (container_type != details::ContainerType::SMALL)
			throw Poco::Exception("Internal error", ErrorCodes::LOGICAL_ERROR);

		if (current_memory_tracker)
			current_memory_tracker->alloc(sizeof(medium));

		Medium * tmp_medium = new Medium;

		for (const auto & x : small)
			tmp_medium->insert(x);

		medium = tmp_medium;

		container_type = details::ContainerType::MEDIUM;
	}

	void toLarge()
	{
		if ((container_type != details::ContainerType::SMALL) && (container_type != details::ContainerType::MEDIUM))
			throw Poco::Exception("Internal error", ErrorCodes::LOGICAL_ERROR);

		if (current_memory_tracker)
			current_memory_tracker->alloc(sizeof(large));

		Large * tmp_large = new Large;

		if (container_type == details::ContainerType::SMALL)
		{
			for (const auto & x : small)
				tmp_large->insert(x);
		}
		else if (container_type == details::ContainerType::MEDIUM)
		{
			for (const auto & x : *medium)
				tmp_large->insert(x);
		}

		large = tmp_large;

		if (container_type == details::ContainerType::MEDIUM)
		{
			delete medium;
			medium = nullptr;

			if (current_memory_tracker)
				current_memory_tracker->free(sizeof(medium));
		}

		container_type = details::ContainerType::LARGE;
	}

private:
	Small small;
	Medium * medium = nullptr;
	Large * large = nullptr;
	const UInt32 medium_set_size_max = 1UL << medium_set_power2_max;
	details::ContainerType container_type = details::ContainerType::SMALL;
};

}
