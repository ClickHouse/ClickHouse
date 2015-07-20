#pragma once

#include <DB/Common/HashTable/SmallTable.h>
#include <DB/Common/HashTable/HashSet.h>
#include <DB/Common/HyperLogLogWithSmallSetOptimization.h>

namespace DB
{

template <typename Key, typename HashType, UInt8 small_set_size, UInt8 medium_set_power, UInt8 K>
class CombinedCardinalityEstimator
{
public:
	using Self = CombinedCardinalityEstimator<Key, HashType, small_set_size, medium_set_power, K>;

private:
	using Small = SmallSet<Key, small_set_size>;
	using Medium = HashType;
	using Large = HyperLogLogWithSmallSetOptimization<Key, small_set_size, K>;
	enum class ContainerType { SMALL, MEDIUM, LARGE };

public:
	~CombinedCardinalityEstimator()
	{
		if (container_type == ContainerType::MEDIUM)
		{
			delete medium;

			if (current_memory_tracker)
				current_memory_tracker->free(sizeof(medium));
		}
		else if (container_type == ContainerType::LARGE)
		{
			delete large;

			if (current_memory_tracker)
				current_memory_tracker->free(sizeof(large));
		}
	}

	void insert(Key value)
	{
		if (container_type == ContainerType::SMALL)
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
		else if (container_type == ContainerType::MEDIUM)
		{
			if (medium->size() < medium_set_size)
				medium->insert(value);
			else
			{
				toLarge();
				large->insert(value);
			}
		}
		else if (container_type == ContainerType::LARGE)
			large->insert(value);
	}

	UInt32 size() const
	{
		if (container_type == ContainerType::SMALL)
			return small.size();
		else if (container_type == ContainerType::MEDIUM)
			return medium->size();
		else if (container_type == ContainerType::LARGE)
			return large->size();

		return 0;
	}

	void merge(const Self & rhs)
	{
		ContainerType res = max(container_type, rhs.container_type);

		if (container_type != res)
		{
			if (res == ContainerType::MEDIUM)
				toMedium();
			else if (res == ContainerType::LARGE)
				toLarge();
		}

		if (container_type == ContainerType::SMALL)
		{
			for (const auto & x : rhs.small)
				insert(x);
		}
		else if (container_type == ContainerType::MEDIUM)
		{
			if (rhs.container_type == ContainerType::SMALL)
			{
				for (const auto & x : rhs.small)
					insert(x);
			}
			else if (rhs.container_type == ContainerType::MEDIUM)
			{
				for (const auto & x : *rhs.medium)
					insert(x);
			}
		}
		else if (container_type == ContainerType::LARGE)
		{
			if (rhs.container_type == ContainerType::SMALL)
			{
				for (const auto & x : rhs.small)
					insert(x);
			}
			else if (rhs.container_type == ContainerType::MEDIUM)
			{
				for (const auto & x : *rhs.medium)
					insert(x);
			}
			else if (rhs.container_type == ContainerType::LARGE)
				large->merge(*rhs.large);
		}
	}

	void read(DB::ReadBuffer & in)
	{
		UInt8 v;
		readBinary(v, in);
		ContainerType t = static_cast<ContainerType>(v);

		if (t == ContainerType::SMALL)
			small.read(in);
		else if (t == ContainerType::MEDIUM)
		{
			toMedium();
			medium->read(in);
		}
		else if (t == ContainerType::LARGE)
		{
			toLarge();
			large->read(in);
		}
	}

	void readAndMerge(DB::ReadBuffer & in)
	{
		Self other;
		other.read(in);
		merge(other);
	}

	void write(DB::WriteBuffer & out) const
	{
		UInt8 v = static_cast<unsigned int>(container_type);
		writeBinary(v, out);

		if (container_type == ContainerType::SMALL)
			small.write(out);
		else if (container_type == ContainerType::MEDIUM)
			medium->write(out);
		else if (container_type == ContainerType::LARGE)
			large->write(out);
	}

	bool isMedium() const
	{
		return container_type == ContainerType::MEDIUM;
	}

private:
	void toMedium()
	{
		if (current_memory_tracker)
			current_memory_tracker->alloc(sizeof(medium));

		Medium * tmp_medium = new Medium;

		for (const auto & x : small)
			tmp_medium->insert(x);

		medium = tmp_medium;

		container_type = ContainerType::MEDIUM;
	}

	void toLarge()
	{
		if (current_memory_tracker)
			current_memory_tracker->alloc(sizeof(large));

		Large * tmp_large = new Large;

		for (const auto & x : *medium)
			tmp_large->insert(x);

		large = tmp_large;

		delete medium;
		medium = nullptr;

		if (current_memory_tracker)
			current_memory_tracker->free(sizeof(medium));

		container_type = ContainerType::LARGE;
	}

	ContainerType max(const ContainerType & lhs, const ContainerType & rhs)
	{
		unsigned int res = std::max(static_cast<unsigned int>(lhs), static_cast<unsigned int>(rhs));
		return static_cast<ContainerType>(res);
	}

private:
	ContainerType container_type = ContainerType::SMALL;
	const UInt32 medium_set_size = 1UL << medium_set_power;
	Small small;
	Medium * medium = nullptr;
	Large * large = nullptr;
};

}
