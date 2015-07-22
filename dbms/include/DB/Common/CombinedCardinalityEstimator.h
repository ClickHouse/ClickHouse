#pragma once

#include <DB/Common/HashTable/SmallTable.h>
#include <DB/Common/HashTable/HashSet.h>
#include <DB/Common/HyperLogLogWithSmallSetOptimization.h>
#include <DB/Core/Defines.h>


namespace DB
{

namespace details
{

enum class ContainerType : UInt8 { SMALL = 1, MEDIUM = 2, LARGE = 3 };

static inline ContainerType max(const ContainerType & lhs, const ContainerType & rhs)
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
	CombinedCardinalityEstimator()
	{
		setContainerType(details::ContainerType::SMALL);
	}

	~CombinedCardinalityEstimator()
	{
		destroy();
	}

	void insert(Key value)
	{
		auto container_type = getContainerType();

		if (container_type == details::ContainerType::SMALL)
		{
			if (small.find(value) == small.end())
			{
				if (!small.full())
					small.insert(value);
				else
				{
					toMedium();
					getObject<Medium>()->insert(value);
				}
			}
		}
		else if (container_type == details::ContainerType::MEDIUM)
		{
			if (getObject<Medium>()->size() < medium_set_size_max)
				getObject<Medium>()->insert(value);
			else
			{
				toLarge();
				getObject<Large>()->insert(value);
			}
		}
		else if (container_type == details::ContainerType::LARGE)
			getObject<Large>()->insert(value);
		else
			throw Poco::Exception("Internal error", ErrorCodes::LOGICAL_ERROR);
	}

	UInt32 size() const
	{
		auto container_type = getContainerType();

		if (container_type == details::ContainerType::SMALL)
			return small.size();
		else if (container_type == details::ContainerType::MEDIUM)
			return getObject<Medium>()->size();
		else if (container_type == details::ContainerType::LARGE)
			return getObject<Large>()->size();
		else
			throw Poco::Exception("Internal error", ErrorCodes::LOGICAL_ERROR);
	}

	void merge(const Self & rhs)
	{
		auto container_type = getContainerType();
		auto max_container_type = details::max(container_type, rhs.getContainerType());

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
			if (rhs.getContainerType() == details::ContainerType::SMALL)
			{
				for (const auto & x : rhs.small)
					insert(x);
			}
			else if (rhs.getContainerType() == details::ContainerType::MEDIUM)
			{
				for (const auto & x : *rhs.getObject<Medium>())
					insert(x);
			}
		}
		else if (container_type == details::ContainerType::LARGE)
		{
			if (rhs.getContainerType() == details::ContainerType::SMALL)
			{
				for (const auto & x : rhs.small)
					insert(x);
			}
			else if (rhs.getContainerType() == details::ContainerType::MEDIUM)
			{
				for (const auto & x : *rhs.getObject<Medium>())
					insert(x);
			}
			else if (rhs.getContainerType() == details::ContainerType::LARGE)
				getObject<Large>()->merge(*rhs.getObject<Large>());
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
			getObject<Medium>()->read(in);
		}
		else if (t == details::ContainerType::LARGE)
		{
			toLarge();
			getObject<Large>()->read(in);
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
		auto container_type = getContainerType();

		UInt8 v = static_cast<UInt8>(container_type);
		writeBinary(v, out);

		if (container_type == details::ContainerType::SMALL)
			small.write(out);
		else if (container_type == details::ContainerType::MEDIUM)
			getObject<Medium>()->write(out);
		else if (container_type == details::ContainerType::LARGE)
			getObject<Large>()->write(out);
		else
			throw Poco::Exception("Internal error", ErrorCodes::LOGICAL_ERROR);
	}

	bool isMedium() const
	{
		return getContainerType() == details::ContainerType::MEDIUM;
	}

private:
	void toMedium()
	{
		if (getContainerType() != details::ContainerType::SMALL)
			throw Poco::Exception("Internal error", ErrorCodes::LOGICAL_ERROR);

		auto tmp_medium = std::make_unique<Medium>();

		for (const auto & x : small)
			tmp_medium->insert(x);

		new (&medium) std::unique_ptr<Medium>{ std::move(tmp_medium) };

		std::atomic_signal_fence(std::memory_order_seq_cst);

		setContainerType(details::ContainerType::MEDIUM);

		std::atomic_signal_fence(std::memory_order_seq_cst);

		if (current_memory_tracker)
			current_memory_tracker->alloc(sizeof(medium));
	}

	void toLarge()
	{
		auto container_type = getContainerType();

		if ((container_type != details::ContainerType::SMALL) && (container_type != details::ContainerType::MEDIUM))
			throw Poco::Exception("Internal error", ErrorCodes::LOGICAL_ERROR);

		auto tmp_large = std::make_unique<Large>();

		if (container_type == details::ContainerType::SMALL)
		{
			for (const auto & x : small)
				tmp_large->insert(x);
		}
		else if (container_type == details::ContainerType::MEDIUM)
		{
			for (const auto & x : *getObject<Medium>())
				tmp_large->insert(x);

			destroy();
		}

		new (&large) std::unique_ptr<Large>{ std::move(tmp_large) };

		std::atomic_signal_fence(std::memory_order_seq_cst);

		setContainerType(details::ContainerType::LARGE);

		std::atomic_signal_fence(std::memory_order_seq_cst);

		if (current_memory_tracker)
			current_memory_tracker->alloc(sizeof(large));

	}

	void NO_INLINE destroy()
	{
		auto container_type = getContainerType();

		clearContainerType();

		if (container_type == details::ContainerType::MEDIUM)
		{
			medium.std::unique_ptr<Medium>::~unique_ptr();
			if (current_memory_tracker)
				current_memory_tracker->free(sizeof(medium));
		}
		else if (container_type == details::ContainerType::LARGE)
		{
			large.std::unique_ptr<Large>::~unique_ptr();
			if (current_memory_tracker)
				current_memory_tracker->free(sizeof(large));
		}
	}

	template<typename T>
	T * getObject()
	{
		return reinterpret_cast<T *>(address & mask);
	}

	template<typename T>
	const T * getObject() const
	{
		return reinterpret_cast<T *>(address & mask);
	}

	void setContainerType(details::ContainerType t)
	{
		address |= static_cast<UInt8>(t);
	}

	details::ContainerType getContainerType() const
	{
		return static_cast<details::ContainerType>(address & ~mask);
	}

	void clearContainerType()
	{
		address &= mask;
	}

private:
	Small small;
	union
	{
		std::unique_ptr<Medium> medium;
		std::unique_ptr<Large> large;
		UInt64 address = 0;
	};
	static const UInt64 mask = 0xFFFFFFFC;
	static const UInt32 medium_set_size_max = 1UL << medium_set_power2_max;
};

}
