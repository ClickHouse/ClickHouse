#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>

namespace DB
{

namespace Conditional
{

/// Execute a given parametrized predicate for each type from a given list of
/// types until it returns true for one of these types.
template <template <typename> class Predicate, typename TType, typename... RTypeList>
struct Disjunction final
{
	template <typename... Args>
	static bool apply(Args &&... args)
	{
		return Predicate<TType>::execute(std::forward<Args>(args)...)
			|| Disjunction<Predicate, RTypeList...>::apply(std::forward<Args>(args)...);
	}
};

template <template <typename> class Predicate, typename TType>
struct Disjunction<Predicate, TType>
{
	template <typename... Args>
	static bool apply(Args &&... args)
	{
		return Predicate<TType>::execute(std::forward<Args>(args)...);
	}
};

/// Common code for NumericTypeDispatcher and DataTypeDispatcher.
/// See comments below.
template <template <typename> class Predicate, bool isNumeric>
struct TypeDispatcher final
{
private:
	template <typename TType, bool isNumeric2>
	struct ActualType;

	template <typename TType>
	struct ActualType<TType, true>
	{
		using Type = TType;
	};

	template <typename TType>
	struct ActualType<TType, false>
	{
		using Type = typename DataTypeFromFieldType<TType>::Type;
	};

public:
	template <typename... Args>
	static bool apply(Args&&... args)
	{
		return Disjunction<
			Predicate,
			typename ActualType<UInt8, isNumeric>::Type,
			typename ActualType<UInt16, isNumeric>::Type,
			typename ActualType<UInt32, isNumeric>::Type,
			typename ActualType<UInt64, isNumeric>::Type,
			typename ActualType<Int8, isNumeric>::Type,
			typename ActualType<Int16, isNumeric>::Type,
			typename ActualType<Int32, isNumeric>::Type,
			typename ActualType<Int64, isNumeric>::Type,
			typename ActualType<Float32, isNumeric>::Type,
			typename ActualType<Float64, isNumeric>::Type
		>::apply(std::forward<Args>(args)...);
	}
};

/// Execute a given parametrized predicate for each numeric type
/// until it returns true for such a numeric type.
template <template <typename> class Predicate>
using NumericTypeDispatcher = TypeDispatcher<Predicate, true>;

/// Execute a given parametrized predicate for each data type
/// until it returns true for such a data type.
template <template <typename> class Predicate>
using DataTypeDispatcher = TypeDispatcher<Predicate, false>;

/// When performing a multiIf for numeric arguments, the following
/// structure is used to collect all the information needed on
/// the branches (1 or more then branches + 1 else branch) for
/// processing.
struct Branch
{
	size_t index;
	DataTypePtr type;
	bool is_const;
};

using Branches = std::vector<Branch>;

/// The following functions are designed to make the code that handles
/// multiIf parameters a tad more readable and less prone to off-by-one
/// errors.
template <typename T>
inline bool hasValidArgCount(const T & args)
{
	return (args.size() >= 3) && ((args.size() % 2) == 1);
}

inline constexpr size_t firstCond()
{
	return 0;
}

inline constexpr size_t firstThen()
{
	return 1;
}

inline constexpr size_t secondThen()
{
	return 3;
}

/// NOTE: we always use this function after the number of arguments of multiIf
/// has been validated. Therefore there is no need to check for zero size.
template <typename T>
inline size_t elseArg(const T & args)
{
	return args.size() - 1;
}

inline size_t thenFromCond(size_t i)
{
	return i + 1;
}

inline size_t nextCond(size_t i)
{
	return i + 2;
}

inline size_t nextThen(size_t i)
{
	return i + 2;
}

inline bool isCond(size_t i)
{
	return (i % 2) == 0;
}

template <typename T>
inline size_t getCondCount(const T & args)
{
	return args.size() / 2;
}

template <typename T>
inline size_t getBranchCount(const T & args)
{
	return (args.size() / 2) + 1;
}

}

}
