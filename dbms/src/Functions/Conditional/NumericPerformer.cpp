#include <DB/Functions/Conditional/NumericPerformer.h>
#include <DB/Functions/Conditional/ArgsInfo.h>
#include <DB/Functions/Conditional/NumericEvaluator.h>
#include <DB/Functions/Conditional/ArrayEvaluator.h>
#include <DB/Functions/NumberTraits.h>
#include <DB/Functions/DataTypeFromFieldTypeOrError.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnArray.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;

}

namespace Conditional
{

namespace
{

/// Forward declarations.
template <typename TFloat, typename TUInt, typename TInt>
struct ElseFunctor;

template <typename TType, typename TInt>
struct ElseFunctorImpl;

/// Perform the multiIf function for either numeric or array branch parameters,
/// after we successfully processed the Else branch.
template <typename TFloat, typename TUInt, typename TInt>
struct ElseFunctor final
{
private:
	using TCombined = typename NumberTraits::ResultOfIf<TFloat, TUInt>::Type;

public:
	static void executeNumeric(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result)
	{
		ElseFunctorImpl<TCombined, TInt>::executeNumeric(branches, block, args, result);
	}

	static void executeArray(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result)
	{
		ElseFunctorImpl<TCombined, TInt>::executeArray(branches, block, args, result);
	}
};

template <typename TType, typename TInt>
struct ElseFunctorImpl final
{
private:
	using TCombined = typename NumberTraits::ResultOfIf<TType, TInt>::Type;

public:
	static void executeNumeric(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result)
	{
		NumericEvaluator<TCombined>::perform(branches, block, args, result);
	}

	static void executeArray(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result)
	{
		ArrayEvaluator<TCombined>::perform(branches, block, args, result);
	}
};

/// Specialization for type composition error.
template <typename TInt>
class ElseFunctorImpl<typename NumberTraits::Error, TInt> final
{
public:
	static void executeNumeric(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}

	static void executeArray(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Specialization for type composition error.
template <typename TType>
class ElseFunctorImpl<TType, typename NumberTraits::Error> final
{
public:
	static void executeNumeric(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}

	static void executeArray(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Specialization for type composition error.
template <>
class ElseFunctorImpl<typename NumberTraits::Error, typename NumberTraits::Error> final
{
public:
	static void executeNumeric(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}

	static void executeArray(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// This class provides a means to collect type information on a branch
/// (then or else) of a multiIf function.
template <typename TType>
struct PredicateBase
{
protected:
	enum Category
	{
		NONE = 0,
		NUMERIC,
		NUMERIC_ARRAY
	};

protected:
	static Category appendBranchInfo(size_t index, const Block & block,
		const ColumnNumbers & args, Branches & branches)
	{
		const IColumn * col = block.getByPosition(args[index]).column.get();

		const ColumnVector<TType> * vec_col = nullptr;
		const ColumnConst<TType> * const_col = nullptr;

		const ColumnArray * arr_col = nullptr;
		const ColumnVector<TType> * arr_vec_col = nullptr;
		const ColumnConstArray * arr_const_col = nullptr;

		Branch branch;

		vec_col = typeid_cast<const ColumnVector<TType> *>(col);
		if (vec_col != nullptr)
			branch.is_const = false;
		else
		{
			const_col = typeid_cast<const ColumnConst<TType> *>(col);
			if (const_col != nullptr)
				branch.is_const = true;
			else
			{
				arr_col = typeid_cast<const ColumnArray *>(col);
				if (arr_col != nullptr)
				{
					arr_vec_col = typeid_cast<const ColumnVector<TType> *>(&arr_col->getData());
					if (arr_vec_col != nullptr)
						branch.is_const = false;
					else
						return NONE;
				}
				else
				{
					arr_const_col = typeid_cast<const ColumnConstArray *>(col);
					if (arr_const_col != nullptr)
					{
						const IDataType * data = arr_const_col->getDataType().get();
						const DataTypeArray * arr = typeid_cast<const DataTypeArray *>(data);
						if (arr == nullptr)
							throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};

						const IDataType * nested_type = arr->getNestedType().get();

						using ElementType = typename DataTypeFromFieldType<TType>::Type;

						if (typeid_cast<const ElementType *>(nested_type) == nullptr)
							return NONE;

						branch.is_const = true;
					}
					else
						return NONE;
				}
			}
		}

		branch.index = index;
		branch.type = DataTypeFromFieldTypeOrError<TType>::getDataType();

		branches.push_back(branch);

		if ((vec_col != nullptr) || (const_col != nullptr))
			return NUMERIC;
		else if ((arr_vec_col != nullptr) || (arr_const_col != nullptr))
			return NUMERIC_ARRAY;
		else
			throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
	}
};

/// Collect type information on the unique else branch of a multiIf function.
/// Determine the returned value type. Perform the multiIf.
template <typename TFloat, typename TUInt, typename TInt, typename TType>
struct ElsePredicate final : public PredicateBase<TType>
{
	using Base = PredicateBase<TType>;

	using TCombinedFloat = typename std::conditional<
		std::is_floating_point<TType>::value,
		typename NumberTraits::ResultOfIf<TFloat, TType>::Type,
		TFloat
	>::type;

	using TCombinedUInt = typename std::conditional<
		std::is_unsigned<TType>::value,
		typename NumberTraits::ResultOfIf<TUInt, TType>::Type,
		TUInt
	>::type;

	using TCombinedInt = typename std::conditional<
		std::is_signed<TType>::value,
		typename NumberTraits::ResultOfIf<TInt, TType>::Type,
		TInt
	>::type;

	static bool execute(size_t index, Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		auto category = Base::appendBranchInfo(index, block, args, branches);

		/// We have collected all the information we need.
		/// Now perform the multiIf.
		if (category == Base::NONE)
			return false;
		else if (category == Base::NUMERIC)
			ElseFunctor<TCombinedFloat, TCombinedUInt, TCombinedInt>::executeNumeric(branches, block, args, result);
		else if (category == Base::NUMERIC_ARRAY)
			ElseFunctor<TCombinedFloat, TCombinedUInt, TCombinedInt>::executeArray(branches, block, args, result);
		else
			throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};

		return true;
	}
};

/// Specialization for incorrect type information.
template <typename TFloat, typename TUInt, typename TInt>
struct ElsePredicate<TFloat, TUInt, TInt, NumberTraits::Error>
{
	static bool execute(size_t index, Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};


/// Specialization for incorrect type information.
template <typename TFloat, typename TUInt, typename TType>
struct ElsePredicate<TFloat, TUInt, NumberTraits::Error, TType>
{
	static bool execute(size_t index, Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Specialization for incorrect type information.
template <typename TFloat, typename TInt, typename TType>
struct ElsePredicate<TFloat, NumberTraits::Error, TInt, TType>
{
	static bool execute(size_t index, Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Specialization for incorrect type information.
template <typename TUInt, typename TInt, typename TType>
struct ElsePredicate<NumberTraits::Error, TUInt, TInt, TType>
{
	static bool execute(size_t index, Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Collect type information on a then branch of a multiIf function.
/// Update the returned value type information. Go to the next branch.
template <typename TFloat, typename TUInt, typename TInt, typename TType>
struct ThenPredicate final : public PredicateBase<TType>
{
	using Base = PredicateBase<TType>;

	using TCombinedFloat = typename std::conditional<
		std::is_floating_point<TType>::value,
		typename NumberTraits::ResultOfIf<TFloat, TType>::Type,
		TFloat
	>::type;

	using TCombinedUInt = typename std::conditional<
		std::is_unsigned<TType>::value,
		typename NumberTraits::ResultOfIf<TUInt, TType>::Type,
		TUInt
	>::type;

	using TCombinedInt = typename std::conditional<
		std::is_signed<TType>::value,
		typename NumberTraits::ResultOfIf<TInt, TType>::Type,
		TInt
	>::type;

	template <typename U>
	using ConcreteThenPredicate = ThenPredicate<TCombinedFloat, TCombinedUInt, TCombinedInt, U>;

	template <typename U>
	using ConcreteElsePredicate = ElsePredicate<TCombinedFloat, TCombinedUInt, TCombinedInt, U>;

	static bool execute(size_t index, Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		auto category = Base::appendBranchInfo(index, block, args, branches);

		if (category == Base::NONE)
			return false;

		/// Guess what comes after Then.
		size_t index2 = index + 1;

		if (index2 != elseArg(args))
		{
			/// We have a pair Cond-Then. Process the next Then.
			if (!NumericTypeDispatcher<ConcreteThenPredicate>::apply(index2 + 1,
				block, args, result, branches))
				throw Exception{"Illegal column " + toString(index2 + 1) +
					" of function multiIf", ErrorCodes::ILLEGAL_COLUMN};
		}
		else
		{
			/// We have an Else which ends the multiIf. Process it.
			if (!NumericTypeDispatcher<ConcreteElsePredicate>::apply(index2,
				block, args, result, branches))
				throw Exception{"Illegal column " + toString(index2) +
					" of function multiIf", ErrorCodes::ILLEGAL_COLUMN};
		}

		return true;
	}
};

/// Specialization for incorrect type information.
template <typename TFloat, typename TUInt, typename TInt>
struct ThenPredicate<TFloat, TUInt, TInt, NumberTraits::Error>
{
	static bool execute(size_t index, Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};


/// Specialization for incorrect type information.
template <typename TFloat, typename TUInt, typename TType>
struct ThenPredicate<TFloat, TUInt, NumberTraits::Error, TType>
{
	static bool execute(size_t index, Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Specialization for incorrect type information.
template <typename TFloat, typename TInt, typename TType>
struct ThenPredicate<TFloat, NumberTraits::Error, TInt, TType>
{
	static bool execute(size_t index, Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Specialization for incorrect type information.
template <typename TUInt, typename TInt, typename TType>
struct ThenPredicate<NumberTraits::Error, TUInt, TInt, TType>
{
	static bool execute(size_t index, Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// First Then
struct FirstThenPredicate final
{
	template <typename U>
	using ConcreteThenPredicate = ThenPredicate<void, void, void, U>;

	static bool execute(Block & block, const ColumnNumbers & args, size_t result)
	{
		Branches branches;
		return NumericTypeDispatcher<ConcreteThenPredicate>::apply(firstThen(), block, args, result, branches);
	}
};

}

bool NumericPerformer::perform(Block & block, const ColumnNumbers & args,
	size_t result)
{
	return FirstThenPredicate::execute(block, args, result);
}

}

}
