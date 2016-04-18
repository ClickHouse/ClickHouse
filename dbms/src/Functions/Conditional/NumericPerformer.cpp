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
template <typename TType, typename TResult>
struct ElsePredicate final : public PredicateBase<TType>
{
	using Base = PredicateBase<TType>;
	using TCombinedResult = typename NumberTraits::ResultOfIf<TResult, TType>::Type;

	static bool execute(size_t index, Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		auto category = Base::appendBranchInfo(index, block, args, branches);

		/// We have collected all the information we need.
		/// Now perform the multiIf.

		if (category == Base::NONE)
			return false;
		else if (category == Base::NUMERIC)
			NumericEvaluator<TCombinedResult>::perform(branches, block, args, result);
		else if (category == Base::NUMERIC_ARRAY)
			ArrayEvaluator<TCombinedResult>::perform(branches, block, args, result);
		else
			throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};

		return true;
	}
};

/// Process a multiIf function if its else branch carries incorrect type information.
template <typename TType>
struct ElsePredicate<TType, NumberTraits::Error>
{
	static bool execute(size_t index, Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Collect type information on a then branch of a multiIf function.
/// Update the returned value type information. Go to the next branch.
template <typename TType, typename TResult>
struct ThenPredicate final : public PredicateBase<TType>
{
	using Base = PredicateBase<TType>;
	using TCombinedResult = typename NumberTraits::ResultOfIf<TResult, TType>::Type;

	template <typename U>
	using ConcreteThenPredicate = ThenPredicate<U, TCombinedResult>;

	template <typename U>
	using ConcreteElsePredicate = ElsePredicate<U, TCombinedResult>;

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

/// Process a multiIf function if one of its then branches carries
/// incorrect type information.
template <typename TType>
struct ThenPredicate<TType, NumberTraits::Error>
{
	static bool execute(size_t index, Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// First Then
template <typename TType>
struct FirstThenPredicate final
{
	static bool execute(Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		return ThenPredicate<TType, TType>::execute(firstThen(), block, args, result, branches);
	}
};

}

bool NumericPerformer::perform(Block & block, const ColumnNumbers & args,
	size_t result)
{
	Branches branches;
	return NumericTypeDispatcher<FirstThenPredicate>::apply(block, args, result, branches);
}

}

}
