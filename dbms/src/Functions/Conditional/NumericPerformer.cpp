#include <DB/Functions/Conditional/NumericPerformer.h>
#include <DB/Functions/Conditional/CondException.h>
#include <DB/Functions/Conditional/ArgsInfo.h>
#include <DB/Functions/Conditional/NumericEvaluator.h>
#include <DB/Functions/Conditional/ArrayEvaluator.h>
#include <DB/Functions/NumberTraits.h>
#include <DB/Functions/DataTypeTraits.h>
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
		branch.type = DataTypeTraits::DataTypeFromFieldTypeOrError<TType>::getDataType();

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
template <typename TResult, typename TType>
struct ElsePredicate final : public PredicateBase<TType>
{
	using Base = PredicateBase<TType>;

	using TCombined = typename NumberTraits::TypeProduct<
		TResult,
		typename NumberTraits::EmbedType<TType>::Type
	>::Type;

	using TFinal = typename NumberTraits::ToOrdinaryType<TCombined>::Type;

	static bool execute(size_t index, Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		auto category = Base::appendBranchInfo(index, block, args, branches);

		/// We have collected all the information we need.
		/// Now perform the multiIf.
		if (category == Base::NONE)
			return false;
		else if (category == Base::NUMERIC)
			NumericEvaluator<TFinal>::perform(branches, block, args, result);
		else if (category == Base::NUMERIC_ARRAY)
			ArrayEvaluator<TFinal>::perform(branches, block, args, result);
		else
			throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};

		return true;
	}
};

/// Specialization for incorrect type information.
template <typename TResult>
struct ElsePredicate<TResult, NumberTraits::Error>
{
	static bool execute(size_t index, Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Specialization for incorrect type information.
template <typename TType>
struct ElsePredicate<NumberTraits::Error, TType>
{
	static bool execute(size_t index, Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Collect type information on a then branch of a multiIf function.
/// Update the returned value type information. Go to the next branch.
template <typename TResult, typename TType>
struct ThenPredicate final : public PredicateBase<TType>
{
	using Base = PredicateBase<TType>;

	using TCombined = typename NumberTraits::TypeProduct<
		TResult,
		typename NumberTraits::EmbedType<TType>::Type
	>::Type;

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
			if (! (ThenPredicate<TCombined, UInt8>::execute(index2 + 1, block, args, result, branches)
				|| ThenPredicate<TCombined, UInt16>::execute(index2 + 1, block, args, result, branches)
				|| ThenPredicate<TCombined, UInt32>::execute(index2 + 1, block, args, result, branches)
				|| ThenPredicate<TCombined, UInt64>::execute(index2 + 1, block, args, result, branches)
				|| ThenPredicate<TCombined, Int8>::execute(index2 + 1, block, args, result, branches)
				|| ThenPredicate<TCombined, Int16>::execute(index2 + 1, block, args, result, branches)
				|| ThenPredicate<TCombined, Int32>::execute(index2 + 1, block, args, result, branches)
				|| ThenPredicate<TCombined, Int64>::execute(index2 + 1, block, args, result, branches)
				|| ThenPredicate<TCombined, Float32>::execute(index2 + 1, block, args, result, branches)
				|| ThenPredicate<TCombined, Float64>::execute(index2 + 1, block, args, result, branches)))
				throw CondException{CondErrorCodes::NUMERIC_PERFORMER_ILLEGAL_COLUMN,
					toString(index2 + 1)};
		}
		else
		{
			/// We have an Else which ends the multiIf. Process it.
			if (! (ElsePredicate<TCombined, UInt8>::execute(index2, block, args, result, branches)
				|| ElsePredicate<TCombined, UInt16>::execute(index2, block, args, result, branches)
				|| ElsePredicate<TCombined, UInt32>::execute(index2, block, args, result, branches)
				|| ElsePredicate<TCombined, UInt64>::execute(index2, block, args, result, branches)
				|| ElsePredicate<TCombined, Int8>::execute(index2, block, args, result, branches)
				|| ElsePredicate<TCombined, Int16>::execute(index2, block, args, result, branches)
				|| ElsePredicate<TCombined, Int32>::execute(index2, block, args, result, branches)
				|| ElsePredicate<TCombined, Int64>::execute(index2, block, args, result, branches)
				|| ElsePredicate<TCombined, Float32>::execute(index2, block, args, result, branches)
				|| ElsePredicate<TCombined, Float64>::execute(index2, block, args, result, branches)))
				throw CondException{CondErrorCodes::NUMERIC_PERFORMER_ILLEGAL_COLUMN,
					toString(index2)};
		}

		return true;
	}
};

/// Specialization for incorrect type information.
template <typename TResult>
struct ThenPredicate<TResult, NumberTraits::Error>
{
	static bool execute(size_t index, Block & block, const ColumnNumbers & args,
		size_t result, Branches & branches)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Specialization for incorrect type information.
template <typename TType>
struct ThenPredicate<NumberTraits::Error, TType>
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
	static bool execute(Block & block, const ColumnNumbers & args, size_t result)
	{
		using Void = NumberTraits::Enriched::Void;
		Branches branches;

		return ThenPredicate<Void, UInt8>::execute(firstThen(), block, args, result, branches)
			|| ThenPredicate<Void, UInt16>::execute(firstThen(), block, args, result, branches)
			|| ThenPredicate<Void, UInt32>::execute(firstThen(), block, args, result, branches)
			|| ThenPredicate<Void, UInt64>::execute(firstThen(), block, args, result, branches)
			|| ThenPredicate<Void, Int8>::execute(firstThen(), block, args, result, branches)
			|| ThenPredicate<Void, Int16>::execute(firstThen(), block, args, result, branches)
			|| ThenPredicate<Void, Int32>::execute(firstThen(), block, args, result, branches)
			|| ThenPredicate<Void, Int64>::execute(firstThen(), block, args, result, branches)
			|| ThenPredicate<Void, Float32>::execute(firstThen(), block, args, result, branches)
			|| ThenPredicate<Void, Float64>::execute(firstThen(), block, args, result, branches);
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
