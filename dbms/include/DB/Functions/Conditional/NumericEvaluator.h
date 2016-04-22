#pragma once

#include <DB/Functions/Conditional/common.h>
#include <DB/Functions/Conditional/CondSource.h>
#include <DB/Functions/NumberTraits.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/ColumnConst.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;

}

namespace Conditional
{

namespace
{

/// This class provides type-independent access to the values of a numeric branch
/// (then, else) column. Returned values have the type TResult.
template <typename TResult>
class NumericSource
{
public:
	virtual TResult get(size_t i) const = 0;
	virtual ~NumericSource() = default;
};

template <typename TResult>
using NumericSourcePtr = std::unique_ptr<NumericSource<TResult> >;

template <typename TResult>
using NumericSources = std::vector<NumericSourcePtr<TResult> >;

/// Column type-specific implementation of NumericSource.
template <typename TResult, typename TType>
class NumericSourceImpl final : public NumericSource<TResult>
{
public:
	NumericSourceImpl(const Block & block, const ColumnNumbers & args, const Branch & br)
		: data_array{initDataArray(block, args, br)}
	{
		size_t index = br.index;
		bool is_const = br.is_const;

		if (is_const)
		{
			const ColumnPtr & col = block.getByPosition(args[index]).column;
			const auto * const_col = typeid_cast<const ColumnConst<TType> *>(&*col);
			if (const_col == nullptr)
				throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
			TType data = const_col->getData();
			accessor = [data](size_t i) { return static_cast<TResult>(data); };
		}
		else
			accessor = [&](size_t i) { return static_cast<TResult>(data_array[i]); };
	}

	NumericSourceImpl(const NumericSourceImpl &) = delete;
	NumericSourceImpl & operator=(const NumericSourceImpl &) = delete;

	NumericSourceImpl(NumericSourceImpl &&) = default;
	NumericSourceImpl & operator=(NumericSourceImpl &&) = default;

	TResult get(size_t i) const override
	{
		return accessor(i);
	};

private:
	static const PaddedPODArray<TType> & initDataArray(const Block & block,
		const ColumnNumbers & args, const Branch & br)
	{
		bool is_const = br.is_const;

		if (is_const)
			return null_array;
		else
		{
			size_t index = br.index;
			const ColumnPtr & col = block.getByPosition(args[index]).column;
			const auto * vec_col = typeid_cast<const ColumnVector<TType> *>(&*col);
			if (vec_col == nullptr)
				throw Exception{"Internal error", ErrorCodes::LOGICAL_ERROR};
			return vec_col->getData();
		}
	}

private:
	static const PaddedPODArray<TType> null_array;

	const PaddedPODArray<TType> & data_array;

	using Accessor = std::function<TResult(size_t)>;
	Accessor accessor;
};

template <typename TResult, typename TType>
const PaddedPODArray<TType> NumericSourceImpl<TResult, TType>::null_array{};

/// Create a numeric column accessor if TType is the type registered
/// in the specified branch info.
template <typename TResult, typename TType>
class NumericSourceCreator final
{
public:
	static bool execute(NumericSourcePtr<TResult> & source, const Block & block,
		const ColumnNumbers & args, const Branch & br)
	{
		auto type_name = br.type->getName();
		if (TypeName<TType>::get() == type_name)
		{
			source = std::make_unique<NumericSourceImpl<TResult, TType> >(block, args, br);
			return true;
		}
		else
			return false;
	}
};

}

/// Processing of multiIf in the case of scalar numeric types.
template <typename TResult>
class NumericEvaluator final
{
public:
	static void perform(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result)
	{
		const CondSources conds = createConds(block, args);
		const NumericSources<TResult> sources = createNumericSources(block, args, branches);
		size_t row_count = conds[0].getSize();
		PaddedPODArray<TResult> & res = createSink(block, result, row_count);

		for (size_t cur_row = 0; cur_row < row_count; ++cur_row)
		{
			bool has_triggered_cond = false;

			size_t cur_source = 0;
			for (const auto & cond : conds)
			{
				if (cond.get(cur_row))
				{
					res[cur_row] = sources[cur_source]->get(cur_row);
					has_triggered_cond = true;
					break;
				}
				++cur_source;
			}

			if (!has_triggered_cond)
				res[cur_row] = sources.back()->get(cur_row);
		}
	}

private:
	template <typename TType>
	using ConcreteNumericSourceCreator = NumericSourceCreator<TResult, TType>;

private:
	/// Create the result column.
	static PaddedPODArray<TResult> & createSink(Block & block, size_t result, size_t size)
	{
		ColumnVector<TResult> * col_res = new ColumnVector<TResult>;
		block.getByPosition(result).column = col_res;

		typename ColumnVector<TResult>::Container_t & vec_res = col_res->getData();
		vec_res.resize(size);

		return vec_res;
	}

	/// Create accessors for condition values.
	static CondSources createConds(const Block & block, const ColumnNumbers & args)
	{
		CondSources conds;
		conds.reserve(getCondCount(args));

		for (size_t i = firstCond(); i < elseArg(args); i = nextCond(i))
			conds.emplace_back(block, args, i);
		return conds;
	}

	/// Create accessors for branch values.
	static NumericSources<TResult> createNumericSources(const Block & block,
		const ColumnNumbers & args, const Branches & branches)
	{
		NumericSources<TResult> sources;
		sources.reserve(branches.size());

		for (const auto & br : branches)
		{
			NumericSourcePtr<TResult> source;

			if (!NumericTypeDispatcher<ConcreteNumericSourceCreator>::apply(source, block, args, br))
				throw Exception{"Illegal type of argument " + toString(br.index) + " of function multiIf",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

			sources.push_back(std::move(source));
		}

		return sources;
	}
};

/// Processing of multiIf in the case of an invalid return type.
template <>
class NumericEvaluator<NumberTraits::Error>
{
public:
	static void perform(const Branches & branches, Block & block, const ColumnNumbers & args, size_t result)
	{
		throw Exception{"Internal logic error", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

}

}
