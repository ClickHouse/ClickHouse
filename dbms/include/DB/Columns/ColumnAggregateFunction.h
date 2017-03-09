#pragma once

#include <DB/Common/Arena.h>

#include <DB/AggregateFunctions/IAggregateFunction.h>

#include <DB/Columns/IColumn.h>

#include <DB/Core/Field.h>

#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int PARAMETER_OUT_OF_BOUND;
	extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}


/** State column of aggregate functions.
  * Presented as an array of pointers to the states of aggregate functions (data).
  * The states themselves are stored in one of the pools (arenas).
  *
  * It can be in two variants:
  *
  * 1. Own its values - that is, be responsible for destroying them.
  * The column consists of the values "assigned to it" after the aggregation is performed (see Aggregator, convertToBlocks function),
  *  or from values created by itself (see `insert` method).
  * In this case, `src` will be `nullptr`, and the column itself will be destroyed (call `IAggregateFunction::destroy`)
  *  states of aggregate functions in the destructor.
  *
  * 2. Do not own its values, but use values taken from another ColumnAggregateFunction column.
  * For example, this is a column obtained by permutation/filtering or other transformations from another column.
  * In this case, `src` will be `shared ptr` to the source column. Destruction of values will be handled by this source column.
  *
  * This solution is somewhat limited:
  * - the variant in which the column contains a part of "it's own" and a part of "another's" values is not supported;
  * - the option of having multiple source columns is not supported, which may be necessary for a more optimal merge of the two columns.
  *
  * These restrictions can be removed if you add an array of flags or even refcount,
  *  specifying which individual values should be destroyed and which ones should not.
  * Clearly, this method would have a substantially non-zero price.
  */
class ColumnAggregateFunction final : public IColumn, public std::enable_shared_from_this<ColumnAggregateFunction>
{
public:
	using Container_t = PaddedPODArray<AggregateDataPtr>;

private:
	/// Memory pools. Aggregate states are allocated from them.
	Arenas arenas;

	/// Used for destroying states and for finalization of values.
	AggregateFunctionPtr func;

	/// Source column. Used (holds source from destruction),
	///  if this column has been constructed from another and uses all or part of its values.
	std::shared_ptr<const ColumnAggregateFunction> src;

	/// Array of pointers to aggregation states, that are placed in arenas.
	Container_t data;

public:
	/// Create a new column that has another column as a source.
	ColumnAggregateFunction(const ColumnAggregateFunction & other)
		: arenas(other.arenas), func(other.func), src(other.shared_from_this())
	{
	}

	ColumnAggregateFunction(const AggregateFunctionPtr & func_)
		: func(func_)
	{
	}

	ColumnAggregateFunction(const AggregateFunctionPtr & func_, const Arenas & arenas_)
		: arenas(arenas_), func(func_)
	{
	}

    ~ColumnAggregateFunction()
	{
		if (!func->hasTrivialDestructor() && !src)
			for (auto val : data)
				func->destroy(val);
	}

    void set(const AggregateFunctionPtr & func_)
	{
		func = func_;
	}

	AggregateFunctionPtr getAggregateFunction() { return func; }
	AggregateFunctionPtr getAggregateFunction() const { return func; }

	/// Take shared ownership of Arena, that holds memory for states of aggregate functions.
	void addArena(ArenaPtr arena_)
	{
		arenas.push_back(arena_);
	}

	/** Transform column with states of aggregate functions to column with final result values.
	  */
	ColumnPtr convertToValues() const;

	std::string getName() const override { return "ColumnAggregateFunction"; }

	size_t sizeOfField() const override { return sizeof(getData()[0]); }

	size_t size() const override
	{
		return getData().size();
	}

	ColumnPtr cloneEmpty() const override
	{
		return std::make_shared<ColumnAggregateFunction>(func, Arenas(1, std::make_shared<Arena>()));
	};

	Field operator[](size_t n) const override
	{
		Field field = String();
		{
			WriteBufferFromString buffer(field.get<String &>());
			func->serialize(getData()[n], buffer);
		}
		return field;
	}

	void get(size_t n, Field & res) const override
	{
		res = String();
		{
			WriteBufferFromString buffer(res.get<String &>());
			func->serialize(getData()[n], buffer);
		}
	}

	StringRef getDataAt(size_t n) const override
	{
		return StringRef(reinterpret_cast<const char *>(&getData()[n]), sizeof(getData()[n]));
	}

	void insertData(const char * pos, size_t length) override
	{
		getData().push_back(*reinterpret_cast<const AggregateDataPtr *>(pos));
	}

	void insertFrom(const IColumn & src, size_t n) override
	{
		/// Must create new state of aggregate function and take ownership of it,
		///  because ownership of states of aggregate function cannot be shared for individual rows,
		///  (only as a whole, see comment above).
		insertDefault();
		insertMergeFrom(src, n);
	}

	void insertFrom(ConstAggregateDataPtr place)
	{
		insertDefault();
		insertMergeFrom(place);
	}

	/// Merge state at last row with specified state in another column.
	void insertMergeFrom(ConstAggregateDataPtr place)
	{
		func->merge(getData().back(), place, &createOrGetArena());
	}

	void insertMergeFrom(const IColumn & src, size_t n)
	{
		insertMergeFrom(static_cast<const ColumnAggregateFunction &>(src).getData()[n]);
	}

	Arena & createOrGetArena()
	{
		if (unlikely(arenas.empty()))
			arenas.emplace_back(std::make_shared<Arena>());
		return *arenas.back().get();
	}

	void insert(const Field & x) override
	{
		IAggregateFunction * function = func.get();

		Arena & arena = createOrGetArena();

		getData().push_back(arena.alloc(function->sizeOfData()));
		function->create(getData().back());
		ReadBufferFromString read_buffer(x.get<const String &>());
		function->deserialize(getData().back(), read_buffer, &arena);
	}

	void insertDefault() override
	{
		IAggregateFunction * function = func.get();

		Arena & arena = createOrGetArena();

		getData().push_back(arena.alloc(function->sizeOfData()));
		function->create(getData().back());
	}

	StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override
	{
		throw Exception("Method serializeValueIntoArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	const char * deserializeAndInsertFromArena(const char * pos) override
	{
		throw Exception("Method deserializeAndInsertFromArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	void updateHashWithValue(size_t n, SipHash & hash) const override;

	size_t byteSize() const override;

	size_t allocatedSize() const override;

	void insertRangeFrom(const IColumn & from, size_t start, size_t length) override;

	void popBack(size_t n) override
	{
		size_t size = data.size();
		size_t new_size = size - n;

		if (!src)
			for (size_t i = new_size; i < size; ++i)
				func->destroy(data[i]);

		data.resize_assume_reserved(new_size);
	}

	ColumnPtr filter(const Filter & filter, ssize_t result_size_hint) const override;

	ColumnPtr permute(const Permutation & perm, size_t limit) const override;

	ColumnPtr replicate(const Offsets_t & offsets) const override
	{
		throw Exception("Method replicate is not supported for ColumnAggregateFunction.", ErrorCodes::NOT_IMPLEMENTED);
	}

	Columns scatter(ColumnIndex num_columns, const Selector & selector) const override
	{
		/// Columns with scattered values will point to this column as the owner of values.
		Columns columns(num_columns);
		for (auto & column : columns)
			column = std::make_shared<ColumnAggregateFunction>(*this);

		size_t num_rows = size();

		{
			size_t reserve_size = num_rows / num_columns * 1.1;	/// 1.1 is just a guess. Better to use n-sigma rule.

			if (reserve_size > 1)
				for (auto & column : columns)
					column->reserve(reserve_size);
		}

		for (size_t i = 0; i < num_rows; ++i)
			static_cast<ColumnAggregateFunction &>(*columns[selector[i]]).data.push_back(data[i]);

		return columns;
	}

	int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override
	{
		return 0;
	}

	void getPermutation(bool reverse, size_t limit, Permutation & res) const override
	{
		size_t s = getData().size();
		res.resize(s);
		for (size_t i = 0; i < s; ++i)
			res[i] = i;
	}

    /** More efficient manipulation methods */
	Container_t & getData()
	{
		return data;
	}

	const Container_t & getData() const
	{
		return data;
	}

	void getExtremes(Field & min, Field & max) const override
	{
		throw Exception("Method getExtremes is not supported for ColumnAggregateFunction.", ErrorCodes::NOT_IMPLEMENTED);
	}
};


}
