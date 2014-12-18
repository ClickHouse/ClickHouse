#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/ColumnString.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{

/** Агрегатные функции, запоминающие одно какое-либо переданное значение.
  * Например, min, max, any, anyLast.
  */


/// Для числовых значений.
template <typename T>
struct SingleValueDataFixed
{
	typedef SingleValueDataFixed<T> Self;

	bool has_value = false;	/// Надо запомнить, было ли передано хотя бы одно значение. Это нужно для AggregateFunctionIf.
	T value;


	bool has() const
	{
		return has_value;
	}

	void insertResultInto(IColumn & to) const
	{
		if (has())
			static_cast<ColumnVector<T> &>(to).getData().push_back(value);
		else
			static_cast<ColumnVector<T> &>(to).insertDefault();
	}

	void write(WriteBuffer & buf, const IDataType & data_type) const
	{
		writeBinary(has(), buf);
		if (has())
			writeBinary(value, buf);
	}

	void read(ReadBuffer & buf, const IDataType & data_type)
	{
		readBinary(has_value, buf);
		if (has())
			readBinary(value, buf);
	}


	void change(const IColumn & column, size_t row_num)
	{
		has_value = true;
		value = static_cast<const ColumnVector<T> &>(column).getData()[row_num];
	}

	void change(const Self & to)
	{
		has_value = true;
		value = to.value;
	}

	void changeFirstTime(const IColumn & column, size_t row_num)
	{
		if (!has())
			change(column, row_num);
	}

	void changeFirstTime(const Self & to)
	{
		if (!has())
			change(to);
	}

	void changeIfLess(const IColumn & column, size_t row_num)
	{
		if (!has() || static_cast<const ColumnVector<T> &>(column).getData()[row_num] < value)
			change(column, row_num);
	}

	void changeIfLess(const Self & to)
	{
		if (to.has() && (!has() || to.value < value))
			change(to);
	}

	void changeIfGreater(const IColumn & column, size_t row_num)
	{
		if (!has() || static_cast<const ColumnVector<T> &>(column).getData()[row_num] > value)
			change(column, row_num);
	}

	void changeIfGreater(const Self & to)
	{
		if (to.has() && (!has() || to.value > value))
			change(to);
	}
};


/** Для строк. Короткие строки хранятся в самой структуре, а длинные выделяются отдельно.
  * NOTE Могло бы подойти также для массивов чисел.
  */
struct __attribute__((__packed__)) SingleValueDataString
{
	typedef SingleValueDataString Self;

	Int32 size = -1;	/// -1 обозначает, что значения нет.

	static constexpr Int32 AUTOMATIC_STORAGE_SIZE = 64;
	static constexpr Int32 MAX_SMALL_STRING_SIZE = AUTOMATIC_STORAGE_SIZE - sizeof(size);

	union
	{
		char small_data[MAX_SMALL_STRING_SIZE];	/// Включая завершающий ноль.
		char * large_data;
	};

	~SingleValueDataString()
	{
		if (size > MAX_SMALL_STRING_SIZE)
			free(large_data);
	}

	bool has() const
	{
		return size >= 0;
	}

	const char * getData() const
	{
		return size <= MAX_SMALL_STRING_SIZE ? small_data : large_data;
	}

	StringRef getStringRef() const
	{
		return StringRef(getData(), size);
	}

	void insertResultInto(IColumn & to) const
	{
		if (has())
			static_cast<ColumnString &>(to).insertDataWithTerminatingZero(getData(), size);
		else
			static_cast<ColumnString &>(to).insertDefault();
	}

	void write(WriteBuffer & buf, const IDataType & data_type) const
	{
		writeBinary(size, buf);
		if (has())
			buf.write(getData(), size);
	}

	void read(ReadBuffer & buf, const IDataType & data_type)
	{
		Int32 rhs_size;
		readBinary(rhs_size, buf);

		if (rhs_size >= 0)
		{
			if (rhs_size <= MAX_SMALL_STRING_SIZE)
			{
				if (size > MAX_SMALL_STRING_SIZE)
					free(large_data);

				size = rhs_size;

				if (size > 0)
					buf.read(small_data, size);
			}
			else
			{
				if (size < rhs_size)
				{
					if (size > MAX_SMALL_STRING_SIZE)
						free(large_data);

					large_data = reinterpret_cast<char *>(malloc(rhs_size));
				}

				size = rhs_size;
				buf.read(large_data, size);
			}
		}
		else
		{
			if (size > MAX_SMALL_STRING_SIZE)
				free(large_data);
			size = rhs_size;
		}
	}


	void changeImpl(StringRef value)
	{
		Int32 value_size = value.size;

		if (value_size <= MAX_SMALL_STRING_SIZE)
		{
			if (size > MAX_SMALL_STRING_SIZE)
				free(large_data);

			size = value_size;

			if (size > 0)
				memcpy(small_data, value.data, size);
		}
		else
		{
			if (size < value_size)
			{
				if (size > MAX_SMALL_STRING_SIZE)
					free(large_data);

				large_data = reinterpret_cast<char *>(malloc(value.size));
			}

			size = value_size;
			memcpy(large_data, value.data, size);
		}
	}

	void change(const IColumn & column, size_t row_num)
	{
		changeImpl(static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num));
	}

	void change(const Self & to)
	{
		changeImpl(to.getStringRef());
	}

	void changeFirstTime(const IColumn & column, size_t row_num)
	{
		if (!has())
			change(column, row_num);
	}

	void changeFirstTime(const Self & to)
	{
		if (!has())
			change(to);
	}

	void changeIfLess(const IColumn & column, size_t row_num)
	{
		if (!has() || static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num) < getStringRef())
			change(column, row_num);
	}

	void changeIfLess(const Self & to)
	{
		if (to.has() && (!has() || to.getStringRef() < getStringRef()))
			change(to);
	}

	void changeIfGreater(const IColumn & column, size_t row_num)
	{
		if (!has() || static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num) > getStringRef())
			change(column, row_num);
	}

	void changeIfGreater(const Self & to)
	{
		if (to.has() && (!has() || to.getStringRef() > getStringRef()))
			change(to);
	}
};


/// Для любых других типов значений.
struct SingleValueDataGeneric
{
	typedef SingleValueDataGeneric Self;

	Field value;

	bool has() const
	{
		return !value.isNull();
	}

	void insertResultInto(IColumn & to) const
	{
		if (has())
			to.insert(value);
		else
			to.insertDefault();
	}

	void write(WriteBuffer & buf, const IDataType & data_type) const
	{
		if (!value.isNull())
		{
			writeBinary(true, buf);
			data_type.serializeBinary(value, buf);
		}
		else
			writeBinary(false, buf);
	}

	void read(ReadBuffer & buf, const IDataType & data_type)
	{
		bool is_not_null;
		readBinary(is_not_null, buf);

		if (is_not_null)
			data_type.deserializeBinary(value, buf);
	}

	void change(const IColumn & column, size_t row_num)
	{
		column.get(row_num, value);
	}

	void change(const Self & to)
	{
		value = to.value;
	}

	void changeFirstTime(const IColumn & column, size_t row_num)
	{
		if (!has())
			change(column, row_num);
	}

	void changeFirstTime(const Self & to)
	{
		if (!has())
			change(to);
	}

	void changeIfLess(const IColumn & column, size_t row_num)
	{
		if (!has())
			change(column, row_num);
		else
		{
			Field new_value;
			column.get(row_num, new_value);
			if (new_value < value)
				value = new_value;
		}
	}

	void changeIfLess(const Self & to)
	{
		if (to.has() && (!has() || to.value < value))
			change(to);
	}

	void changeIfGreater(const IColumn & column, size_t row_num)
	{
		if (!has())
			change(column, row_num);
		else
		{
			Field new_value;
			column.get(row_num, new_value);
			if (new_value > value)
				value = new_value;
		}
	}

	void changeIfGreater(const Self & to)
	{
		if (to.has() && (!has() || to.value > value))
			change(to);
	}
};


/** То, чем отличаются друг от другая агрегатные функции min, max, any, anyLast
  *  (условием, при котором сохранённое значение заменяется на новое,
  *   а также, конечно, именем).
  */

template <typename Data>
struct AggregateFunctionMinData : Data
{
	typedef AggregateFunctionMinData<Data> Self;

	void changeIfBetter(const IColumn & column, size_t row_num) { this->changeIfLess(column, row_num); }
	void changeIfBetter(const Self & to) 						{ this->changeIfLess(to); }

	static const char * name() { return "min"; }
};

template <typename Data>
struct AggregateFunctionMaxData : Data
{
	typedef AggregateFunctionMaxData<Data> Self;

	void changeIfBetter(const IColumn & column, size_t row_num) { this->changeIfGreater(column, row_num); }
	void changeIfBetter(const Self & to) 						{ this->changeIfGreater(to); }

	static const char * name() { return "max"; }
};

template <typename Data>
struct AggregateFunctionAnyData : Data
{
	typedef AggregateFunctionAnyData<Data> Self;

	void changeIfBetter(const IColumn & column, size_t row_num) { this->changeFirstTime(column, row_num); }
	void changeIfBetter(const Self & to) 						{ this->changeFirstTime(to); }

	static const char * name() { return "any"; }
};

template <typename Data>
struct AggregateFunctionAnyLastData : Data
{
	typedef AggregateFunctionAnyLastData<Data> Self;

	void changeIfBetter(const IColumn & column, size_t row_num) { this->change(column, row_num); }
	void changeIfBetter(const Self & to) 						{ this->change(to); }

	static const char * name() { return "anyLast"; }
};


template <typename Data>
class AggregateFunctionsSingleValue final : public IUnaryAggregateFunction<Data, AggregateFunctionsSingleValue<Data> >
{
private:
	DataTypePtr type;

public:
	String getName() const { return Data::name(); }

	DataTypePtr getReturnType() const
	{
		return type;
	}

	void setArgument(const DataTypePtr & argument)
	{
		type = argument;
	}


	void addOne(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		this->data(place).changeIfBetter(column, row_num);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		this->data(place).changeIfBetter(this->data(rhs));
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		this->data(place).write(buf, *type.get());
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		Data rhs;	/// Для строчек не очень оптимально, так как может делаться одна лишняя аллокация.
		rhs.read(buf, *type.get());

		this->data(place).changeIfBetter(rhs);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		this->data(place).insertResultInto(to);
	}
};

}
