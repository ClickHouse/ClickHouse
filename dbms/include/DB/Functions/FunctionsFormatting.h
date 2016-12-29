#include <DB/Functions/IFunction.h>
#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/IO/WriteBufferFromVector.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/Common/formatReadable.h>


namespace DB
{

/** Функция для необычного преобразования в строку:
	*
	* bitmaskToList - принимает целое число - битовую маску, возвращает строку из степеней двойки через запятую.
	* 					например, bitmaskToList(50) = '2,16,32'
	*
	* formatReadableSize - выводит переданный размер в байтах в виде 123.45 GiB.
	*/

class FunctionBitmaskToList : public IFunction
{
public:
	static constexpr auto name = "bitmaskToList";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionBitmaskToList>(); }

	/// Получить основное имя функции.
	virtual String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return 1; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	virtual DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		const IDataType * type = &*arguments[0];

		if (!typeid_cast<const DataTypeUInt8 *>(type) &&
			!typeid_cast<const DataTypeUInt16 *>(type) &&
			!typeid_cast<const DataTypeUInt32 *>(type) &&
			!typeid_cast<const DataTypeUInt64 *>(type) &&
			!typeid_cast<const DataTypeInt8 *>(type) &&
			!typeid_cast<const DataTypeInt16 *>(type) &&
			!typeid_cast<const DataTypeInt32 *>(type) &&
			!typeid_cast<const DataTypeInt64 *>(type))
			throw Exception("Cannot format " + type->getName() + " as bitmask string", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeString>();
	}

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		if (!(	executeType<UInt8>(block, arguments, result)
			||	executeType<UInt16>(block, arguments, result)
			||	executeType<UInt32>(block, arguments, result)
			||	executeType<UInt64>(block, arguments, result)
			||	executeType<Int8>(block, arguments, result)
			||	executeType<Int16>(block, arguments, result)
			||	executeType<Int32>(block, arguments, result)
			||	executeType<Int64>(block, arguments, result)))
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
							ErrorCodes::ILLEGAL_COLUMN);
	}

private:
	template <typename T>
	inline static void writeBitmask(T x, WriteBuffer & out)
	{
		bool first = true;
		while (x)
		{
			T y = (x & (x - 1));
			T bit = x ^ y;
			x = y;
			if (!first)
				out.write(",", 1);
			first = false;
			writeIntText(bit, out);
		}
	}

	template <typename T>
	bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnVector<T> * col_from = typeid_cast<const ColumnVector<T> *>(block.getByPosition(arguments[0]).column.get()))
		{
			auto col_to = std::make_shared<ColumnString>();
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<T>::Container_t & vec_from = col_from->getData();
			ColumnString::Chars_t & data_to = col_to->getChars();
			ColumnString::Offsets_t & offsets_to = col_to->getOffsets();
			size_t size = vec_from.size();
			data_to.resize(size * 2);
			offsets_to.resize(size);

			WriteBufferFromVector<ColumnString::Chars_t> buf_to(data_to);

			for (size_t i = 0; i < size; ++i)
			{
				writeBitmask<T>(vec_from[i], buf_to);
				writeChar(0, buf_to);
				offsets_to[i] = buf_to.count();
			}
			data_to.resize(buf_to.count());
		}
		else if (const ColumnConst<T> * col_from = typeid_cast<const ColumnConst<T> *>(block.getByPosition(arguments[0]).column.get()))
		{
			std::string res;
			{
				WriteBufferFromString buf(res);
				writeBitmask<T>(col_from->getData(), buf);
			}

			block.getByPosition(result).column = std::make_shared<ColumnConstString>(col_from->size(), res);
		}
		else
		{
			return false;
		}

		return true;
	}
};


class FunctionFormatReadableSize : public IFunction
{
public:
	static constexpr auto name = "formatReadableSize";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionFormatReadableSize>(); }

	/// Получить основное имя функции.
	virtual String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return 1; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	virtual DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		const IDataType & type = *arguments[0];

		if (!type.behavesAsNumber())
			throw Exception("Cannot format " + type.getName() + " as size in bytes", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeString>();
	}

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		if (!(	executeType<UInt8>(block, arguments, result)
			||	executeType<UInt16>(block, arguments, result)
			||	executeType<UInt32>(block, arguments, result)
			||	executeType<UInt64>(block, arguments, result)
			||	executeType<Int8>(block, arguments, result)
			||	executeType<Int16>(block, arguments, result)
			||	executeType<Int32>(block, arguments, result)
			||	executeType<Int64>(block, arguments, result)
			||	executeType<Float32>(block, arguments, result)
			||	executeType<Float64>(block, arguments, result)))
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
				+ " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}

private:
	template <typename T>
	bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnVector<T> * col_from = typeid_cast<const ColumnVector<T> *>(block.getByPosition(arguments[0]).column.get()))
		{
			auto col_to = std::make_shared<ColumnString>();
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<T>::Container_t & vec_from = col_from->getData();
			ColumnString::Chars_t & data_to = col_to->getChars();
			ColumnString::Offsets_t & offsets_to = col_to->getOffsets();
			size_t size = vec_from.size();
			data_to.resize(size * 2);
			offsets_to.resize(size);

			WriteBufferFromVector<ColumnString::Chars_t> buf_to(data_to);

			for (size_t i = 0; i < size; ++i)
			{
				formatReadableSizeWithBinarySuffix(vec_from[i], buf_to);
				writeChar(0, buf_to);
				offsets_to[i] = buf_to.count();
			}
			data_to.resize(buf_to.count());
		}
		else if (const ColumnConst<T> * col_from = typeid_cast<const ColumnConst<T> *>(block.getByPosition(arguments[0]).column.get()))
		{
			block.getByPosition(result).column = std::make_shared<ColumnConstString>(col_from->size(), formatReadableSizeWithBinarySuffix(col_from->getData()));
		}
		else
		{
			return false;
		}

		return true;
	}
};

}
