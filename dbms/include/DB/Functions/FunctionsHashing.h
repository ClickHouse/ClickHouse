#pragma once

#include <openssl/md5.h>
#include <city.h>

#include <Poco/ByteOrder.h>

#include <DB/Common/SipHash.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Common/HashTable/Hash.h>
#include <DB/Functions/IFunction.h>

#include <stats/IntHash.h>


namespace DB
{

/** Функции хэширования.
  *
  * Половинка MD5:
  * halfMD5: 	String -> UInt64
  *
  * Более быстрая криптографическая хэш-функция:
  * sipHash64:  String -> UInt64
  *
  * Быстрая некриптографическая хэш функция для строк:
  * cityHash64: String -> UInt64
  *
  * Некриптографический хеш от кортежа значений любых типов (использует cityHash64 для строк и intHash64 для чисел):
  * cityHash64:  any* -> UInt64
  *
  * Быстрая некриптографическая хэш функция от любого целого числа:
  * intHash32:	number -> UInt32
  * intHash64:  number -> UInt64
  *
  */

struct HalfMD5Impl
{
	static UInt64 apply(const char * begin, size_t size)
	{
		union
		{
			unsigned char char_data[16];
			Poco::UInt64 uint64_data;
		} buf;

		MD5_CTX ctx;
		MD5_Init(&ctx);
		MD5_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
		MD5_Final(buf.char_data, &ctx);

		return Poco::ByteOrder::flipBytes(buf.uint64_data);		/// Совместимость с существующим кодом.
	}
};

struct SipHash64Impl
{
	static UInt64 apply(const char * begin, size_t size)
	{
		return sipHash64(begin, size);
	}
};

struct IntHash32Impl
{
	typedef UInt32 ReturnType;

	static UInt32 apply(UInt64 x)
	{
		/// seed взят из /dev/urandom. Он позволяет избежать нежелательных зависимостей с хэшами в разных структурах данных.
		return intHash32<0x75D9543DE018BF45ULL>(x);
	}
};

struct IntHash64Impl
{
	typedef UInt64 ReturnType;

	static UInt64 apply(UInt64 x)
	{
		return intHash64(x ^ 0x4CF2D2BAAE6DA887ULL);
	}
};


template <typename Impl, typename Name>
class FunctionStringHash64 : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return Name::get();
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeUInt64;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnString * col_from = typeid_cast<const ColumnString *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnUInt64 * col_to = new ColumnUInt64;
			block.getByPosition(result).column = col_to;

			const typename ColumnString::Chars_t & data = col_from->getChars();
			const typename ColumnString::Offsets_t & offsets = col_from->getOffsets();
			typename ColumnUInt64::Container_t & vec_to = col_to->getData();
			size_t size = offsets.size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
				vec_to[i] = Impl::apply(
					reinterpret_cast<const char *>(&data[i == 0 ? 0 : offsets[i - 1]]),
					i == 0 ? offsets[i] - 1 : (offsets[i] - 1 - offsets[i - 1]));
		}
		else if (const ColumnConstString * col_from = typeid_cast<const ColumnConstString *>(&*block.getByPosition(arguments[0]).column))
		{
			block.getByPosition(result).column = new ColumnConstUInt64(
				col_from->size(),
				Impl::apply(col_from->getData().data(), col_from->getData().size()));
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + Name::get(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


template <typename Impl, typename Name>
class FunctionIntHash : public IFunction
{
private:
	typedef typename Impl::ReturnType ToType;

	template <typename FromType>
	void executeType(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (ColumnVector<FromType> * col_from = typeid_cast<ColumnVector<FromType> *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<ToType> * col_to = new ColumnVector<ToType>;
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<FromType>::Container_t & vec_from = col_from->getData();
			typename ColumnVector<ToType>::Container_t & vec_to = col_to->getData();

			size_t size = vec_from.size();
			vec_to.resize(size);
			for (size_t i = 0; i < size; ++i)
				vec_to[i] = Impl::apply(vec_from[i]);
		}
		else if (ColumnConst<FromType> * col_from = typeid_cast<ColumnConst<FromType> *>(&*block.getByPosition(arguments[0]).column))
		{
			block.getByPosition(result).column = new ColumnConst<ToType>(col_from->size(), Impl::apply(col_from->getData()));
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + Name::get(),
				ErrorCodes::ILLEGAL_COLUMN);
	}

public:
	/// Получить имя функции.
	String getName() const
	{
		return Name::get();
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!arguments[0]->isNumeric())
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new typename DataTypeFromFieldType<typename Impl::ReturnType>::Type;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		IDataType * from_type = &*block.getByPosition(arguments[0]).type;

		if      (typeid_cast<const DataTypeUInt8 *		>(from_type)) executeType<UInt8	>(block, arguments, result);
		else if (typeid_cast<const DataTypeUInt16 *	>(from_type)) executeType<UInt16>(block, arguments, result);
		else if (typeid_cast<const DataTypeUInt32 *	>(from_type)) executeType<UInt32>(block, arguments, result);
		else if (typeid_cast<const DataTypeUInt64 *	>(from_type)) executeType<UInt64>(block, arguments, result);
		else if (typeid_cast<const DataTypeInt8 *		>(from_type)) executeType<Int8	>(block, arguments, result);
		else if (typeid_cast<const DataTypeInt16 *		>(from_type)) executeType<Int16	>(block, arguments, result);
		else if (typeid_cast<const DataTypeInt32 *		>(from_type)) executeType<Int32	>(block, arguments, result);
		else if (typeid_cast<const DataTypeInt64 *		>(from_type)) executeType<Int64	>(block, arguments, result);
		else if (typeid_cast<const DataTypeDate *		>(from_type)) executeType<UInt16>(block, arguments, result);
		else if (typeid_cast<const DataTypeDateTime *	>(from_type)) executeType<UInt32>(block, arguments, result);
		else
			throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};


template <typename T>
static UInt64 toInteger(T x)
{
	return x;
}

template <>
UInt64 toInteger<Float32>(Float32 x)
{
	UInt32 res;
	memcpy(&res, &x, sizeof(x));
	return res;
}

template <>
UInt64 toInteger<Float64>(Float64 x)
{
	UInt64 res;
	memcpy(&res, &x, sizeof(x));
	return res;
}


class FunctionCityHash64 : public IFunction
{
private:
	template <typename FromType, bool first>
	void executeIntType(const IColumn * column, ColumnUInt64::Container_t & vec_to)
	{
		if (const ColumnVector<FromType> * col_from = typeid_cast<const ColumnVector<FromType> *>(column))
		{
			const typename ColumnVector<FromType>::Container_t & vec_from = col_from->getData();
			size_t size = vec_from.size();
			for (size_t i = 0; i < size; ++i)
			{
				UInt64 h = IntHash64Impl::apply(toInteger(vec_from[i]));
				if (first)
					vec_to[i] = h;
				else
					vec_to[i] = Hash128to64(uint128(vec_to[i], h));
			}
		}
		else if (const ColumnConst<FromType> * col_from = typeid_cast<const ColumnConst<FromType> *>(column))
		{
			UInt64 hash = IntHash64Impl::apply(toInteger(col_from->getData()));
			size_t size = vec_to.size();
			if (first)
			{
				vec_to.assign(size, hash);
			}
			else
			{
				for (size_t i = 0; i < size; ++i)
					vec_to[i] = Hash128to64(uint128(vec_to[i], hash));
			}
		}
		else
			throw Exception("Illegal column " + column->getName()
				+ " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}

	template <bool first>
	void executeString(const IColumn * column, ColumnUInt64::Container_t & vec_to)
	{
		if (const ColumnString * col_from = typeid_cast<const ColumnString *>(column))
		{
			const typename ColumnString::Chars_t & data = col_from->getChars();
			const typename ColumnString::Offsets_t & offsets = col_from->getOffsets();
			size_t size = offsets.size();

			for (size_t i = 0; i < size; ++i)
			{
				UInt64 h = CityHash64(
					reinterpret_cast<const char *>(&data[i == 0 ? 0 : offsets[i - 1]]),
					i == 0 ? offsets[i] - 1 : (offsets[i] - 1 - offsets[i - 1]));
				if (first)
					vec_to[i] = h;
				else
					vec_to[i] = Hash128to64(uint128(vec_to[i], h));
			}
		}
		else if (const ColumnFixedString * col_from = typeid_cast<const ColumnFixedString *>(column))
		{
			const typename ColumnString::Chars_t & data = col_from->getChars();
			size_t n = col_from->getN();
			size_t size = data.size() / n;
			for (size_t i = 0; i < size; ++i)
			{
				UInt64 h = CityHash64(reinterpret_cast<const char *>(&data[i * n]), n);
				if (first)
					vec_to[i] = h;
				else
					vec_to[i] = Hash128to64(uint128(vec_to[i], h));
			}
		}
		else if (const ColumnConstString * col_from = typeid_cast<const ColumnConstString *>(column))
		{
			UInt64 hash = CityHash64(col_from->getData().data(), col_from->getData().size());
			size_t size = vec_to.size();
			if (first)
			{
				vec_to.assign(size, hash);
			}
			else
			{
				for (size_t i = 0; i < size; ++i)
				{
					vec_to[i] = Hash128to64(uint128(vec_to[i], hash));
				}
			}
		}
		else
			throw Exception("Illegal column " + column->getName()
					+ " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}

	template <bool first>
	void executeArray(const IDataType * type, const IColumn * column, ColumnUInt64::Container_t & vec_to)
	{
		const IDataType * nested_type = &*typeid_cast<const DataTypeArray *>(type)->getNestedType();

		if (const ColumnArray * col_from = typeid_cast<const ColumnArray *>(column))
		{
			const IColumn * nested_column = &col_from->getData();
			const ColumnArray::Offsets_t & offsets = col_from->getOffsets();
			size_t nested_size = nested_column->size();

			ColumnUInt64::Container_t vec_temp(nested_size);
			executeAny<true>(nested_type, nested_column, vec_temp);

			size_t size = offsets.size();

			for (size_t i = 0; i < size; ++i)
			{
				size_t begin = i == 0 ? 0 : offsets[i - 1];
				size_t end = offsets[i];

				UInt64 h = IntHash64Impl::apply(end - begin);
				if (first)
					vec_to[i] = h;
				else
					vec_to[i] = Hash128to64(uint128(vec_to[i], h));

				for (size_t j = begin; j < end; ++j)
					vec_to[i] = Hash128to64(uint128(vec_to[i], vec_temp[j]));
			}
		}
		else if (const ColumnConstArray * col_from = typeid_cast<const ColumnConstArray *>(column))
		{
			/// NOTE: тут, конечно, можно обойтись без материалиации столбца.
			ColumnPtr full_column = col_from->convertToFullColumn();
			executeArray<first>(type, &*full_column, vec_to);
		}
		else
			throw Exception("Illegal column " + column->getName()
					+ " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}

	template <bool first>
	void executeAny(const IDataType * from_type, const IColumn * icolumn, ColumnUInt64::Container_t & vec_to)
	{
		if      (typeid_cast<const DataTypeUInt8 *		>(from_type)) executeIntType<UInt8,		first>(icolumn, vec_to);
		else if (typeid_cast<const DataTypeUInt16 *		>(from_type)) executeIntType<UInt16,	first>(icolumn, vec_to);
		else if (typeid_cast<const DataTypeUInt32 *		>(from_type)) executeIntType<UInt32,	first>(icolumn, vec_to);
		else if (typeid_cast<const DataTypeUInt64 *		>(from_type)) executeIntType<UInt64,	first>(icolumn, vec_to);
		else if (typeid_cast<const DataTypeInt8 *		>(from_type)) executeIntType<Int8,		first>(icolumn, vec_to);
		else if (typeid_cast<const DataTypeInt16 *		>(from_type)) executeIntType<Int16,		first>(icolumn, vec_to);
		else if (typeid_cast<const DataTypeInt32 *		>(from_type)) executeIntType<Int32,		first>(icolumn, vec_to);
		else if (typeid_cast<const DataTypeInt64 *		>(from_type)) executeIntType<Int64,		first>(icolumn, vec_to);
		else if (typeid_cast<const DataTypeDate *		>(from_type)) executeIntType<UInt16,	first>(icolumn, vec_to);
		else if (typeid_cast<const DataTypeDateTime *	>(from_type)) executeIntType<UInt32,	first>(icolumn, vec_to);
		else if (typeid_cast<const DataTypeFloat32 *	>(from_type)) executeIntType<Float32,	first>(icolumn, vec_to);
		else if (typeid_cast<const DataTypeFloat64 *	>(from_type)) executeIntType<Float64,	first>(icolumn, vec_to);
		else if (typeid_cast<const DataTypeString *		>(from_type)) executeString	<			first>(icolumn, vec_to);
		else if (typeid_cast<const DataTypeFixedString *>(from_type)) executeString	<			first>(icolumn, vec_to);
		else if (typeid_cast<const DataTypeArray *		>(from_type)) executeArray	<			first>(from_type, icolumn, vec_to);
		else
			throw Exception("Unexpected type " + from_type->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

public:
	/// Получить имя функции.
	String getName() const
	{
		return "cityHash64";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		return new DataTypeUInt64;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		size_t rows = block.rowsInFirstColumn();
		ColumnUInt64 * col_to = new ColumnUInt64(rows);
		block.getByPosition(result).column = col_to;

		ColumnUInt64::Container_t & vec_to = col_to->getData();

		if (arguments.empty())
		{
			/// Случайное число из /dev/urandom используется как хеш пустого кортежа.
			vec_to.assign(rows, 0xe28dbde7fe22e41c);
		}

		for (size_t i = 0; i < arguments.size(); ++i)
		{
			const ColumnWithNameAndType & column = block.getByPosition(arguments[i]);
			const IDataType * from_type = &*column.type;
			const IColumn * icolumn = &*column.column;

			if (i == 0)
				executeAny<true>(from_type, icolumn, vec_to);
			else
				executeAny<false>(from_type, icolumn, vec_to);
		}
	}
};


struct NameHalfMD5 			{ static const char * get() { return "halfMD5"; } };
struct NameSipHash64		{ static const char * get() { return "sipHash64"; } };
struct NameCityHash64 		{ static const char * get() { return "cityHash64"; } };
struct NameIntHash32 		{ static const char * get() { return "intHash32"; } };
struct NameIntHash64 		{ static const char * get() { return "intHash64"; } };

typedef FunctionStringHash64<HalfMD5Impl,		NameHalfMD5> 		FunctionHalfMD5;
typedef FunctionStringHash64<SipHash64Impl,		NameSipHash64> 		FunctionSipHash64;
typedef FunctionIntHash<IntHash32Impl,			NameIntHash32> 		FunctionIntHash32;
typedef FunctionIntHash<IntHash64Impl,			NameIntHash64> 		FunctionIntHash64;


}
