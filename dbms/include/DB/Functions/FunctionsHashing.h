#pragma once

#include <openssl/md5.h>
#include <city.h>

#include <Poco/ByteOrder.h>

#include <DB/Common/SipHash.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnConst.h>
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
  * Быстрая некриптографическая хэш функция от любого целого числа:
  * intHash32:	number -> UInt32
  * intHash64:  number -> UInt64
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

struct CityHash64Impl
{
	static UInt64 apply(const char * begin, size_t size)
	{
		return CityHash64(begin, size);
	}
};

struct IntHash32Impl
{
	typedef UInt32 ReturnType;
	
	static UInt32 apply(UInt64 x)
	{
		/// seed взят из /dev/urandom.
		return intHash32<0x75D9543DE018BF45ULL>(x);
	}
};

struct IntHash64Impl
{
	typedef UInt64 ReturnType;

	static UInt64 apply(UInt64 x)
	{
		return intHash64(x);
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

		if (!dynamic_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeUInt64;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnString * col_from = dynamic_cast<const ColumnString *>(&*block.getByPosition(arguments[0]).column))
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
		else if (const ColumnConstString * col_from = dynamic_cast<const ColumnConstString *>(&*block.getByPosition(arguments[0]).column))
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
		if (ColumnVector<FromType> * col_from = dynamic_cast<ColumnVector<FromType> *>(&*block.getByPosition(arguments[0]).column))
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
		else if (ColumnConst<FromType> * col_from = dynamic_cast<ColumnConst<FromType> *>(&*block.getByPosition(arguments[0]).column))
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
		
		if      (dynamic_cast<const DataTypeUInt8 *		>(from_type)) executeType<UInt8	>(block, arguments, result);
		else if (dynamic_cast<const DataTypeUInt16 *	>(from_type)) executeType<UInt16>(block, arguments, result);
		else if (dynamic_cast<const DataTypeUInt32 *	>(from_type)) executeType<UInt32>(block, arguments, result);
		else if (dynamic_cast<const DataTypeUInt64 *	>(from_type)) executeType<UInt64>(block, arguments, result);
		else if (dynamic_cast<const DataTypeInt8 *		>(from_type)) executeType<Int8	>(block, arguments, result);
		else if (dynamic_cast<const DataTypeInt16 *		>(from_type)) executeType<Int16	>(block, arguments, result);
		else if (dynamic_cast<const DataTypeInt32 *		>(from_type)) executeType<Int32	>(block, arguments, result);
		else if (dynamic_cast<const DataTypeInt64 *		>(from_type)) executeType<Int64	>(block, arguments, result);
		else if (dynamic_cast<const DataTypeDate *		>(from_type)) executeType<UInt16>(block, arguments, result);
		else if (dynamic_cast<const DataTypeDateTime *	>(from_type)) executeType<UInt32>(block, arguments, result);
		else
			throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
};


struct NameHalfMD5 			{ static const char * get() { return "halfMD5"; } };
struct NameSipHash64		{ static const char * get() { return "sipHash64"; } };
struct NameCityHash64 		{ static const char * get() { return "cityHash64"; } };
struct NameIntHash32 		{ static const char * get() { return "intHash32"; } };
struct NameIntHash64 		{ static const char * get() { return "intHash64"; } };

typedef FunctionStringHash64<HalfMD5Impl,		NameHalfMD5> 		FunctionHalfMD5;
typedef FunctionStringHash64<SipHash64Impl,		NameSipHash64> 		FunctionSipHash64;
typedef FunctionStringHash64<CityHash64Impl,	NameCityHash64> 	FunctionCityHash64;
typedef FunctionIntHash<IntHash32Impl,			NameIntHash32> 		FunctionIntHash32;
typedef FunctionIntHash<IntHash64Impl,			NameIntHash64> 		FunctionIntHash64;


}
