#pragma once

#include <openssl/md5.h>
#include <openssl/sha.h>
#include <city.h>
#include <farmhash.h>
#include <metrohash.h>

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

#include <statdaemons/ext/range.hpp>

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

struct MD5Impl
{
	static constexpr auto name = "MD5";
	static constexpr auto length = 16;

	static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
	{
		MD5_CTX ctx;
		MD5_Init(&ctx);
		MD5_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
		MD5_Final(out_char_data, &ctx);
	}
};

struct SHA1Impl
{
	static constexpr auto name = "SHA1";
	static constexpr auto length = 20;

	static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
	{
		SHA_CTX ctx;
		SHA1_Init(&ctx);
		SHA1_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
		SHA1_Final(out_char_data, &ctx);
	}
};

struct SHA224Impl
{
	static constexpr auto name = "SHA224";
	static constexpr auto length = 28;

	static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
	{
		SHA256_CTX ctx;
		SHA224_Init(&ctx);
		SHA224_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
		SHA224_Final(out_char_data, &ctx);
	}
};

struct SHA256Impl
{
	static constexpr auto name = "SHA256";
	static constexpr auto length = 32;

	static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
	{
		SHA256_CTX ctx;
		SHA256_Init(&ctx);
		SHA256_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
		SHA256_Final(out_char_data, &ctx);
	}
};

struct SipHash64Impl
{
	static UInt64 apply(const char * begin, size_t size)
	{
		return sipHash64(begin, size);
	}
};

struct SipHash128Impl
{
	static constexpr auto name = "sipHash128";
	static constexpr auto length = 16;

	static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
	{
		sipHash128(begin, size, reinterpret_cast<char*>(out_char_data));
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
	static constexpr auto name = Name::name;
	static IFunction * create(const Context & context) { return new FunctionStringHash64; };

	/// Получить имя функции.
	String getName() const
	{
		return name;
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
					+ " of first argument of function " + Name::name,
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


template <typename Impl>
class FunctionStringHashFixedString : public IFunction
{
public:
	static constexpr auto name = Impl::name;
	static IFunction * create(const Context & context) { return new FunctionStringHashFixedString; };

	/// Получить имя функции.
	String getName() const
	{
		return name;
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

		return new DataTypeFixedString{Impl::length};
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnString * col_from = typeid_cast<const ColumnString *>(&*block.getByPosition(arguments[0]).column))
		{
			auto col_to = new ColumnFixedString{Impl::length};
			block.getByPosition(result).column = col_to;

			const typename ColumnString::Chars_t & data = col_from->getChars();
			const typename ColumnString::Offsets_t & offsets = col_from->getOffsets();
			auto & chars_to = col_to->getChars();
			const auto size = offsets.size();
			chars_to.resize(size * Impl::length);

			for (size_t i = 0; i < size; ++i)
				Impl::apply(
					reinterpret_cast<const char *>(&data[i == 0 ? 0 : offsets[i - 1]]),
					i == 0 ? offsets[i] - 1 : (offsets[i] - 1 - offsets[i - 1]),
					&chars_to[i * Impl::length]);
		}
		else if (const ColumnConstString * col_from = typeid_cast<const ColumnConstString *>(&*block.getByPosition(arguments[0]).column))
		{
			const auto & data = col_from->getData();

			String hash(Impl::length, 0);
			Impl::apply(data.data(), data.size(), reinterpret_cast<unsigned char *>(&hash[0]));

			block.getByPosition(result).column = new ColumnConst<String>{
				col_from->size(),
				hash,
				new DataTypeFixedString{Impl::length}
			};
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


template <typename Impl, typename Name>
class FunctionIntHash : public IFunction
{
public:
	static constexpr auto name = Name::name;
	static IFunction * create(const Context & context) { return new FunctionIntHash; };

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
					+ " of first argument of function " + Name::name,
				ErrorCodes::ILLEGAL_COLUMN);
	}

public:
	/// Получить имя функции.
	String getName() const
	{
		return name;
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


template <typename Impl>
class FunctionNeighbourhoodHash64 : public IFunction
{
public:
	static constexpr auto name = Impl::name;
	static IFunction * create(const Context & context) { return new FunctionNeighbourhoodHash64; };

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
					vec_to[i] = Impl::Hash128to64(typename Impl::uint128_t(vec_to[i], h));
			}
		}
		else if (const ColumnConst<FromType> * col_from = typeid_cast<const ColumnConst<FromType> *>(column))
		{
			const UInt64 hash = IntHash64Impl::apply(toInteger(col_from->getData()));
			size_t size = vec_to.size();
			if (first)
			{
				vec_to.assign(size, hash);
			}
			else
			{
				for (size_t i = 0; i < size; ++i)
					vec_to[i] = Impl::Hash128to64(typename Impl::uint128_t(vec_to[i], hash));
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
				const UInt64 h = Impl::Hash64(
					reinterpret_cast<const char *>(&data[i == 0 ? 0 : offsets[i - 1]]),
					i == 0 ? offsets[i] - 1 : (offsets[i] - 1 - offsets[i - 1]));
				if (first)
					vec_to[i] = h;
				else
					vec_to[i] = Impl::Hash128to64(typename Impl::uint128_t(vec_to[i], h));
			}
		}
		else if (const ColumnFixedString * col_from = typeid_cast<const ColumnFixedString *>(column))
		{
			const typename ColumnString::Chars_t & data = col_from->getChars();
			size_t n = col_from->getN();
			size_t size = data.size() / n;
			for (size_t i = 0; i < size; ++i)
			{
				const UInt64 h = Impl::Hash64(reinterpret_cast<const char *>(&data[i * n]), n);
				if (first)
					vec_to[i] = h;
				else
					vec_to[i] = Impl::Hash128to64(typename Impl::uint128_t(vec_to[i], h));
			}
		}
		else if (const ColumnConstString * col_from = typeid_cast<const ColumnConstString *>(column))
		{
			const UInt64 hash = Impl::Hash64(col_from->getData().data(), col_from->getData().size());
			const size_t size = vec_to.size();
			if (first)
			{
				vec_to.assign(size, hash);
			}
			else
			{
				for (size_t i = 0; i < size; ++i)
				{
					vec_to[i] = Impl::Hash128to64(typename Impl::uint128_t(vec_to[i], hash));
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
			const size_t nested_size = nested_column->size();

			ColumnUInt64::Container_t vec_temp(nested_size);
			executeAny<true>(nested_type, nested_column, vec_temp);

			const size_t size = offsets.size();

			for (size_t i = 0; i < size; ++i)
			{
				const size_t begin = i == 0 ? 0 : offsets[i - 1];
				const size_t end = offsets[i];

				UInt64 h = IntHash64Impl::apply(end - begin);
				if (first)
					vec_to[i] = h;
				else
					vec_to[i] = Impl::Hash128to64(typename Impl::uint128_t(vec_to[i], h));

				for (size_t j = begin; j < end; ++j)
					vec_to[i] = Impl::Hash128to64(typename Impl::uint128_t(vec_to[i], vec_temp[j]));
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
		return name;
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
			const ColumnWithTypeAndName & column = block.getByPosition(arguments[i]);
			const IDataType * from_type = &*column.type;
			const IColumn * icolumn = &*column.column;

			if (i == 0)
				executeAny<true>(from_type, icolumn, vec_to);
			else
				executeAny<false>(from_type, icolumn, vec_to);
		}
	}
};


struct URLHashImpl
{
	static UInt64 apply(const char * data, const std::size_t size)
	{
		/// do not take last slash, '?' or '#' character into account
		if (size > 0 && (data[size - 1] == '/' || data[size - 1] == '?' || data[size - 1] == '#'))
			return CityHash64(data, size - 1);

		return CityHash64(data, size);
	}
};


struct URLHierarchyHashImpl
{
	static std::size_t findLevelLength(const UInt64 level, const char * begin, const char * const end)
	{
		auto pos = begin;

		/// Распарсим всё, что идёт до пути

		/// Предположим, что протокол уже переведён в нижний регистр.
		while (pos < end && ((*pos > 'a' && *pos < 'z') || (*pos > '0' && *pos < '9')))
			++pos;

		/** Будем вычислять иерархию только для URL-ов, в которых есть протокол, и после него идут два слеша.
		*	(http, file - подходят, mailto, magnet - не подходят), и после двух слешей ещё хоть что-нибудь есть
		*	Для остальных просто вернём полный URL как единственный элемент иерархии.
		*/
		if (pos == begin || pos == end || !(*pos++ == ':' && pos < end && *pos++ == '/' && pos < end && *pos++ == '/' && pos < end))
		{
			pos = end;
			return 0 == level ? pos - begin : 0;
		}

		/// Доменом для простоты будем считать всё, что после протокола и двух слешей, до следующего слеша или до ? или до #
		while (pos < end && !(*pos == '/' || *pos == '?' || *pos == '#'))
			++pos;

		if (pos != end)
			++pos;

		if (0 == level)
			return pos - begin;

		UInt64 current_level = 0;

		while (current_level != level && pos < end)
		{
			/// Идём до следующего / или ? или #, пропуская все те, что вначале.
			while (pos < end && (*pos == '/' || *pos == '?' || *pos == '#'))
				++pos;
			if (pos == end)
				break;
			while (pos < end && !(*pos == '/' || *pos == '?' || *pos == '#'))
				++pos;

			if (pos != end)
				++pos;

			++current_level;
		}

		return current_level == level ? pos - begin : 0;
	}

	static UInt64 apply(const UInt64 level, const char * data, const std::size_t size)
	{
		return URLHashImpl::apply(data, findLevelLength(level, data, data + size));
	}
};


class FunctionURLHash : public IFunction
{
public:
	static constexpr auto name = "URLHash";
	static IFunction * create(const Context &) { return new FunctionURLHash; }

	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		const auto arg_count = arguments.size();
		if (arg_count != 1 && arg_count != 2)
			throw Exception{
				"Number of arguments for function " + getName() + " doesn't match: passed " +
					toString(arg_count) + ", should be 1 or 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
			};

		const auto first_arg = arguments.front().get();
		if (!typeid_cast<const DataTypeString *>(first_arg))
			throw Exception{
				"Illegal type " + first_arg->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
			};

		if (arg_count == 2)
		{
			const auto second_arg = arguments.back().get();
			if (!typeid_cast<const DataTypeUInt8 *>(second_arg) &&
				!typeid_cast<const DataTypeUInt16 *>(second_arg) &&
				!typeid_cast<const DataTypeUInt32 *>(second_arg) &&
				!typeid_cast<const DataTypeUInt64 *>(second_arg) &&
				!typeid_cast<const DataTypeInt8 *>(second_arg) &&
				!typeid_cast<const DataTypeInt16 *>(second_arg) &&
				!typeid_cast<const DataTypeInt32 *>(second_arg) &&
				!typeid_cast<const DataTypeInt64 *>(second_arg))
				throw Exception{
					"Illegal type " + second_arg->getName() + " of argument of function " + getName(),
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
				};
		}

		return new DataTypeUInt64;
	}

	void execute(Block & block, const ColumnNumbers & arguments, const size_t result)
	{
		const auto arg_count = arguments.size();

		if (arg_count == 1)
			executeSingleArg(block, arguments, result);
		else if (arg_count == 2)
			executeTwoArgs(block, arguments, result);
		else
			throw std::logic_error{"got into IFunction::execute with unexpected number of arguments"};
	}

private:
	void executeSingleArg(Block & block, const ColumnNumbers & arguments, const std::size_t result) const
	{
		const auto col_untyped = block.getByPosition(arguments.front()).column.get();

		if (const auto col_from = typeid_cast<const ColumnString *>(col_untyped))
		{
			const auto size = col_from->size();
			const auto col_to = new ColumnVector<UInt64>{size};
			block.getByPosition(result).column = col_to;

			const auto & chars = col_from->getChars();
			const auto & offsets = col_from->getOffsets();
			auto & out = col_to->getData();

			for (const auto i : ext::range(0, size))
				out[i] = URLHashImpl::apply(
					reinterpret_cast<const char *>(&chars[i == 0 ? 0 : offsets[i - 1]]),
					i == 0 ? offsets[i] - 1 : (offsets[i] - 1 - offsets[i - 1]));
		}
		else if (const auto col_from = typeid_cast<const ColumnConstString *>(col_untyped))
		{
			block.getByPosition(result).column = new ColumnConstUInt64{
				col_from->size(),
				URLHashImpl::apply(col_from->getData().data(), col_from->getData().size())
			};
		}
		else
			throw Exception{
				"Illegal column " + block.getByPosition(arguments[0]).column->getName() +
				" of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN
			};
	}

	void executeTwoArgs(Block & block, const ColumnNumbers & arguments, const std::size_t result) const
	{
		const auto level_col = block.getByPosition(arguments.back()).column.get();
		if (!level_col->isConst())
			throw Exception{
				"Second argument of function " + getName() + " must be an integral constant",
				ErrorCodes::ILLEGAL_COLUMN
			};

		const auto level = level_col->get64(0);

		const auto col_untyped = block.getByPosition(arguments.front()).column.get();
		if (const auto col_from = typeid_cast<const ColumnString *>(col_untyped))
		{
			const auto size = col_from->size();
			const auto col_to = new ColumnVector<UInt64>{size};
			block.getByPosition(result).column = col_to;

			const auto & chars = col_from->getChars();
			const auto & offsets = col_from->getOffsets();
			auto & out = col_to->getData();

			for (const auto i : ext::range(0, size))
				out[i] = URLHierarchyHashImpl::apply(level,
					reinterpret_cast<const char *>(&chars[i == 0 ? 0 : offsets[i - 1]]),
					i == 0 ? offsets[i] - 1 : (offsets[i] - 1 - offsets[i - 1]));
		}
		else if (const auto col_from = typeid_cast<const ColumnConstString *>(col_untyped))
		{
			block.getByPosition(result).column = new ColumnConstUInt64{
				col_from->size(),
				URLHierarchyHashImpl::apply(level, col_from->getData().data(), col_from->getData().size())
			};
		}
		else
			throw Exception{
				"Illegal column " + block.getByPosition(arguments[0]).column->getName() +
				" of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN
			};
	}
};


struct NameHalfMD5 			{ static constexpr auto name = "halfMD5"; };
struct NameSipHash64		{ static constexpr auto name = "sipHash64"; };
struct NameIntHash32 		{ static constexpr auto name = "intHash32"; };
struct NameIntHash64 		{ static constexpr auto name = "intHash64"; };

struct ImplCityHash64
{
	static constexpr auto name = "cityHash64";
	using uint128_t = uint128;

	static auto Hash128to64(const uint128_t & x) { return ::Hash128to64(x); }
	static auto Hash64(const char * const s, const std::size_t len) { return CityHash64(s, len); }
};

struct ImplFarmHash64
{
	static constexpr auto name = "farmHash64";
	using uint128_t = farmhash::uint128_t;

	static auto Hash128to64(const uint128_t & x) { return farmhash::Hash128to64(x); }
	static auto Hash64(const char * const s, const std::size_t len) { return farmhash::Hash64(s, len); }
};

struct ImplMetroHash64
{
	static constexpr auto name = "metroHash64";
	using uint128_t = uint128;

	static auto Hash128to64(const uint128_t & x) { return ::Hash128to64(x); }
	static auto Hash64(const char * const s, const std::size_t len)
	{
		union {
			std::uint64_t u64;
			std::uint8_t u8[sizeof(u64)];
		};

		metrohash64_1(reinterpret_cast<const std::uint8_t *>(s), len, 0, u8);

		return u64;
	}
};

using FunctionHalfMD5 = FunctionStringHash64<HalfMD5Impl, NameHalfMD5>;
using FunctionSipHash64 = FunctionStringHash64<SipHash64Impl, NameSipHash64>;
using FunctionIntHash32 = FunctionIntHash<IntHash32Impl, NameIntHash32>;
using FunctionIntHash64 = FunctionIntHash<IntHash64Impl, NameIntHash64>;
using FunctionMD5 = FunctionStringHashFixedString<MD5Impl>;
using FunctionSHA1 = FunctionStringHashFixedString<SHA1Impl>;
using FunctionSHA224 = FunctionStringHashFixedString<SHA224Impl>;
using FunctionSHA256 = FunctionStringHashFixedString<SHA256Impl>;
using FunctionSipHash128 = FunctionStringHashFixedString<SipHash128Impl>;
using FunctionCityHash64 = FunctionNeighbourhoodHash64<ImplCityHash64>;
using FunctionFarmHash64 = FunctionNeighbourhoodHash64<ImplFarmHash64>;
using FunctionMetroHash64 = FunctionNeighbourhoodHash64<ImplMetroHash64>;

}
