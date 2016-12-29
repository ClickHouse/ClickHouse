#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Functions/IFunction.h>
#include <DB/Common/HashTable/Hash.h>
#include <DB/Common/randomSeed.h>


namespace DB
{

/** Функции генерации псевдослучайных чисел.
  * Функция может быть вызвана без аргументов или с одним аргументом.
  * Аргумент игнорируется и служит лишь для того, чтобы несколько вызовов одной функции считались разными и не склеивались.
  *
  * Пример:
  * SELECT rand(), rand() - выдаст два одинаковых столбца.
  * SELECT rand(1), rand(2) - выдаст два разных столбца.
  *
  * Некриптографические генераторы:
  *
  * rand   - linear congruental generator 0 .. 2^32 - 1.
  * rand64 - комбинирует несколько значений rand, чтобы получить значения из диапазона 0 .. 2^64 - 1.
  *
  * randConstant - служебная функция, выдаёт константный столбец со случайным значением.
  *
  * В качестве затравки используют время.
  * Замечание: переинициализируется на каждый блок.
  * Это значит, что таймер должен быть достаточного разрешения, чтобы выдавать разные значения на каждый блок.
  */

namespace detail
{
	/// NOTE Probably
	///    http://www.pcg-random.org/
	/// or http://www.math.sci.hiroshima-u.ac.jp/~m-mat/MT/SFMT/
	/// or http://docs.yeppp.info/c/group__yep_random___w_e_l_l1024a.html
	/// could go better.

	struct LinearCongruentialGenerator
	{
		/// Константы из man lrand48_r.
		static constexpr UInt64 a = 0x5DEECE66D;
		static constexpr UInt64 c = 0xB;

		/// А эта - из head -c8 /dev/urandom | xxd -p
		UInt64 current = 0x09826f4a081cee35ULL;

		LinearCongruentialGenerator() {}
		LinearCongruentialGenerator(UInt64 value) : current(value) {}

		void seed(UInt64 value)
		{
			current = value;
		}

		UInt32 next()
		{
			current = current * a + c;
			return current >> 16;
		}
	};

	void seed(LinearCongruentialGenerator & generator, intptr_t additional_seed)
	{
		generator.seed(intHash64(randomSeed() ^ intHash64(additional_seed)));
	}
}

struct RandImpl
{
	using ReturnType = UInt32;

	static void execute(PaddedPODArray<ReturnType> & res)
	{
		detail::LinearCongruentialGenerator generator0;
		detail::LinearCongruentialGenerator generator1;
		detail::LinearCongruentialGenerator generator2;
		detail::LinearCongruentialGenerator generator3;

		detail::seed(generator0, 0xfb4121280b2ab902ULL + reinterpret_cast<intptr_t>(&res[0]));
		detail::seed(generator1, 0x0121cf76df39c673ULL + reinterpret_cast<intptr_t>(&res[0]));
		detail::seed(generator2, 0x17ae86e3a19a602fULL + reinterpret_cast<intptr_t>(&res[0]));
		detail::seed(generator3, 0x8b6e16da7e06d622ULL + reinterpret_cast<intptr_t>(&res[0]));

		size_t size = res.size();
		ReturnType * pos = &res[0];
		ReturnType * end = pos + size;
		ReturnType * end4 = pos + size / 4 * 4;

		while (pos < end4)
		{
			pos[0] = generator0.next();
			pos[1] = generator1.next();
			pos[2] = generator2.next();
			pos[3] = generator3.next();
			pos += 4;
		}

		while (pos < end)
		{
			pos[0] = generator0.next();
			++pos;
		}
	}
};

struct Rand64Impl
{
	using ReturnType = UInt64;

	static void execute(PaddedPODArray<ReturnType> & res)
	{
		detail::LinearCongruentialGenerator generator0;
		detail::LinearCongruentialGenerator generator1;
		detail::LinearCongruentialGenerator generator2;
		detail::LinearCongruentialGenerator generator3;

		detail::seed(generator0, 0xfb4121280b2ab902ULL + reinterpret_cast<intptr_t>(&res[0]));
		detail::seed(generator1, 0x0121cf76df39c673ULL + reinterpret_cast<intptr_t>(&res[0]));
		detail::seed(generator2, 0x17ae86e3a19a602fULL + reinterpret_cast<intptr_t>(&res[0]));
		detail::seed(generator3, 0x8b6e16da7e06d622ULL + reinterpret_cast<intptr_t>(&res[0]));

		size_t size = res.size();
		ReturnType * pos = &res[0];
		ReturnType * end = pos + size;
		ReturnType * end2 = pos + size / 2 * 2;

		while (pos < end2)
		{
			pos[0] = (static_cast<UInt64>(generator0.next()) << 32) | generator1.next();
			pos[1] = (static_cast<UInt64>(generator2.next()) << 32) | generator3.next();
			pos += 2;
		}

		while (pos < end)
		{
			pos[0] = (static_cast<UInt64>(generator0.next()) << 32) | generator1.next();
			++pos;
		}
	}
};


template <typename Impl, typename Name>
class FunctionRandom : public IFunction
{
private:
	using ToType = typename Impl::ReturnType;

public:
	static constexpr auto name = Name::name;
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionRandom>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	bool isVariadic() const override { return true; }
	size_t getNumberOfArguments() const override { return 0; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		if (arguments.size() > 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 0 or 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return std::make_shared<typename DataTypeFromFieldType<typename Impl::ReturnType>::Type>();
	}

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		auto col_to = std::make_shared<ColumnVector<ToType>>();
		block.getByPosition(result).column = col_to;

		typename ColumnVector<ToType>::Container_t & vec_to = col_to->getData();

		size_t size = block.rowsInFirstColumn();
		vec_to.resize(size);
		Impl::execute(vec_to);
	}
};


template <typename Impl, typename Name>
class FunctionRandomConstant : public IFunction
{
private:
	using ToType = typename Impl::ReturnType;

	/// Значение одно для разных блоков.
	bool is_initialized = false;
	ToType value;

public:
	static constexpr auto name = Name::name;
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionRandomConstant>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	bool isVariadic() const override { return true; }
	size_t getNumberOfArguments() const override { return 0; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		if (arguments.size() > 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 0 or 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return std::make_shared<typename DataTypeFromFieldType<typename Impl::ReturnType>::Type>();
	}

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		if (!is_initialized)
		{
			is_initialized = true;
			typename ColumnVector<ToType>::Container_t vec_to(1);
			Impl::execute(vec_to);
			value = vec_to[0];
		}

		block.getByPosition(result).column = std::make_shared<ColumnConst<ToType>>(block.rowsInFirstColumn(), value);
	}
};


struct NameRand 		{ static constexpr auto name = "rand"; };
struct NameRand64 		{ static constexpr auto name = "rand64"; };
struct NameRandConstant { static constexpr auto name = "randConstant"; };

using FunctionRand = FunctionRandom<RandImpl,	NameRand> ;
using FunctionRand64 = FunctionRandom<Rand64Impl,	NameRand64>;
using FunctionRandConstant = FunctionRandomConstant<RandImpl, NameRandConstant>;


}
