#pragma once

#include <time.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Functions/IFunction.h>
#include <DB/Common/HashTable/Hash.h>
#include <stats/IntHash.h>


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
  * В качестве затравки используют время.
  * Замечание: переинициализируется на каждый блок.
  * Это значит, что таймер должен быть достаточного разрешения, чтобы выдавать разные значения на каждый блок.
  */

namespace detail
{
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
		struct timespec times;
		if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &times))
			throwFromErrno("Cannot clock_gettime.", ErrorCodes::CANNOT_CLOCK_GETTIME);

		generator.seed(intHash64(times.tv_nsec ^ intHash64(additional_seed)));
	}
}

struct RandImpl
{
	typedef UInt32 ReturnType;

	static void execute(PODArray<ReturnType> & res)
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
	typedef UInt64 ReturnType;

	static void execute(PODArray<ReturnType> & res)
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
	typedef typename Impl::ReturnType ToType;

public:
	static constexpr auto name = Name::name;
	static IFunction * create(const Context & context) { return new FunctionRandom; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() > 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 0 or 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		return new typename DataTypeFromFieldType<typename Impl::ReturnType>::Type;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		ColumnVector<ToType> * col_to = new ColumnVector<ToType>;
		block.getByPosition(result).column = col_to;

		typename ColumnVector<ToType>::Container_t & vec_to = col_to->getData();

		size_t size = block.rowsInFirstColumn();
		vec_to.resize(size);
		Impl::execute(vec_to);
	}
};


struct NameRand 	{ static constexpr auto name = "rand"; };
struct NameRand64 	{ static constexpr auto name = "rand64"; };

typedef FunctionRandom<RandImpl,	NameRand> 	FunctionRand;
typedef FunctionRandom<Rand64Impl,	NameRand64> FunctionRand64;


}
