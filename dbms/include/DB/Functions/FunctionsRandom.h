#pragma once

#include <time.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Functions/IFunction.h>
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
  * rand   - linear congruental generator 0 .. 2^31 - 1.
  * rand64 - комбинирует несколько значений rand, чтобы получить значения из диапазона 0 .. 2^64 - 1.
  *
  * В качестве затравки используют время.
  * Замечание: переинициализируется на каждый блок.
  * Это значит, что таймер должен быть достаточного разрешения, чтобы выдавать разные значения на каждый блок.
  */

namespace detail
{
	void seed(drand48_data & rand_state, intptr_t additional_seed)
	{
		struct timespec times;
		if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &times))
			throwFromErrno("Cannot clock_gettime.", ErrorCodes::CANNOT_CLOCK_GETTIME);

		srand48_r(intHash32<0>(times.tv_nsec ^ intHash32<0>(additional_seed)), &rand_state);
	}
}

struct RandImpl
{
	typedef UInt32 ReturnType;
	
	static void execute(PODArray<ReturnType> & res)
	{
		drand48_data rand_state;
		detail::seed(rand_state, reinterpret_cast<intptr_t>(&res[0]));
		
		size_t size = res.size();
		for (size_t i = 0; i < size; ++i)
		{
			long rand_res;
			lrand48_r(&rand_state, &rand_res);
			res[i] = rand_res;
		}
	}
};

struct Rand64Impl
{
	typedef UInt64 ReturnType;

	static void execute(PODArray<ReturnType> & res)
	{
		drand48_data rand_state;
		detail::seed(rand_state, reinterpret_cast<intptr_t>(&res[0]));

		size_t size = res.size();
		for (size_t i = 0; i < size; ++i)
		{
			long rand_res1;
			long rand_res2;
			long rand_res3;
			
			lrand48_r(&rand_state, &rand_res1);
			lrand48_r(&rand_state, &rand_res2);
			lrand48_r(&rand_state, &rand_res3);
			
			res[i] = rand_res1 ^ (rand_res2 << 18) ^ (rand_res3 << 33);
		}
	}
};


template <typename Impl, typename Name>
class FunctionRandom : public IFunction
{
private:
	typedef typename Impl::ReturnType ToType;
	
public:
	/// Получить имя функции.
	String getName() const
	{
		return Name::get();
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


struct NameRand 	{ static const char * get() { return "rand"; } };
struct NameRand64 	{ static const char * get() { return "rand64"; } };

typedef FunctionRandom<RandImpl,	NameRand> 	FunctionRand;
typedef FunctionRandom<Rand64Impl,	NameRand64> FunctionRand64;


}
