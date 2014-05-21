#include <sched.h>

#include <iostream>
#include <iomanip>

#include <DB/Common/HashTable/Hash.h>
#include <stats/IntHash.h>

#include <statdaemons/Stopwatch.h>

#include "AvalancheTest.h"	/// Взято из SMHasher.


void setAffinity()
{
	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET(0, &mask);

	if (-1 == sched_setaffinity(0, sizeof(mask), &mask))
		throw Poco::Exception("Cannot set CPU affinity");
}


static inline __attribute__((__always_inline__)) UInt64 rdtsc()
{
    UInt32 a, d;
    __asm__ volatile ("rdtsc" : "=a" (a), "=d" (d));
    return static_cast<UInt64>(a) | (static_cast<UInt64>(d) << 32);
}


static inline size_t identity(UInt64 x)
{
	return x;
}

static inline size_t intHash32(UInt64 x)
{
	x = (~x) + (x << 18);
	x = x ^ ((x >> 31) | (x << 33));
	x = x * 21;
	x = x ^ ((x >> 11) | (x << 53));
	x = x + (x << 6);
	x = x ^ ((x >> 22) | (x << 42));

	return x;
}

static inline size_t hash3(UInt64 x)
{
	x ^= x >> 23;
	x *= 0x2127599bf4325c37ULL;
	x ^= x >> 47;
	x *= 0xb492b66fbe98f273ULL;
	x ^= x >> 33;

	return x;
}

static inline size_t hash4(UInt64 x)
{
	UInt64 a = x;
	UInt64 b = x;

	a ^= a >> 23;
	b ^= b >> 15;

	a *= 0x2127599bf4325c37ULL;
	b *= 0xb492b66fbe98f273ULL;

	a ^= a >> 47;
	b ^= b >> 33;

	return a ^ b;
}

static inline size_t hash5(UInt64 x)
{
	x *= 0xb492b66fbe98f273ULL;
	x ^= x >> 23;
	x *= 0x2127599bf4325c37ULL;
	x ^= x >> 47;

	return x;
}

static inline size_t murmurMix(UInt64 x)
{
	x ^= x >> 33;
	x *= 0xff51afd7ed558ccdULL;
	x ^= x >> 33;
	x *= 0xc4ceb9fe1a85ec53ULL;
	x ^= x >> 33;

	return x;
}


const size_t BUF_SIZE = 1024;

typedef std::vector<UInt64> Source;


void report(const char * name, size_t n, double elapsed, UInt64 tsc_diff, size_t res)
{
	std::cerr << name << std::endl
		<< "Done in " << elapsed
		<< " (" << n / elapsed << " elem/sec."
		<< ", " << n * sizeof(UInt64) / elapsed / (1 << 30) << " GiB/sec."
		<< ", " << (tsc_diff * 1.0 / n) << " tick/elem)"
		<< "; res = " << res
		<< std::endl << std::endl;
}


template <size_t Func(UInt64)>
static inline void test(size_t n, const Source & data, const char * name)
{
	/// throughput. Вычисления хэш-функций от разных значений могут перекрываться.
	{
		Stopwatch watch;

		size_t res = 0;
		UInt64 tsc = rdtsc();

		for (size_t i = 0; i < n; ++i)
			res += Func(data[i & (BUF_SIZE - 1)]);

		UInt64 tsc_diff = rdtsc() - tsc;

		watch.stop();

		std::cerr << "Throughput of ";
		report(name, n, watch.elapsedSeconds(), tsc_diff, res);
	}

	/// latency. Чтобы вычислить следующее значение, надо сначала вычислить предыдущее. Добавляется latency L1-кэша.
	{
		Stopwatch watch;

		size_t res = 0;
		UInt64 tsc = rdtsc();

		size_t pos = 0;
		for (size_t i = 0; i < n; ++i)
		{
			res += Func(data[pos]);
			pos = res & (BUF_SIZE - 1);
		}

		UInt64 tsc_diff = rdtsc() - tsc;

		watch.stop();

		std::cerr << "Latency of ";
		report(name, n, watch.elapsedSeconds(), tsc_diff, res);
	}

	/// quality. Методы взяты из SMHasher.
	{
		auto wrapper = [](const void * blob, const int len, const uint32_t seed, void * out)
		{
			*reinterpret_cast<UInt64*>(out) = Func(*reinterpret_cast<const UInt64 *>(blob));
		};

		std::cerr << "Avalanche: " << std::endl;
		AvalancheTest<UInt64, UInt64>(wrapper, 300000);
	//	std::cerr << "Bit Independence Criteria: " << std::endl;
	//	BicTest3<UInt64, UInt64>(wrapper, 2000000);

		std::cerr << std::endl;
	}
}


int main(int argc, char ** argv)
{
	const size_t BUF_SIZE = 1024;

	size_t n = (atoi(argv[1]) + (BUF_SIZE - 1)) / BUF_SIZE * BUF_SIZE;
	size_t method = argc <= 2 ? 0 : atoi(argv[2]);

	std::cerr << std::fixed << std::setprecision(2);

	typedef std::vector<UInt64> Source;
	Source data(BUF_SIZE);

	{
		Stopwatch watch;

		srand48(rdtsc());
		for (size_t i = 0; i < BUF_SIZE; ++i)
			data[i] = lrand48();

		watch.stop();
		double elapsed = watch.elapsedSeconds();
		std::cerr << std::fixed << std::setprecision(2)
			<< "Fill. Size: " << BUF_SIZE
			<< ", elapsed: " << elapsed
			<< " (" << BUF_SIZE / elapsed << " elem/sec.)"
			<< std::endl << std::endl;
	}

	setAffinity();

	if (!method || method == 0) test<identity>		(n, data, "0: identity");
	if (!method || method == 1) test<intHash32>		(n, data, "1: intHash32");
	if (!method || method == 2) test<intHash64>		(n, data, "2: intHash64");
	if (!method || method == 3) test<hash3>			(n, data, "3: two rounds");
	if (!method || method == 4) test<hash4>			(n, data, "4: two rounds and two variables");
	if (!method || method == 5) test<hash5>			(n, data, "5: two rounds with less ops");
	if (!method || method == 6) test<murmurMix>		(n, data, "6: murmur64 mixer");

	return 0;
}
