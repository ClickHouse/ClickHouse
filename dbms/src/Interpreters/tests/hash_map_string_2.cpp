#include <iostream>
#include <iomanip>
#include <vector>

#include <statdaemons/Stopwatch.h>

//#define DBMS_HASH_MAP_COUNT_COLLISIONS
#define DBMS_HASH_MAP_DEBUG_RESIZES

#include <DB/Core/Types.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/Core/StringRef.h>
#include <DB/Common/HashTable/HashMap.h>
#include <DB/Interpreters/AggregationCommon.h>


/** Выполнять так:
for file in SearchPhrase URL; do
	for size in 30000 100000 300000 1000000 5000000; do
		for method in {1..9}; do
			echo $file $size $method;
			for i in {0..100}; do
				./hash_map_string_2 $size $method < ${file}.bin 2>&1 |
					grep HashMap | grep -oE '[0-9\.]+ elem';
			done | awk -W interactive '{ if ($1 > x) { x = $1 }; printf(".") } END { print x }';
		done;
	done;
done
*/


#define DefineStringRef(STRUCT) \
\
struct STRUCT : public StringRef {}; \
\
namespace ZeroTraits \
{ \
	template <> \
	inline bool check<STRUCT>(STRUCT x) { return nullptr == x.data; } \
 \
	template <> \
	inline void set<STRUCT>(STRUCT & x) { x.data = nullptr; } \
}; \
 \
template <> \
struct DefaultHash<STRUCT> \
{ \
	size_t operator() (STRUCT x) const \
	{ \
		return CityHash64(x.data, x.size); \
	} \
};

DefineStringRef(StringRef_Compare1_Ptrs)
DefineStringRef(StringRef_Compare1_Index)
DefineStringRef(StringRef_CompareMemcmp)
DefineStringRef(StringRef_Compare8_1_byUInt64)
DefineStringRef(StringRef_Compare16_1_byMemcmp)
DefineStringRef(StringRef_Compare16_1_byUInt64_logicAnd)
DefineStringRef(StringRef_Compare16_1_byUInt64_bitAnd)
DefineStringRef(StringRef_Compare16_1_byIntSSE)
DefineStringRef(StringRef_Compare16_1_byFloatSSE)


inline bool operator==(StringRef_Compare1_Ptrs lhs, StringRef_Compare1_Ptrs rhs)
{
	if (lhs.size != rhs.size)
		return false;

	if (lhs.size == 0)
		return true;

	const char * pos1 = lhs.data;
	const char * pos2 = rhs.data;

	const char * end1 = pos1 + lhs.size;

	while (pos1 < end1)
	{
		if (*pos1 != *pos2)
			return false;

		++pos1;
		++pos2;
	}

	return true;
}

inline bool operator==(StringRef_Compare1_Index lhs, StringRef_Compare1_Index rhs)
{
	if (lhs.size != rhs.size)
		return false;

	if (lhs.size == 0)
		return true;

	for (size_t i = 0; i < lhs.size; ++i)
		if (lhs.data[i] != rhs.data[i])
			return false;

	return true;
}

inline bool operator==(StringRef_CompareMemcmp lhs, StringRef_CompareMemcmp rhs)
{
	if (lhs.size != rhs.size)
		return false;

	if (lhs.size == 0)
		return true;

	return 0 == memcmp(lhs.data, rhs.data, lhs.size);
}


inline bool operator==(StringRef_Compare8_1_byUInt64 lhs, StringRef_Compare8_1_byUInt64 rhs)
{
	if (lhs.size != rhs.size)
		return false;

	if (lhs.size == 0)
		return true;

	const char * p1 = lhs.data;
	const char * p2 = rhs.data;
	size_t size = lhs.size;

	const char * p1_end = p1 + size;
	const char * p1_end_8 = p1 + size / 8 * 8;

	while (p1 < p1_end_8)
	{
		if (reinterpret_cast<const uint64_t *>(p1)[0] != reinterpret_cast<const uint64_t *>(p2)[0])
			return false;

		p1 += 8;
		p2 += 8;
	}

	while (p1 < p1_end)
	{
		if (*p1 != *p2)
			return false;

		++p1;
		++p2;
	}

	return true;
}



inline bool compare_byMemcmp(const char * p1, const char * p2)
{
	return 0 == memcmp(p1, p2, 16);
}

inline bool compare_byUInt64_logicAnd(const char * p1, const char * p2)
{
	return reinterpret_cast<const uint64_t *>(p1)[0] == reinterpret_cast<const uint64_t *>(p2)[0]
		&& reinterpret_cast<const uint64_t *>(p1)[1] == reinterpret_cast<const uint64_t *>(p2)[1];
}

inline bool compare_byUInt64_bitAnd(const char * p1, const char * p2)
{
	return (reinterpret_cast<const uint64_t *>(p1)[0] == reinterpret_cast<const uint64_t *>(p2)[0])
		 & (reinterpret_cast<const uint64_t *>(p1)[1] == reinterpret_cast<const uint64_t *>(p2)[1]);
}

inline bool compare_byIntSSE(const char * p1, const char * p2)
{
	return 0xFFFF == _mm_movemask_epi8(_mm_cmpeq_epi8(
		_mm_loadu_si128(reinterpret_cast<const __m128i *>(p1)),
		_mm_loadu_si128(reinterpret_cast<const __m128i *>(p2))));
}

inline bool compare_byFloatSSE(const char * p1, const char * p2)
{
	return !_mm_movemask_ps(_mm_cmpneq_ps(
		_mm_loadu_ps(reinterpret_cast<const float *>(p1)),
		_mm_loadu_ps(reinterpret_cast<const float *>(p2))));
}


template <bool compare(const char *, const char *)>
inline bool memequal(const char * p1, const char * p2, size_t size)
{
	const char * p1_end = p1 + size;
	const char * p1_end_16 = p1 + size / 16 * 16;

	while (p1 < p1_end_16)
	{
		if (!compare(p1, p2))
			return false;

		p1 += 16;
		p2 += 16;
	}

	while (p1 < p1_end)
	{
		if (*p1 != *p2)
			return false;

		++p1;
		++p2;
	}

	return true;
}


#define Op(METHOD) \
inline bool operator==(StringRef_Compare16_1_ ## METHOD lhs, StringRef_Compare16_1_ ## METHOD rhs) \
{ \
	if (lhs.size != rhs.size) \
		return false; \
\
	if (lhs.size == 0) \
		return true; \
\
	return memequal<compare_  ## METHOD>(lhs.data, rhs.data, lhs.size); \
}

Op(byMemcmp)
Op(byUInt64_logicAnd)
Op(byUInt64_bitAnd)
Op(byIntSSE)
Op(byFloatSSE)


typedef UInt64 Value;


template <typename Key>
void NO_INLINE bench(const std::vector<StringRef> & data, const char * name)
{
	Stopwatch watch;

	typedef HashMapWithSavedHash<Key, Value, DefaultHash<Key>> Map;

	Map map;
	typename Map::iterator it;
	bool inserted;

	for (size_t i = 0, size = data.size(); i < size; ++i)
	{
		map.emplace(static_cast<const Key &>(data[i]), it, inserted);
		if (inserted)
			it->second = 0;
		++it->second;
	}

	watch.stop();
	std::cerr << std::fixed << std::setprecision(2)
		<< "HashMap (" << name << "). Size: " << map.size()
		<< ", elapsed: " << watch.elapsedSeconds()
		<< " (" << data.size() / watch.elapsedSeconds() << " elem/sec.)"
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
		<< ", collisions: " << map.getCollisions()
#endif
		<< std::endl;
}


int main(int argc, char ** argv)
{
	size_t n = atoi(argv[1]);
	size_t m = atoi(argv[2]);

	DB::Arena pool;
	std::vector<StringRef> data(n);

	std::cerr << "sizeof(Key) = " << sizeof(StringRef) << ", sizeof(Value) = " << sizeof(Value) << std::endl;

	{
		Stopwatch watch;
		DB::ReadBufferFromFileDescriptor in1(STDIN_FILENO);
		DB::CompressedReadBuffer in2(in1);

		std::string tmp;
		for (size_t i = 0; i < n && !in2.eof(); ++i)
		{
			DB::readStringBinary(tmp, in2);
			data[i] = StringRef(pool.insert(tmp.data(), tmp.size()), tmp.size());
		}

		watch.stop();
		std::cerr << std::fixed << std::setprecision(2)
			<< "Vector. Size: " << n
			<< ", elapsed: " << watch.elapsedSeconds()
			<< " (" << n / watch.elapsedSeconds() << " elem/sec.)"
			<< std::endl;
	}

	if (!m || m == 1) bench<StringRef_Compare1_Ptrs>				(data, "StringRef_Compare1_Ptrs");
	if (!m || m == 2) bench<StringRef_Compare1_Index>				(data, "StringRef_Compare1_Index");
	if (!m || m == 3) bench<StringRef_CompareMemcmp>				(data, "StringRef_CompareMemcmp");
	if (!m || m == 4) bench<StringRef_Compare8_1_byUInt64>			(data, "StringRef_Compare8_1_byUInt64");
	if (!m || m == 5) bench<StringRef_Compare16_1_byMemcmp>			(data, "StringRef_Compare16_1_byMemcmp");
	if (!m || m == 6) bench<StringRef_Compare16_1_byUInt64_logicAnd>(data, "StringRef_Compare16_1_byUInt64_logicAnd");
	if (!m || m == 7) bench<StringRef_Compare16_1_byUInt64_bitAnd>	(data, "StringRef_Compare16_1_byUInt64_bitAnd");
	if (!m || m == 8) bench<StringRef_Compare16_1_byIntSSE>			(data, "StringRef_Compare16_1_byIntSSE");
	if (!m || m == 9) bench<StringRef_Compare16_1_byFloatSSE>		(data, "StringRef_Compare16_1_byFloatSSE");

	return 0;
}
