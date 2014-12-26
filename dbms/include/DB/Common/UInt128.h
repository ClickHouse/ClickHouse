#include <DB/Common/HashTable/Hash.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{

/// Для агрегации по SipHash или конкатенации нескольких полей.
struct UInt128
{
	UInt64 first;
	UInt64 second;

	bool operator== (const UInt128 rhs) const { return first == rhs.first && second == rhs.second; }
	bool operator!= (const UInt128 rhs) const { return first != rhs.first || second != rhs.second; }

	bool operator== (const UInt64 rhs) const { return first == rhs && second == 0; }
	bool operator!= (const UInt64 rhs) const { return first != rhs || second != 0; }

	UInt128 & operator= (const UInt64 rhs) { first = rhs; second = 0; return *this; }
};

struct UInt128Hash
{
	DefaultHash<UInt64> hash64;
	size_t operator()(UInt128 x) const { return hash64(hash64(x.first) ^ x.second); }
};

struct UInt128HashCRC32
{
	size_t operator()(UInt128 x) const
	{
		UInt64 crc = -1ULL;
		asm("crc32q %[x], %[crc]\n" : [crc] "+r" (crc) : [x] "rm" (x.first));
		asm("crc32q %[x], %[crc]\n" : [crc] "+r" (crc) : [x] "rm" (x.second));
		return crc;
	}
};

struct UInt128TrivialHash
{
	size_t operator()(UInt128 x) const { return x.first; }
};

inline void readBinary(UInt128 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void writeBinary(const UInt128 & x, WriteBuffer & buf) { writePODBinary(x, buf); }

}
