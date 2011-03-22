#ifndef DB_VARINT_H
#define DB_VARINT_H

#include <DB/Core/Types.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>


namespace DB
{


/** Записать UInt64 в формате переменной длины (base128) */
void writeVarUInt(UInt64 x, std::ostream & ostr);
void writeVarUInt(UInt64 x, WriteBuffer & ostr);
char * writeVarUInt(UInt64 x, char * ostr);


/** Прочитать UInt64, записанный в формате переменной длины (base128) */
void readVarUInt(UInt64 & x, std::istream & istr);
void readVarUInt(UInt64 & x, ReadBuffer & istr);
const char * readVarUInt(UInt64 & x, const char * istr, size_t size);


/** Получить длину UInt64 в формате VarUInt */
size_t getLengthOfVarUInt(UInt64 x);

/** Получить длину Int64 в формате VarInt */
size_t getLengthOfVarInt(Int64 x);


/** Записать Int64 в формате переменной длины (base128) */
template <typename OUT>
inline void writeVarInt(Int64 x, OUT & ostr)
{
	writeVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)), ostr);
}

inline char * writeVarInt(Int64 x, char * ostr)
{
	return writeVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)), ostr);
}


/** Прочитать Int64, записанный в формате переменной длины (base128) */
template <typename IN>
inline void readVarInt(Int64 & x, IN & istr)
{
	readVarUInt(*reinterpret_cast<UInt64*>(&x), istr);
	x = (static_cast<UInt64>(x) >> 1) ^ -(x & 1);
}

inline const char * readVarInt(Int64 & x, const char * istr, size_t size)
{
	const char * res = readVarUInt(*reinterpret_cast<UInt64*>(&x), istr, size);
	x = (static_cast<UInt64>(x) >> 1) ^ -(x & 1);
	return res;
}


inline void writeVarT(UInt64 x, std::ostream & ostr) { writeVarUInt(x, ostr); }
inline void writeVarT(Int64 x, std::ostream & ostr) { writeVarInt(x, ostr); }
inline void writeVarT(UInt64 x, WriteBuffer & ostr) { writeVarUInt(x, ostr); }
inline void writeVarT(Int64 x, WriteBuffer & ostr) { writeVarInt(x, ostr); }
inline char * writeVarT(UInt64 x, char * & ostr) { return writeVarUInt(x, ostr); }
inline char * writeVarT(Int64 x, char * & ostr) { return writeVarInt(x, ostr); }

inline void readVarT(UInt64 & x, std::istream & istr) { readVarUInt(x, istr); }
inline void readVarT(Int64 & x, std::istream & istr) { readVarInt(x, istr); }
inline void readVarT(UInt64 & x, ReadBuffer & istr) { readVarUInt(x, istr); }
inline void readVarT(Int64 & x, ReadBuffer & istr) { readVarInt(x, istr); }
inline const char * readVarT(UInt64 & x, const char * istr, size_t size) { return readVarUInt(x, istr, size); }
inline const char * readVarT(Int64 & x, const char * istr, size_t size) { return readVarInt(x, istr, size); }

}

#endif
