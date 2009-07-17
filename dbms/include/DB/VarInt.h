#ifndef DB_VARINT_H
#define DB_VARINT_H

#include <DB/Field.h>


namespace DB
{


/** Записать UInt64 в формате переменной длины (base128) */
void writeVarUInt(UInt x, std::ostream & ostr);


/** Прочитать UInt64, записанный в формате переменной длины (base128) */
void readVarUInt(UInt & x, std::istream & istr);


/** Записать Int64 в формате переменной длины (base128) */
inline void writeVarInt(Int x, std::ostream & ostr)
{
	writeVarUInt(static_cast<UInt>((x << 1) ^ (x >> 63)), ostr);
}


/** Прочитать Int64, записанный в формате переменной длины (base128) */
inline void readVarInt(Int & x, std::istream & istr)
{
	readVarUInt(reinterpret_cast<UInt&>(x), istr);
	x = ((x >> 1) ^ (x << 63));
}


}

#endif
