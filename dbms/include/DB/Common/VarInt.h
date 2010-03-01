#ifndef DB_VARINT_H
#define DB_VARINT_H

#include <DB/Core/Types.h>


namespace DB
{


/** Записать UInt64 в формате переменной длины (base128) */
void writeVarUInt(UInt64 x, std::ostream & ostr);


/** Прочитать UInt64, записанный в формате переменной длины (base128) */
void readVarUInt(UInt64 & x, std::istream & istr);


/** Получить длину UInt64 в формате VarUInt */
size_t getLengthOfVarUInt(UInt64 x);


/** Записать Int64 в формате переменной длины (base128) */
inline void writeVarInt(Int64 x, std::ostream & ostr)
{
	writeVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)), ostr);
}


/** Прочитать Int64, записанный в формате переменной длины (base128) */
inline void readVarInt(Int64 & x, std::istream & istr)
{
	readVarUInt(*reinterpret_cast<UInt64*>(&x), istr);
	x = (static_cast<UInt64>(x) >> 1) ^ -(x & 1);
}


}

#endif
