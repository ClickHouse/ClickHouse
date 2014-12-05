#pragma once

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


/// Для [U]Int32, [U]Int16.

inline void readVarUInt(UInt32 & x, ReadBuffer & istr)
{
	UInt64 tmp;
	readVarUInt(tmp, istr);
	x = tmp;
}

inline void readVarInt(Int32 & x, ReadBuffer & istr)
{
	Int64 tmp;
	readVarInt(tmp, istr);
	x = tmp;
}

inline void readVarUInt(UInt16 & x, ReadBuffer & istr)
{
	UInt64 tmp;
	readVarUInt(tmp, istr);
	x = tmp;
}

inline void readVarInt(Int16 & x, ReadBuffer & istr)
{
	Int64 tmp;
	readVarInt(tmp, istr);
	x = tmp;
}


inline void writeVarUInt(UInt64 x, std::ostream & ostr)
{
	char buf[9];

	buf[0] = static_cast<UInt8>(x | 0x80);
	if (x >= (1ULL << 7))
	{
		buf[1] = static_cast<UInt8>((x >> 7) | 0x80);
		if (x >= (1ULL << 14))
		{
			buf[2] = static_cast<UInt8>((x >> 14) | 0x80);
			if (x >= (1ULL << 21))
			{
				buf[3] = static_cast<UInt8>((x >> 21) | 0x80);
				if (x >= (1ULL << 28))
				{
					buf[4] = static_cast<UInt8>((x >> 28) | 0x80);
					if (x >= (1ULL << 35))
					{
						buf[5] = static_cast<UInt8>((x >> 35) | 0x80);
						if (x >= (1ULL << 42))
						{
							buf[6] = static_cast<UInt8>((x >> 42) | 0x80);
							if (x >= (1ULL << 49))
							{
								buf[7] = static_cast<UInt8>((x >> 49) | 0x80);
								if (x >= (1ULL << 56))
								{
									buf[8] = static_cast<UInt8>(x >> 56);
									ostr.write(buf, 9);
								}
								else
								{
									buf[7] &= 0x7F;
									ostr.write(buf, 8);
								}
							}
							else
							{
								buf[6] &= 0x7F;
								ostr.write(buf, 7);
							}
						}
						else
						{
							buf[5] &= 0x7F;
							ostr.write(buf, 6);
						}
					}
					else
					{
						buf[4] &= 0x7F;
						ostr.write(buf, 5);
					}
				}
				else
				{
					buf[3] &= 0x7F;
					ostr.write(buf, 4);
				}
			}
			else
			{
				buf[2] &= 0x7F;
				ostr.write(buf, 3);
			}
		}
		else
		{
			buf[1] &= 0x7F;
			ostr.write(buf, 2);
		}
	}
	else
	{
		buf[0] &= 0x7F;
		ostr.write(buf, 1);
	}
}


inline void throwReadAfterEOF()
{
	throw Exception("Attempt to read after eof", ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
}

inline void readChar(char & x, ReadBuffer & buf)
{
	if (!buf.eof())
	{
		x = *buf.position();
		++buf.position();
	}
	else
		throwReadAfterEOF();
}


inline void readVarUInt(UInt64 & x, std::istream & istr)
{
	int byte;

	byte = istr.get();
	x = static_cast<UInt64>(byte) & 0x7F;
	if (byte & 0x80)
	{
		byte = istr.get();
		x |= (static_cast<UInt64>(byte) & 0x7F) << 7;
		if (byte & 0x80)
		{
			byte = istr.get();
			x |= (static_cast<UInt64>(byte) & 0x7F) << 14;
			if (byte & 0x80)
			{
				byte = istr.get();
				x |= (static_cast<UInt64>(byte) & 0x7F) << 21;
				if (byte & 0x80)
				{
					byte = istr.get();
					x |= (static_cast<UInt64>(byte) & 0x7F) << 28;
					if (byte & 0x80)
					{
						byte = istr.get();
						x |= (static_cast<UInt64>(byte) & 0x7F) << 35;
						if (byte & 0x80)
						{
							byte = istr.get();
							x |= (static_cast<UInt64>(byte) & 0x7F) << 42;
							if (byte & 0x80)
							{
								byte = istr.get();
								x |= (static_cast<UInt64>(byte) & 0x7F) << 49;
								if (byte & 0x80)
								{
									byte = istr.get();
									x |= static_cast<UInt64>(byte) << 56;
								}
							}
						}
					}
				}
			}
		}
	}
}


inline void writeVarUInt(UInt64 x, WriteBuffer & ostr)
{
	char buf[9];

	buf[0] = static_cast<UInt8>(x | 0x80);
	if (x >= (1ULL << 7))
	{
		buf[1] = static_cast<UInt8>((x >> 7) | 0x80);
		if (x >= (1ULL << 14))
		{
			buf[2] = static_cast<UInt8>((x >> 14) | 0x80);
			if (x >= (1ULL << 21))
			{
				buf[3] = static_cast<UInt8>((x >> 21) | 0x80);
				if (x >= (1ULL << 28))
				{
					buf[4] = static_cast<UInt8>((x >> 28) | 0x80);
					if (x >= (1ULL << 35))
					{
						buf[5] = static_cast<UInt8>((x >> 35) | 0x80);
						if (x >= (1ULL << 42))
						{
							buf[6] = static_cast<UInt8>((x >> 42) | 0x80);
							if (x >= (1ULL << 49))
							{
								buf[7] = static_cast<UInt8>((x >> 49) | 0x80);
								if (x >= (1ULL << 56))
								{
									buf[8] = static_cast<UInt8>(x >> 56);
									ostr.write(buf, 9);
								}
								else
								{
									buf[7] &= 0x7F;
									ostr.write(buf, 8);
								}
							}
							else
							{
								buf[6] &= 0x7F;
								ostr.write(buf, 7);
							}
						}
						else
						{
							buf[5] &= 0x7F;
							ostr.write(buf, 6);
						}
					}
					else
					{
						buf[4] &= 0x7F;
						ostr.write(buf, 5);
					}
				}
				else
				{
					buf[3] &= 0x7F;
					ostr.write(buf, 4);
				}
			}
			else
			{
				buf[2] &= 0x7F;
				ostr.write(buf, 3);
			}
		}
		else
		{
			buf[1] &= 0x7F;
			ostr.write(buf, 2);
		}
	}
	else
	{
		buf[0] &= 0x7F;
		ostr.write(buf, 1);
	}
}


inline void readVarUInt(UInt64 & x, ReadBuffer & istr)
{
	char byte;

	readChar(byte, istr);
	x = static_cast<UInt64>(byte) & 0x7F;
	if (byte & 0x80)
	{
		readChar(byte, istr);
		x |= (static_cast<UInt64>(byte) & 0x7F) << 7;
		if (byte & 0x80)
		{
			readChar(byte, istr);
			x |= (static_cast<UInt64>(byte) & 0x7F) << 14;
			if (byte & 0x80)
			{
				readChar(byte, istr);
				x |= (static_cast<UInt64>(byte) & 0x7F) << 21;
				if (byte & 0x80)
				{
					readChar(byte, istr);
					x |= (static_cast<UInt64>(byte) & 0x7F) << 28;
					if (byte & 0x80)
					{
						readChar(byte, istr);
						x |= (static_cast<UInt64>(byte) & 0x7F) << 35;
						if (byte & 0x80)
						{
							readChar(byte, istr);
							x |= (static_cast<UInt64>(byte) & 0x7F) << 42;
							if (byte & 0x80)
							{
								readChar(byte, istr);
								x |= (static_cast<UInt64>(byte) & 0x7F) << 49;
								if (byte & 0x80)
								{
									readChar(byte, istr);
									x |= static_cast<UInt64>(byte) << 56;
								}
							}
						}
					}
				}
			}
		}
	}
}


inline char * writeVarUInt(UInt64 x, char * ostr)
{
	*ostr = static_cast<UInt8>(x | 0x80);
	if (x >= (1ULL << 7))
	{
		*++ostr = static_cast<UInt8>((x >> 7) | 0x80);
		if (x >= (1ULL << 14))
		{
			*++ostr = static_cast<UInt8>((x >> 14) | 0x80);
			if (x >= (1ULL << 21))
			{
				*++ostr = static_cast<UInt8>((x >> 21) | 0x80);
				if (x >= (1ULL << 28))
				{
					*++ostr = static_cast<UInt8>((x >> 28) | 0x80);
					if (x >= (1ULL << 35))
					{
						*++ostr = static_cast<UInt8>((x >> 35) | 0x80);
						if (x >= (1ULL << 42))
						{
							*++ostr = static_cast<UInt8>((x >> 42) | 0x80);
							if (x >= (1ULL << 49))
							{
								*++ostr = static_cast<UInt8>((x >> 49) | 0x80);
								if (x >= (1ULL << 56))
								{
									*++ostr = static_cast<UInt8>(x >> 56);
								}
								else
									*ostr &= 0x7F;
							}
							else
								*ostr &= 0x7F;
						}
						else
							*ostr &= 0x7F;
					}
					else
						*ostr &= 0x7F;
				}
				else
					*ostr &= 0x7F;
			}
			else
				*ostr &= 0x7F;
		}
		else
			*ostr &= 0x7F;
	}
	else
		*ostr &= 0x7F;

	return ++ostr;
}


inline const char * readVarUInt(UInt64 & x, const char * istr, size_t size)
{
	const char * end = istr + size;

	x = static_cast<UInt64>(*istr) & 0x7F;
	if (*istr & 0x80 && ++istr < end)
	{
		x |= (static_cast<UInt64>(*istr) & 0x7F) << 7;
		if (*istr & 0x80 && ++istr < end)
		{
			x |= (static_cast<UInt64>(*istr) & 0x7F) << 14;
			if (*istr & 0x80 && ++istr < end)
			{
				x |= (static_cast<UInt64>(*istr) & 0x7F) << 21;
				if (*istr & 0x80 && ++istr < end)
				{
					x |= (static_cast<UInt64>(*istr) & 0x7F) << 28;
					if (*istr & 0x80 && ++istr < end)
					{
						x |= (static_cast<UInt64>(*istr) & 0x7F) << 35;
						if (*istr & 0x80 && ++istr < end)
						{
							x |= (static_cast<UInt64>(*istr) & 0x7F) << 42;
							if (*istr & 0x80 && ++istr < end)
							{
								x |= (static_cast<UInt64>(*istr) & 0x7F) << 49;
								if (*istr & 0x80 && ++istr < end)
								{
									x |= static_cast<UInt64>(*istr) << 56;
								}
							}
						}
					}
				}
			}
		}
	}

	return ++istr;
}


inline size_t getLengthOfVarUInt(UInt64 x)
{
	return x < (1ULL << 7) ? 1
		: (x < (1ULL << 14) ? 2
		: (x < (1ULL << 21) ? 3
		: (x < (1ULL << 28) ? 4
		: (x < (1ULL << 35) ? 5
		: (x < (1ULL << 42) ? 6
		: (x < (1ULL << 49) ? 7
		: (x < (1ULL << 56) ? 8
		: 9)))))));
}


inline size_t getLengthOfVarInt(Int64 x)
{
	return getLengthOfVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)));
}

}
