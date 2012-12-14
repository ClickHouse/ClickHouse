#include <istream>
#include <ostream>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/IO/VarInt.h>

#include <Poco/Types.h>


namespace DB
{


void writeVarUInt(UInt64 x, std::ostream & ostr)
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


void readVarUInt(UInt64 & x, std::istream & istr)
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


void writeVarUInt(UInt64 x, WriteBuffer & ostr)
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


void readVarUInt(UInt64 & x, ReadBuffer & istr)
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


char * writeVarUInt(UInt64 x, char * ostr)
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


const char * readVarUInt(UInt64 & x, const char * istr, size_t size)
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


size_t getLengthOfVarUInt(UInt64 x)
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


size_t getLengthOfVarInt(Int64 x)
{
	return getLengthOfVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)));
}


}
