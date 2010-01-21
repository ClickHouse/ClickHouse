#include <DB/VarInt.h>

#include <Poco/Types.h>


namespace DB
{


void writeVarUInt(UInt x, std::ostream & ostr)
{
	char buf[9];

	buf[0] = static_cast<Poco::UInt8>(x | 0x80);
	if (x >= (1ULL << 7))
	{
		buf[1] = static_cast<Poco::UInt8>((x >> 7) | 0x80);
		if (x >= (1ULL << 14))
		{
			buf[2] = static_cast<Poco::UInt8>((x >> 14) | 0x80);
			if (x >= (1ULL << 21))
			{
				buf[3] = static_cast<Poco::UInt8>((x >> 21) | 0x80);
				if (x >= (1ULL << 28))
				{
					buf[4] = static_cast<Poco::UInt8>((x >> 28) | 0x80);
					if (x >= (1ULL << 35))
					{
						buf[5] = static_cast<Poco::UInt8>((x >> 35) | 0x80);
						if (x >= (1ULL << 42))
						{
							buf[6] = static_cast<Poco::UInt8>((x >> 42) | 0x80);
							if (x >= (1ULL << 49))
							{
								buf[7] = static_cast<Poco::UInt8>((x >> 49) | 0x80);
								if (x >= (1ULL << 56))
								{
									buf[8] = static_cast<Poco::UInt8>((x >> 56) | 0x80);
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


void readVarUInt(UInt & x, std::istream & istr)
{
	int byte;
	
	byte = istr.get();
	x = static_cast<Poco::UInt64>(byte) & 0x7F;
	if (byte & 0x80)
	{
		byte = istr.get();
		x |= (static_cast<Poco::UInt64>(byte) & 0x7F) << 7;
		if (byte & 0x80)
		{
			byte = istr.get();
			x |= (static_cast<Poco::UInt64>(byte) & 0x7F) << 14;
			if (byte & 0x80)
			{
				byte = istr.get();
				x |= (static_cast<Poco::UInt64>(byte) & 0x7F) << 21;
				if (byte & 0x80)
				{
					byte = istr.get();
					x |= (static_cast<Poco::UInt64>(byte) & 0x7F) << 28;
					if (byte & 0x80)
					{
						byte = istr.get();
						x |= (static_cast<Poco::UInt64>(byte) & 0x7F) << 35;
						if (byte & 0x80)
						{
							byte = istr.get();
							x |= (static_cast<Poco::UInt64>(byte) & 0x7F) << 42;
							if (byte & 0x80)
							{
								byte = istr.get();
								x |= (static_cast<Poco::UInt64>(byte) & 0x7F) << 49;
								if (byte & 0x80)
								{
									byte = istr.get();
									x |= (static_cast<Poco::UInt64>(byte) & 0x7F) << 56;
								}
							}
						}
					}
				}
			}
		}
	}
}


size_t getLengthOfVarUInt(UInt x)
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


}
