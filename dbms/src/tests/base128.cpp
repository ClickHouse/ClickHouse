#include <iostream>
#include <fstream>
#include <sstream>

#include <Poco/Types.h>
#include <Poco/BinaryWriter.h>
#include <Poco/BinaryReader.h>
#include <Poco/Stopwatch.h>
#include <Poco/Exception.h>
#include <Poco/NumberFormatter.h>


void writeVarUInt(Poco::UInt64 x, std::ostream & ostr)
{
	static char buf[9];

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


void readVarUInt(Poco::UInt64 & x, std::istream & istr)
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


void readVarUInt_2(Poco::UInt64 & x, std::istream & istr)
{
	static char buf[9];
	istr.read(buf, 9);
	
	x = static_cast<Poco::UInt64>(buf[0]) & 0x7F;
	if (buf[0] & 0x80)
	{
		x |= (static_cast<Poco::UInt64>(buf[1]) & 0x7F) << 7;
		if (buf[1] & 0x80)
		{
			x |= (static_cast<Poco::UInt64>(buf[2]) & 0x7F) << 14;
			if (buf[2] & 0x80)
			{
				x |= (static_cast<Poco::UInt64>(buf[3]) & 0x7F) << 21;
				if (buf[3] & 0x80)
				{
					x |= (static_cast<Poco::UInt64>(buf[4]) & 0x7F) << 28;
					if (buf[4] & 0x80)
					{
						x |= (static_cast<Poco::UInt64>(buf[5]) & 0x7F) << 35;
						if (buf[5] & 0x80)
						{
							x |= (static_cast<Poco::UInt64>(buf[6]) & 0x7F) << 42;
							if (buf[6] & 0x80)
							{
								x |= (static_cast<Poco::UInt64>(buf[7]) & 0x7F) << 49;
								if (buf[7] & 0x80)
								{
									x |= (static_cast<Poco::UInt64>(buf[8]) & 0x7F) << 56;
								}
								else
								{
									istr.unget();
								}
							}
							else
							{
								istr.unget();
								istr.unget();
							}
						}
						else
						{
							istr.unget();
							istr.unget();
							istr.unget();
						}
					}
					else
					{
						istr.unget();
						istr.unget();
						istr.unget();
						istr.unget();
					}
				}
				else
				{
					istr.unget();
					istr.unget();
					istr.unget();
					istr.unget();
					istr.unget();
				}
			}
			else
			{
				istr.unget();
				istr.unget();
				istr.unget();
				istr.unget();
				istr.unget();
				istr.unget();
			}
		}
		else
		{
			istr.unget();
			istr.unget();
			istr.unget();
			istr.unget();
			istr.unget();
			istr.unget();
			istr.unget();
		}
	}
	else
	{
		istr.unget();
		istr.unget();
		istr.unget();
		istr.unget();
		istr.unget();
		istr.unget();
		istr.unget();
		istr.unget();
	}
}


void writeVarInt(Poco::Int64 x, std::ostream & ostr)
{
	writeVarUInt(static_cast<Poco::UInt64>((x << 1) ^ (x >> 63)), ostr);
}


void readVarInt(Poco::Int64 & x, std::istream & istr)
{
	readVarUInt_2(reinterpret_cast<Poco::UInt64&>(x), istr);
	x = ((x >> 1) ^ (x << 63));
}


int main(int argc, char ** argv)
{
	try
	{
		Poco::Stopwatch stopwatch;
		Poco::Timestamp::TimeDiff elapsed;
		Poco::Int64 start = 0;//1234567890123456789LL;

		{
			std::ofstream ostr("tmp1");
			
			stopwatch.restart();
			for (Poco::Int64 i = start; i < start + 10000000; i++)
				writeVarInt(i, ostr);
			stopwatch.stop();
			elapsed = stopwatch.elapsed();
			std::cout << "writeVarInt: " << static_cast<double>(elapsed) / 1000000 << std::endl;
		}

		{
			std::ifstream ostr("tmp1");
			Poco::Int64 x;
			
			stopwatch.restart();
			for (Poco::Int64 i = start; i < start + 10000000; i++)
			{
				readVarInt(x, ostr);
				if (x != i)
					throw Poco::Exception(Poco::NumberFormatter::format(i));
			}
			stopwatch.stop();
			elapsed = stopwatch.elapsed();
			std::cout << "readVarInt: " << static_cast<double>(elapsed) / 1000000 << std::endl;
		}
		
		{
			std::ofstream ostr("tmp3");
			Poco::BinaryWriter writer(ostr);
			
			stopwatch.restart();
			for (Poco::Int64 i = start; i < start + 10000000; i++)
				writer << i;
			stopwatch.stop();
			elapsed = stopwatch.elapsed();
			std::cout << "BinaryWriter: " << static_cast<double>(elapsed) / 1000000 << std::endl;
		}
		
		{
			std::ofstream ostr("tmp4");
			
			stopwatch.restart();
			for (Poco::Int64 i = start; i < start + 10000000; i++)
				ostr.write(reinterpret_cast<char*>(&i), 8);
			stopwatch.stop();
			elapsed = stopwatch.elapsed();
			std::cout << "ostream::write: " << static_cast<double>(elapsed) / 1000000 << std::endl;
		}
	}
	catch (const Poco::Exception & e)
	{
		std::cout << e.message() << std::endl;
		throw;
	}
	
	return 0;
}
