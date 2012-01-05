#pragma once

/** Общие дефайны */

#define DBMS_MAX_COMPRESSED_SIZE 0x40000000ULL	/// 1GB

#define QUICKLZ_ADDITIONAL_SPACE 400
#define QUICKLZ_HEADER_SIZE 9

#define LZ4_ADDITIONAL_SPACE_MIN 8.0
#define LZ4_ADDITIONAL_SPACE_K 0.004


namespace DB
{

namespace CompressionMethod
{
	/** Метод сжатия */	
	enum Enum
	{
		QuickLZ = 0,
		LZ4		= 1,
	};
}

}
