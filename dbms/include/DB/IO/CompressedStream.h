#pragma once

/** Общие дефайны */

#define DBMS_MAX_COMPRESSED_SIZE 0x40000000ULL	/// 1GB

#define QUICKLZ_ADDITIONAL_SPACE 400
#define QUICKLZ_HEADER_SIZE 9


namespace DB
{

namespace CompressionMethod
{
	/** Метод сжатия */
	enum Enum
	{
		QuickLZ,
		LZ4,
		LZ4HC,		/// Формат такой же, как у LZ4. Разница только при сжатии.
	};
}

}
