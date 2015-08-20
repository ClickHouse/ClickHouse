#pragma once

#include <tuple>

#include <DB/Core/Types.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{

/** Засечка - позиция в сжатом файле. Сжатый файл состоит из уложенных подряд сжатых блоков.
  * Засечка представляют собой пару - смещение в файле до начала сжатого блока, смещение в разжатом блоке до начала данных.
  */
struct MarkInCompressedFile
{
	size_t offset_in_compressed_file;
	size_t offset_in_decompressed_block;

	bool operator==(const MarkInCompressedFile & rhs) const
	{
		return std::tie(offset_in_compressed_file, offset_in_decompressed_block)
			== std::tie(rhs.offset_in_compressed_file, rhs.offset_in_decompressed_block);
	}
	bool operator!=(const MarkInCompressedFile & rhs) const
	{
		return !(*this == rhs);
	}

	String toString() const
	{
		return "(" + DB::toString(offset_in_compressed_file) + "," + DB::toString(offset_in_decompressed_block) + ")";
	}
};

using MarksInCompressedFile = std::vector<MarkInCompressedFile>;

}
