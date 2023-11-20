//
// CompressedLogFile.cpp
//
// Library: Foundation
// Package: Logging
// Module:  CompressedLogFile
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/CompressedLogFile.h"

namespace Poco {


CompressedLogFile::CompressedLogFile(const std::string& path): 
	LogFile(path + ".lz4"),
	_buffer(0)
{
	memset(&_kPrefs, 0, sizeof(_kPrefs));

	size_t ret = LZ4F_createCompressionContext(&_ctx, LZ4F_VERSION);

	if (LZ4F_isError(ret))
		throw IOException(LZ4F_getErrorName(ret));

	/// That should be enough
	_buffer.setCapacity(LZ4F_compressBound(16 * 1024, &_kPrefs));

	/// Write frame header and check for errors
	size_t header_size = LZ4F_compressBegin(_ctx, static_cast<void *>(_buffer.begin()), _buffer.capacity(), &_kPrefs);

	if (LZ4F_isError(header_size))
		throw IOException(LZ4F_getErrorName(header_size));

	writeBinary(_buffer.begin(), header_size, true);
}

CompressedLogFile::~CompressedLogFile()
{
	/// Compression end
	size_t end_size = LZ4F_compressEnd(_ctx, static_cast<void *>(_buffer.begin()), _buffer.capacity(), nullptr);

	if (!LZ4F_isError(end_size))
		writeBinary(_buffer.begin(), end_size, true);
	
	LZ4F_freeCompressionContext(_ctx);
}

void CompressedLogFile::write(const std::string& text, bool flush)
{
	size_t in_capacity = text.size();
	const char * in_data = text.data();

	while (in_capacity > 0)
	{
		/// LZ4F_compressUpdate compresses whole input buffer at once so we need to shink it manually
		size_t cur_buffer_size = in_capacity;

		while (_buffer.capacity() < LZ4F_compressBound(cur_buffer_size, &_kPrefs))
			cur_buffer_size /= 2;

		size_t compressed_size = LZ4F_compressUpdate(
			_ctx,
			static_cast<void *>(_buffer.begin()),
			_buffer.capacity(),
			static_cast<const void *>(in_data),
			cur_buffer_size,
			nullptr);

		if (LZ4F_isError(compressed_size))
			throw IOException(LZ4F_getErrorName(compressed_size));
		
		if (compressed_size > 0)
			writeBinary(_buffer.begin(), compressed_size, false);

		in_capacity -= cur_buffer_size;
		in_data += cur_buffer_size;
	}

	char newline = '\n';

	size_t compressed_size = LZ4F_compressUpdate(
		_ctx,
		static_cast<void *>(_buffer.begin()),
		_buffer.capacity(),
		&newline,
		sizeof(newline),
		nullptr);

	if (LZ4F_isError(compressed_size))
		throw IOException(LZ4F_getErrorName(compressed_size));
	
	if (compressed_size == 0 && flush)
	{
		compressed_size = LZ4F_flush(_ctx, static_cast<void *>(_buffer.begin()), _buffer.capacity(), nullptr);

		if (LZ4F_isError(compressed_size))
			throw IOException(LZ4F_getErrorName(compressed_size));
	}
		

	if (compressed_size > 0)
		writeBinary(_buffer.begin(), compressed_size, flush);
}


} // namespace Poco
