#pragma once

#include <DB/IO/ReadBufferFromFileBase.h>
#include <string>
#include <sys/stat.h>

namespace DB
{

ReadBufferFromFileBase * createReadBufferFromFileBase(const std::string & filename_,
		size_t aio_threshold,
		size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
		int flags_ = -1,
		char * existing_memory_ = nullptr,
		size_t alignment = 0);

}
