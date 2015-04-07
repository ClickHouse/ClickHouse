#pragma once

#include <DB/IO/WriteBufferFromFileBase.h>
#include <string>
#include <sys/stat.h>

namespace DB
{

WriteBufferFromFileBase * createWriteBufferFromFileBase(const std::string & filename_,
		size_t estimated_size,
		size_t aio_threshold,
		size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
		int flags_ = -1,
		mode_t mode = 0666,
		char * existing_memory_ = nullptr,
		size_t alignment = 0);

}
