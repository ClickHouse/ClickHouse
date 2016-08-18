#pragma once

#include <DB/IO/ReadBufferFromFileBase.h>
#include <string>
#include <memory>


namespace DB
{

/** Создать объект для чтения данных из файла.
  * estimated_size - количество байтов, которые надо читать
  * aio_threshold - минимальное количество байт для асинхронных операций чтения
  *
  * Если aio_threshold = 0 или estimated_size < aio_threshold, операции чтения выполняются синхронно.
  * В противном случае операции чтения выполняются асинхронно.
  */
std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBase(const std::string & filename_,
		size_t estimated_size,
		size_t aio_threshold,
		size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
		int flags_ = -1,
		char * existing_memory_ = nullptr,
		size_t alignment = 0);

}
