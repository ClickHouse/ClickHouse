#pragma once

#include <IO/WriteBufferFromFileBase.h>
#include <string>

namespace DB
{

/** Создать объект для записи данных в файл.
  * estimated_size - количество байтов, которые надо записать
  * aio_threshold - минимальное количество байт для асинхронных операций записи
  *
  * Если aio_threshold = 0 или estimated_size < aio_threshold, операции записи выполняются синхронно.
  * В противном случае операции записи выполняются асинхронно.
  */
WriteBufferFromFileBase * createWriteBufferFromFileBase(const std::string & filename_,
        size_t estimated_size,
        size_t aio_threshold,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        int flags_ = -1,
        mode_t mode = 0666,
        char * existing_memory_ = nullptr,
        size_t alignment = 0);

}
