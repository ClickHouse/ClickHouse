#pragma once

#include <atomic>

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>


namespace DB
{

/** Копирует данные из ReadBuffer в WriteBuffer, все что есть.
  */
void copyData(ReadBuffer & from, WriteBuffer & to);

/** Копирует bytes байт из ReadBuffer в WriteBuffer. Если нет bytes байт, то кидает исключение.
  */
void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes);

/** То же самое, с условием на остановку.
  */
void copyData(ReadBuffer & from, WriteBuffer & to, std::atomic<bool> & is_cancelled);
void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes, std::atomic<bool> & is_cancelled);


}
