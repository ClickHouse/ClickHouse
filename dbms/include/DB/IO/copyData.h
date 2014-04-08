#pragma once

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>


namespace DB
{

/** Копирует данные из ReadBuffer в WriteBuffer
  */
void copyData(ReadBuffer & from, WriteBuffer & to);

/** Копирует bytes байт из ReadBuffer в WriteBuffer
  */
void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes);

}
