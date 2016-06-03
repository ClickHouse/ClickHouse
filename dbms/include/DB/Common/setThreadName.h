#pragma once

/** Устанавливает имя потока (максимальная длина - 15 байт),
  *  которое будет видно в ps, gdb, /proc,
  *  для удобства наблюдений и отладки.
  */
void setThreadName(const char * name);
