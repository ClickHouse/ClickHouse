#pragma once

#include <string>

/** Получить FQDN для локального сервера путём DNS-резолвинга hostname - аналогично вызову утилиты hostname с флагом -f.
  * Если не получилось отрезолвить, то вернуть hostname - аналогично вызову утилиты hostname без флагов или uname -n.
  */
const std::string & getFQDNOrHostName();
