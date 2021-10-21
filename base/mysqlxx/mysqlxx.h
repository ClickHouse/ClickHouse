#pragma once

#include <mysqlxx/Connection.h>
#include <mysqlxx/Transaction.h>
#include <mysqlxx/Pool.h>
#include <common/LocalDate.h>
#include <common/LocalDateTime.h>
#include <mysqlxx/Null.h>


/** 'mysqlxx' - very simple library for replacement of 'mysql++' library.
  *
  * For whatever reason, in Yandex.Metrica, back in 2008, 'mysql++' library was used.
  * There are the following shortcomings of 'mysql++':
  * 1. Too rich functionality: most of it is not used.
  * 2. Low performance (when used for Yandex.Metrica).
  *
  * Low performance is caused by the following reasons:
  *
  * 1. Excessive copying: 'mysqlpp::Row' works like 'std::vector<std::string>'.
  *    Content of MYSQL_ROW is copied inside it.
  *    But MYSQL_ROW is a 'char**', that is allocated in single piece,
  *     where values are stored consecutively as (non-zero-terminated) strings.
  *
  * 2. Too slow methods for converting values to numbers.
  *    In mysql++, it is done through std::stringstream.
  *    This is slower than POSIX functions (strtoul, etc).
  *    In turn, this is slower than simple hand-coded functions,
  *     that doesn't respect locales and unused by MySQL number representations.
  *
  * 3. Too slow methods of escaping and quoting.
  *    In mysql++, 'mysql_real_escape_string' is used, that works correct
  *     even for charsets, that are not based on ASCII (examples: UTF-16, Shift-JIS).
  *    But when using charsets based on ASCII, as UTF-8,
  *     (in general, charsets, where escape characters are represented in same way as in ASCII,
  *      and where that codes cannot appear in representation of another characters)
  *     this function is redundant.
  *
  * 4. Too much garbage (dynamic_cast, typeid when converting values).
  *
  * Low performance cause the following effects:
  * 1. In sequential read from MySQL table, the client CPU becomes a bottleneck, while MySQL server is not loaded.
  *    When using bare MySQL C API, this doesn't happen.
  * 2. Sequential read from MySQL is lower than 30MB/s.
  *
  * Warning!
  *
  * mysqlxx is implemented as very simple wrapper around MySQL C API,
  *  and implements only limited subset of mysql++ interface, that we use.
  * And for the sake of simplicity, some functions work only with certain assumptions,
  *  or with slightly different semantic than in mysql++.
  * And we don't care about cross-platform usage of mysqlxx.
  * These assumptions are specific for Yandex.Metrica. Your mileage may vary.
  *
  * mysqlxx could not be considered as separate full-featured library,
  *  because it is developed from the principle - "everything that we don't need is not implemented".
  * It is assumed that the user will add all missing functionality that is needed.
  */
