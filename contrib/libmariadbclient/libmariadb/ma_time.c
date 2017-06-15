/****************************************************************************
   Copyright (C) 2013 Monty Program AB
                 2016 MariaDB Corporation AB
   
   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.
   
   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.
   
   You should have received a copy of the GNU Library General Public
   License along with this library; if not see <http://www.gnu.org/licenses>
   or write to the Free Software Foundation, Inc., 
   51 Franklin St., Fifth Floor, Boston, MA 02110, USA

   Part of this code includes code from the PHP project which
   is freely available from http://www.php.net
*****************************************************************************/
#include <ma_global.h>
#include <mysql.h>
#include <stdio.h>


size_t mariadb_time_to_string(const MYSQL_TIME *tm, char *time_str, size_t len,
                           unsigned int digits)
{
  size_t length;

  if (!time_str || !len)
    return 0;

  if (digits == AUTO_SEC_PART_DIGITS)
    digits= MIN((tm->second_part) ? SEC_PART_DIGITS : 0, 15);

  switch(tm->time_type) {
    case MYSQL_TIMESTAMP_DATE:
      length= snprintf(time_str, len, "%04u-%02u-%02u", tm->year, tm->month, tm->day);
      digits= 0;
      break;
    case MYSQL_TIMESTAMP_DATETIME:
      length= snprintf(time_str, len, "%04u-%02u-%02u %02u:%02u:%02u", 
                      tm->year, tm->month, tm->day, tm->hour, tm->minute, tm->second);
      break;
    case MYSQL_TIMESTAMP_TIME:
      length= snprintf(time_str, len, "%s%02u:%02u:%02u",
                       (tm->neg ? "-" : ""), tm->hour, tm->minute, tm->second);
    break;
    default:
      time_str[0]= '\0';
      return 0;
      break;
  }
  if (digits && (len < length))
  {
    char helper[16];
    snprintf(helper, 16, ".%%0%du", digits);
    length+= snprintf(time_str + length, len - length, helper, digits);
  }
  return length;
}

