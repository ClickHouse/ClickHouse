#ifndef DBMS_COMMON_QUOTE_MANIPULATORS_H
#define DBMS_COMMON_QUOTE_MANIPULATORS_H

#include <strconvert/escape_manip.h>
#include <strconvert/unescape_manip.h>


namespace DB
{

typedef strconvert::quote_fast quote;
typedef strconvert::unquote_fast unquote;

}


#endif
