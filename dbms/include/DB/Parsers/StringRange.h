#ifndef DBMS_PARSERS_STRINGRANGE_H
#define DBMS_PARSERS_STRINGRANGE_H

#include <map>


namespace DB
{

typedef std::pair<const char *, const char *> StringRange;
typedef Poco::SharedPtr<String> StringPtr;

}

#endif
