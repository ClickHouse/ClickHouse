#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

/** Knows the names and types of all possible virtual columns.
  * It is necessary for engines that redirect a request to other tables without knowing in advance what virtual columns they contain.
  */
class VirtualColumnFactory
{
public:
    static bool hasColumn(const String & name);
    static DataTypePtr getType(const String & name);

    static DataTypePtr tryGetType(const String & name);
};

}
