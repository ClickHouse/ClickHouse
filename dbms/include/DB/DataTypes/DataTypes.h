#ifndef DBMS_DATA_TYPES_DATATYPES_H
#define DBMS_DATA_TYPES_DATATYPES_H

#include <vector>

#include <Poco/SharedPtr.h>

#include <DB/DataTypes/IDataType.h>


namespace DB
{

using Poco::SharedPtr;

typedef std::vector<SharedPtr<IDataType> > DataTypes;

}

#endif
