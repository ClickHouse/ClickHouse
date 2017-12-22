#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>


namespace DB
{

void assertNoParameters(const std::string & name, const Array & parameters);
void assertUnary(const std::string & name, const DataTypes & argument_types);
void assertBinary(const std::string & name, const DataTypes & argument_types);

}
