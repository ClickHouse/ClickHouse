#pragma once


namespace DB
{

enum class UserDefinedSQLObjectType
{
    Function
};

const char * toString(UserDefinedSQLObjectType type);

}
