#include <DataTypes/DataTypeFunction.h>


namespace DB
{

std::string DataTypeFunction::getName() const
{
    std::string res = "Function(";
    if (argument_types.size() > 1)
        res += "(";
    for (size_t i = 0; i < argument_types.size(); ++i)
    {
        if (i > 0)
            res += ", ";
        const DataTypePtr & type = argument_types[i];
        res += type ? type->getName() : "?";
    }
    if (argument_types.size() > 1)
        res += ")";
    res += " -> ";
    res += return_type ? return_type->getName() : "?";
    res += ")";
    return res;
}

bool DataTypeFunction::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && getName() == rhs.getName();
}

}
