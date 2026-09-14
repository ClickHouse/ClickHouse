#include <DataTypes/DataTypeFunction.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/SipHash.h>


namespace DB
{

std::string DataTypeFunction::doGetName() const
{
    WriteBufferFromOwnString res;

    res << "Function(";
    if (argument_types.size() > 1)
        res << "(";
    for (size_t i = 0; i < argument_types.size(); ++i)
    {
        if (i > 0)
            res << ", ";
        const DataTypePtr & type = argument_types[i];
        res << (type ? type->getName() : "?");
    }
    if (argument_types.size() > 1)
        res << ")";
    res << " -> ";
    res << (return_type ? return_type->getName() : "?");
    res << ")";
    return res.str();
}

bool DataTypeFunction::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && getName() == rhs.getName();
}

void DataTypeFunction::updateHashImpl(SipHash & hash) const
{
    /// Argument types and return type can be nullptr when the lambda is not yet resolved.
    hash.update(argument_types.size());
    for (const auto & arg_type : argument_types)
    {
        hash.update(arg_type != nullptr);
        if (arg_type)
            arg_type->updateHash(hash);
    }

    hash.update(return_type != nullptr);
    if (return_type)
        return_type->updateHash(hash);
}

}
