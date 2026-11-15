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
    if (typeid(rhs) != typeid(*this))
        return false;

    const auto & rhs_function = static_cast<const DataTypeFunction &>(rhs);

    /// Compare element-wise via IDataType::equals rather than by getName(), so that types sharing the
    /// same underlying type but differing only by display name (e.g. Bool and UInt8) compare equal,
    /// consistent with every other composite type (Array/Tuple/Nullable/Map/LowCardinality). Argument
    /// and return types may be nullptr for not-yet-resolved lambdas; a nullptr equals only nullptr.
    auto element_equals = [](const DataTypePtr & lhs_type, const DataTypePtr & rhs_type)
    {
        if (!lhs_type || !rhs_type)
            return lhs_type == rhs_type;
        return lhs_type->equals(*rhs_type);
    };

    if (argument_types.size() != rhs_function.argument_types.size())
        return false;

    for (size_t i = 0; i < argument_types.size(); ++i)
        if (!element_equals(argument_types[i], rhs_function.argument_types[i]))
            return false;

    return element_equals(return_type, rhs_function.return_type);
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
