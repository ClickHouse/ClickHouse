#include <Functions/array/arrayMap.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

DataTypePtr ArrayMapImpl::getReturnType(const DataTypePtr & expression_return, const DataTypePtr & /*array_element*/)
{
    const auto * type_tuple = typeid_cast<const DataTypeTuple *>(expression_return.get());
    if (!type_tuple)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected Array(Tuple(key, value)) type, got {}", expression_return->getName());

    if (type_tuple->getElements().size() != 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected Array(Tuple(key, value)) type, got {}", expression_return->getName());

    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{type_tuple->getElement(0), type_tuple->getElement(1)}, Names{"keys", "values"}));
}

REGISTER_FUNCTION(ArrayMap)
{
    factory.registerFunction<FunctionArrayMap>();
}

}
