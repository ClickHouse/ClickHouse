#include <Common/FieldVisitorByteSize.h>

namespace DB
{

bool isFixedSizeFieldType(Field::Types::Which type)
{
    switch (type)
    {
        case Field::Types::String:
        case Field::Types::Array:
        case Field::Types::Tuple:
        case Field::Types::AggregateFunctionState:
        case Field::Types::Map:
        case Field::Types::Object:
        case Field::Types::CustomType:
            return false;
        default:
            return true;
    }
}

}
