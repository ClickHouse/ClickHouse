#include <IO/WriteBufferFromString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/Serializations/SerializationEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/UTF8Helpers.h>
#include <Poco/UTF8Encoding.h>

#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_TYPE_OF_FIELD;
    extern const int EMPTY_DATA_PASSED;
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


template <typename FieldType> struct EnumName;
template <> struct EnumName<Int8> { static constexpr auto value = "Enum8"; };
template <> struct EnumName<Int16> { static constexpr auto value = "Enum16"; };


template <typename Type>
const char * DataTypeEnum<Type>::getFamilyName() const
{
    return EnumName<FieldType>::value;
}


template <typename Type>
std::string DataTypeEnum<Type>::generateName(const Values & values)
{
    WriteBufferFromOwnString out;

    writeString(EnumName<FieldType>::value, out);
    writeChar('(', out);

    auto first = true;
    for (const auto & name_and_value : values)
    {
        if (!first)
            writeString(", ", out);

        first = false;

        writeQuotedString(name_and_value.first, out);
        writeString(" = ", out);
        writeText(name_and_value.second, out);
    }

    writeChar(')', out);

    return out.str();
}

template <typename Type>
DataTypeEnum<Type>::DataTypeEnum(const Values & values_)
    : EnumValues<Type>(values_)
    , type_name(generateName(this->getValues()))
{
}

template <typename Type>
Field DataTypeEnum<Type>::getDefault() const
{
    return this->getValues().front().second;
}

template <typename Type>
void DataTypeEnum<Type>::insertDefaultInto(IColumn & column) const
{
    assert_cast<ColumnType &>(column).getData().push_back(this->getValues().front().second);
}

template <typename Type>
bool DataTypeEnum<Type>::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && type_name == static_cast<const DataTypeEnum<Type> &>(rhs).type_name;
}


template <typename Type>
bool DataTypeEnum<Type>::textCanContainOnlyValidUTF8() const
{
    for (const auto & elem : this->getValues())
    {
        const char * pos = elem.first.data();
        const char * end = pos + elem.first.size();
        while (pos < end)
        {
            size_t length = UTF8::seqLength(*pos);
            if (pos + length > end)
                return false;

            if (Poco::UTF8Encoding::isLegal(reinterpret_cast<const unsigned char *>(pos), length))
                pos += length;
            else
                return false;
        }
    }
    return true;
}

template <typename Type>
static void checkOverflow(Int64 value)
{
    if (!(std::numeric_limits<Type>::min() <= value && value <= std::numeric_limits<Type>::max()))
        throw Exception("DataTypeEnum: Unexpected value " + toString(value), ErrorCodes::BAD_TYPE_OF_FIELD);
}

template <typename Type>
Field DataTypeEnum<Type>::castToName(const Field & value_or_name) const
{
    if (value_or_name.getType() == Field::Types::String)
    {
        this->getValue(value_or_name.get<String>()); /// Check correctness
        return value_or_name.get<String>();
    }
    else if (value_or_name.getType() == Field::Types::Int64)
    {
        Int64 value = value_or_name.get<Int64>();
        checkOverflow<Type>(value);
        return this->getNameForValue(static_cast<Type>(value)).toString();
    }
    else
        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD,
            "DataTypeEnum: Unsupported type of field {}", value_or_name.getTypeName());
}

template <typename Type>
Field DataTypeEnum<Type>::castToValue(const Field & value_or_name) const
{
    if (value_or_name.getType() == Field::Types::String)
    {
        return this->getValue(value_or_name.get<String>());
    }
    else if (value_or_name.getType() == Field::Types::Int64
          || value_or_name.getType() == Field::Types::UInt64)
    {
        Int64 value = value_or_name.get<Int64>();
        checkOverflow<Type>(value);
        this->getNameForValue(static_cast<Type>(value)); /// Check correctness
        return value;
    }
    else
        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD,
            "DataTypeEnum: Unsupported type of field {}", value_or_name.getTypeName());
}


template <typename Type>
bool DataTypeEnum<Type>::contains(const IDataType & rhs) const
{
    if (const auto * rhs_enum8 = typeid_cast<const DataTypeEnum8 *>(&rhs))
        return this->containsAll(rhs_enum8->getValues());
    if (const auto * rhs_enum16 = typeid_cast<const DataTypeEnum16 *>(&rhs))
        return this->containsAll(rhs_enum16->getValues());
    return false;
}

template <typename Type>
SerializationPtr DataTypeEnum<Type>::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationEnum<Type>>(this->getValues());
}


/// Explicit instantiations.
template class DataTypeEnum<Int8>;
template class DataTypeEnum<Int16>;

static void checkASTStructure(const ASTPtr & child)
{
    const auto * func = child->as<ASTFunction>();
    if (!func
        || func->name != "equals"
        || func->parameters
        || !func->arguments
        || func->arguments->children.size() != 2)
        throw Exception("Elements of Enum data type must be of form: 'name' = number, where name is string literal and number is an integer",
                        ErrorCodes::UNEXPECTED_AST_STRUCTURE);
}

static void autoAssignNumberForEnum(const ASTPtr & arguments)
{
    Int64 literal_child_assign_num = 1;
    ASTs assign_number_child;
    assign_number_child.reserve(arguments->children.size());
    bool is_first_child = true;
    size_t assign_count= 0;

    for (const ASTPtr & child : arguments->children)
    {
        if (child->as<ASTLiteral>())
        {
            assign_count += !is_first_child;
            ASTPtr func = makeASTFunction("equals", child, std::make_shared<ASTLiteral>(literal_child_assign_num + assign_count));
            assign_number_child.emplace_back(func);
        }
        else if (child->as<ASTFunction>())
        {
            if (is_first_child)
            {
                checkASTStructure(child);
                const auto * func = child->as<ASTFunction>();
                const auto * value_literal = func->arguments->children[1]->as<ASTLiteral>();

                if (!value_literal
                    || (value_literal->value.getType() != Field::Types::UInt64 && value_literal->value.getType() != Field::Types::Int64))
                    throw Exception("Elements of Enum data type must be of form: 'name' = number or 'name', where name is string literal and number is an integer",
                                    ErrorCodes::UNEXPECTED_AST_STRUCTURE);

                literal_child_assign_num = value_literal->value.get<Int64>();
            }
            assign_number_child.emplace_back(child);
        }
        else
            throw Exception("Elements of Enum data type must be of form: 'name' = number or 'name', where name is string literal and number is an integer",
                            ErrorCodes::UNEXPECTED_AST_STRUCTURE);

        is_first_child = false;
    }

    if (assign_count != 0 && assign_count != arguments->children.size() - 1)
        throw Exception("All elements of Enum data type must be of form: 'name' = number or 'name', where name is string literal and number is an integer",
                        ErrorCodes::UNEXPECTED_AST_STRUCTURE);

    arguments->children = assign_number_child;
}

template <typename DataTypeEnum>
static DataTypePtr createExact(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        throw Exception("Enum data type cannot be empty", ErrorCodes::EMPTY_DATA_PASSED);

    typename DataTypeEnum::Values values;
    values.reserve(arguments->children.size());

    using FieldType = typename DataTypeEnum::FieldType;

    autoAssignNumberForEnum(arguments);
    /// Children must be functions 'equals' with string literal as left argument and numeric literal as right argument.
    for (const ASTPtr & child : arguments->children)
    {
        checkASTStructure(child);

        const auto * func = child->as<ASTFunction>();
        const auto * name_literal = func->arguments->children[0]->as<ASTLiteral>();
        const auto * value_literal = func->arguments->children[1]->as<ASTLiteral>();

        if (!name_literal
            || !value_literal
            || name_literal->value.getType() != Field::Types::String
            || (value_literal->value.getType() != Field::Types::UInt64 && value_literal->value.getType() != Field::Types::Int64))
            throw Exception("Elements of Enum data type must be of form: 'name' = number or 'name', where name is string literal and number is an integer",
                ErrorCodes::UNEXPECTED_AST_STRUCTURE);

        const String & field_name = name_literal->value.get<String>();
        const auto value = value_literal->value.get<FieldType>();

        if (value > std::numeric_limits<FieldType>::max() || value < std::numeric_limits<FieldType>::min())
            throw Exception{"Value " + toString(value) + " for element '" + field_name + "' exceeds range of " + EnumName<FieldType>::value,
                ErrorCodes::ARGUMENT_OUT_OF_BOUND};

        values.emplace_back(field_name, value);
    }

    return std::make_shared<DataTypeEnum>(values);
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        throw Exception("Enum data type cannot be empty", ErrorCodes::EMPTY_DATA_PASSED);

    autoAssignNumberForEnum(arguments);
    /// Children must be functions 'equals' with string literal as left argument and numeric literal as right argument.
    for (const ASTPtr & child : arguments->children)
    {
        checkASTStructure(child);

        const auto * func = child->as<ASTFunction>();
        const auto * value_literal = func->arguments->children[1]->as<ASTLiteral>();

        if (!value_literal
            || (value_literal->value.getType() != Field::Types::UInt64 && value_literal->value.getType() != Field::Types::Int64))
            throw Exception("Elements of Enum data type must be of form: 'name' = number or 'name', where name is string literal and number is an integer",
                    ErrorCodes::UNEXPECTED_AST_STRUCTURE);

        Int64 value = value_literal->value.get<Int64>();

        if (value > std::numeric_limits<Int8>::max() || value < std::numeric_limits<Int8>::min())
            return createExact<DataTypeEnum16>(arguments);
    }

    return createExact<DataTypeEnum8>(arguments);
}

void registerDataTypeEnum(DataTypeFactory & factory)
{
    factory.registerDataType("Enum8", createExact<DataTypeEnum<Int8>>);
    factory.registerDataType("Enum16", createExact<DataTypeEnum<Int16>>);
    factory.registerDataType("Enum", create);

    /// MySQL
    factory.registerAlias("ENUM", "Enum", DataTypeFactory::CaseInsensitive);
}

}
