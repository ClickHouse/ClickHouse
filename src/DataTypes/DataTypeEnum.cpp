#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/Serializations/SerializationEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/WriteHelpers.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/UTF8Helpers.h>
#include <Common/SipHash.h>
#include <Columns/ColumnSparse.h>
#include <Poco/UTF8Encoding.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

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
Type DataTypeEnum<Type>::getDefaultValue() const
{
    return this->getValues().front().second;
}

template <typename Type>
void DataTypeEnum<Type>::insertDefaultInto(IColumn & column) const
{
    const auto & default_value = this->getValues().front().second;

    /// This code is actually bad, but unfortunately, `IDataType::insertDefaultInto`
    /// breaks the abstraction of the separation of data types, serializations, and columns.
    /// Since this method is overridden only for `DataTypeEnum` and this code
    /// has remained unchanged for years, so it should be okay.
    if (auto * sparse_column = typeid_cast<ColumnSparse *>(&column))
    {
        if (default_value == Type{})
            sparse_column->insertDefault();
        else
            sparse_column->insert(default_value);
    }
    else
    {
        assert_cast<ColumnType &>(column).getData().push_back(default_value);
    }
}

template <typename Type>
bool DataTypeEnum<Type>::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && type_name == static_cast<const DataTypeEnum<Type> &>(rhs).type_name;
}

template <typename Type>
void DataTypeEnum<Type>::updateHashImpl(SipHash & hash) const
{
    hash.update(type_name);
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

            if (Poco::UTF8Encoding::isLegal(reinterpret_cast<const unsigned char *>(pos), static_cast<int>(length)))
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
        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "DataTypeEnum: Unexpected value {}", toString(value));
}


template <typename Type>
Field DataTypeEnum<Type>::castToName(const Field & value_or_name) const
{
    if (value_or_name.getType() == Field::Types::String)
    {
        this->getValue(value_or_name.safeGet<String>()); /// Check correctness
        return value_or_name.safeGet<String>();
    }
    if (value_or_name.getType() == Field::Types::Int64)
    {
        Int64 value = value_or_name.safeGet<Int64>();
        checkOverflow<Type>(value);
        return std::string{this->getNameForValue(static_cast<Type>(value))};
    }
    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "DataTypeEnum: Unsupported type of field {}", value_or_name.getTypeName());
}

template <typename Type>
Field DataTypeEnum<Type>::castToValue(const Field & value_or_name) const
{
    if (value_or_name.getType() == Field::Types::String)
    {
        return this->getValue(value_or_name.safeGet<String>());
    }
    if (value_or_name.getType() == Field::Types::Int64 || value_or_name.getType() == Field::Types::UInt64)
    {
        Int64 value = value_or_name.safeGet<Int64>();
        checkOverflow<Type>(value);
        this->getNameForValue(static_cast<Type>(value)); /// Check correctness
        return value;
    }
    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "DataTypeEnum: Unsupported type of field {}", value_or_name.getTypeName());
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
SerializationPtr DataTypeEnum<Type>::doGetSerialization(const SerializationInfoSettings &) const
{
    return SerializationEnum<Type>::create(std::static_pointer_cast<const DataTypeEnum<Type>>(shared_from_this()));
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
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Elements of Enum data type must be of form: "
                        "'name' = number, where name is string literal and number is an integer");
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
            ASTPtr func = makeASTOperator("equals", child, make_intrusive<ASTLiteral>(literal_child_assign_num + assign_count));
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
                    throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                                    "Elements of Enum data type must be of form: "
                                    "'name' = number or 'name', where name is string literal and number is an integer");

                literal_child_assign_num = value_literal->value.safeGet<Int64>();
            }
            assign_number_child.emplace_back(child);
        }
        else
            throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                                    "Elements of Enum data type must be of form: "
                                    "'name' = number or 'name', where name is string literal and number is an integer");

        is_first_child = false;
    }

    if (assign_count != 0 && assign_count != arguments->children.size() - 1)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                        "All elements of Enum data type must be of form: "
                        "'name' = number or 'name', where name is string literal and number is an integer");

    arguments->children = assign_number_child;
}

template <typename DataTypeEnum>
static DataTypePtr createExact(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        throw Exception(ErrorCodes::EMPTY_DATA_PASSED, "Enum data type cannot be empty");

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
            throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                                    "Elements of Enum data type must be of form: "
                                    "'name' = number or 'name', where name is string literal and number is an integer");

        const String & field_name = name_literal->value.safeGet<String>();
        const auto value = value_literal->value.safeGet<FieldType>();

        if (value > std::numeric_limits<FieldType>::max() || value < std::numeric_limits<FieldType>::min())
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Value {} for element '{}' exceeds range of {}",
                toString(value), field_name, EnumName<FieldType>::value);

        values.emplace_back(field_name, value);
    }

    return std::make_shared<DataTypeEnum>(values);
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        throw Exception(ErrorCodes::EMPTY_DATA_PASSED, "Enum data type cannot be empty");

    autoAssignNumberForEnum(arguments);
    /// Children must be functions 'equals' with string literal as left argument and numeric literal as right argument.
    for (const ASTPtr & child : arguments->children)
    {
        checkASTStructure(child);

        const auto * func = child->as<ASTFunction>();
        const auto * value_literal = func->arguments->children[1]->as<ASTLiteral>();

        if (!value_literal
            || (value_literal->value.getType() != Field::Types::UInt64 && value_literal->value.getType() != Field::Types::Int64))
            throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                                    "Elements of Enum data type must be of form: "
                                    "'name' = number or 'name', where name is string literal and number is an integer");

        Int64 value = value_literal->value.safeGet<Int64>();

        if (value > std::numeric_limits<Int8>::max() || value < std::numeric_limits<Int8>::min())
            return createExact<DataTypeEnum16>(arguments);
    }

    return createExact<DataTypeEnum8>(arguments);
}

void registerDataTypeEnum(DataTypeFactory & factory)
{
    factory.registerDataType("Enum8", createExact<DataTypeEnum<Int8>>, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = R"DOCS_MD(
An enumeration type that stores values as 8-bit signed integers (`Int8`), allowing up to 256 named values in the range `[-128, 127]`. Each `'string' = integer` pair maps a human-readable name to its stored numeric value. Use it instead of `Enum16` when the set of values is small to save space.
)DOCS_MD",
            .syntax = "Enum8('name1' = num1, 'name2' = num2, ...)",
            .related = {"Enum"},
        });
    factory.registerDataType("Enum16", createExact<DataTypeEnum<Int16>>, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = R"DOCS_MD(
An enumeration type that stores values as 16-bit signed integers (`Int16`), allowing up to 65536 named values in the range `[-32768, 32767]`. Each `'string' = integer` pair maps a human-readable name to its stored numeric value. Use it when the set of named values is too large to fit in `Enum8`.
)DOCS_MD",
            .syntax = "Enum16('name1' = num1, 'name2' = num2, ...)",
            .related = {"Enum"},
        });
    factory.registerDataType("Enum", create, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = R"DOCS_MD(
Enumerated type consisting of named values.

Named values can be declared as `'string' = integer` pairs or `'string'` names . ClickHouse stores only numbers, but supports operations with the values through their names.

ClickHouse supports:

- 8-bit `Enum`. It can contain up to 256 values enumerated in the `[-128, 127]` range.
- 16-bit `Enum`. It can contain up to 65536 values enumerated in the `[-32768, 32767]` range.

ClickHouse automatically chooses the type of `Enum` when data is inserted. You can also use `Enum8` or `Enum16` types to be sure in the size of storage.

## Usage Examples {#usage-examples}

Here we create a table with an `Enum8('hello' = 1, 'world' = 2)` type column:

```sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world' = 2)
)
ENGINE = TinyLog
```

Similarly, you could omit numbers. ClickHouse will assign consecutive numbers automatically. Numbers are assigned starting from 1 by default.

```sql
CREATE TABLE t_enum
(
    x Enum('hello', 'world')
)
ENGINE = TinyLog
```

You can also specify legal starting number for the first name.

```sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world')
)
ENGINE = TinyLog
```

```sql
CREATE TABLE t_enum
(
    x Enum8('hello' = -129, 'world')
)
ENGINE = TinyLog
```

```text
Exception on server:
Code: 69. DB::Exception: Value -129 for element 'hello' exceeds range of Enum8.
```

Column `x` can only store values that are listed in the type definition: `'hello'` or `'world'`. If you try to save any other value, ClickHouse will raise an exception. 8-bit size for this `Enum` is chosen automatically.

```sql
INSERT INTO t_enum VALUES ('hello'), ('world'), ('hello')
```

```text
Ok.
```

```sql
INSERT INTO t_enum VALUES('a')
```

```text
Exception on client:
Code: 49. DB::Exception: Unknown element 'a' for type Enum('hello' = 1, 'world' = 2)
```

When you query data from the table, ClickHouse outputs the string values from `Enum`.

```sql
SELECT * FROM t_enum
```

```text
┌─x─────┐
│ hello │
│ world │
│ hello │
└───────┘
```

If you need to see the numeric equivalents of the rows, you must cast the `Enum` value to integer type.

```sql
SELECT CAST(x, 'Int8') FROM t_enum
```

```text
┌─CAST(x, 'Int8')─┐
│               1 │
│               2 │
│               1 │
└─────────────────┘
```

To create an Enum value in a query, you also need to use `CAST`.

```sql
SELECT toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))
```

```text
┌─toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))─┐
│ Enum8('a' = 1, 'b' = 2)                             │
└─────────────────────────────────────────────────────┘
```

## General Rules and Usage {#general-rules-and-usage}

Each of the values is assigned a number in the range `-128 ... 127` for `Enum8` or in the range `-32768 ... 32767` for `Enum16`. All the strings and numbers must be different. An empty string is allowed. If this type is specified (in a table definition), numbers can be in an arbitrary order. However, the order does not matter.

Neither the string nor the numeric value in an `Enum` can be [NULL](../../sql-reference/syntax.md).

An `Enum` can be contained in [Nullable](../../sql-reference/data-types/nullable.md) type. So if you create a table using the query

```sql
CREATE TABLE t_enum_nullable
(
    x Nullable( Enum8('hello' = 1, 'world' = 2) )
)
ENGINE = TinyLog
```

it can store not only `'hello'` and `'world'`, but `NULL`, as well.

```sql
INSERT INTO t_enum_nullable VALUES('hello'),('world'),(NULL)
```

In RAM, an `Enum` column is stored in the same way as `Int8` or `Int16` of the corresponding numerical values.

When reading in text form, ClickHouse parses the value as a string and searches for the corresponding string from the set of Enum values. If it is not found, an exception is thrown. When reading in text format, the string is read and the corresponding numeric value is looked up. An exception will be thrown if it is not found.
When writing in text form, it writes the value as the corresponding string. If column data contains garbage (numbers that are not from the valid set), an exception is thrown. When reading and writing in binary form, it works the same way as for Int8 and Int16 data types.
The implicit default value is the value with the lowest number.

During `ORDER BY`, `GROUP BY`, `IN`, `DISTINCT` and so on, Enums behave the same way as the corresponding numbers. For example, ORDER BY sorts them numerically. Equality and comparison operators work the same way on Enums as they do on the underlying numeric values.

Enum values cannot be compared with numbers. Enums can be compared to a constant string. If the string compared to is not a valid value for the Enum, an exception will be thrown. The IN operator is supported with the Enum on the left-hand side and a set of strings on the right-hand side. The strings are the values of the corresponding Enum.

Most numeric and string operations are not defined for Enum values, e.g. adding a number to an Enum or concatenating a string to an Enum.
However, the Enum has a natural `toString` function that returns its string value.

Enum values are also convertible to numeric types using the `toT` function, where T is a numeric type. When T corresponds to the enum's underlying numeric type, this conversion is zero-cost.
The Enum type can be changed without cost using ALTER, if only the set of values is changed. It is possible to both add and remove members of the Enum using ALTER (removing is safe only if the removed value has never been used in the table). As a safeguard, changing the numeric value of a previously defined Enum member will throw an exception.

Using ALTER, it is possible to change an Enum8 to an Enum16 or vice versa, just like changing an Int8 to Int16.
)DOCS_MD",
            .syntax = "Enum(...)",
            .related = {"Enum8", "Enum16"},
        });

    /// MySQL
    factory.registerAlias("ENUM", "Enum", DataTypeFactory::Case::Insensitive);
}

}
