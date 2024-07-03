#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <DataTypes/Serializations/SerializationDynamicElement.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnVariant.h>
#include <Core/Field.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

DataTypeDynamic::DataTypeDynamic(size_t max_dynamic_types_) : max_dynamic_types(max_dynamic_types_)
{
}

MutableColumnPtr DataTypeDynamic::createColumn() const
{
    return ColumnDynamic::create(max_dynamic_types);
}

String DataTypeDynamic::doGetName() const
{
    if (max_dynamic_types == DEFAULT_MAX_DYNAMIC_TYPES)
        return "Dynamic";
    return "Dynamic(max_types=" + toString(max_dynamic_types) + ")";
}

Field DataTypeDynamic::getDefault() const
{
    return Field(Null());
}

SerializationPtr DataTypeDynamic::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationDynamic>(max_dynamic_types);
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        return std::make_shared<DataTypeDynamic>();

    if (arguments->children.size() > 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Dynamic data type can have only one optional argument - the maximum number of dynamic types in a form 'Dynamic(max_types=N)");


    const auto * argument = arguments->children[0]->as<ASTFunction>();
    if (!argument || argument->name != "equals")
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Dynamic data type argument should be in a form 'max_types=N'");

    auto identifier_name = argument->arguments->children[0]->as<ASTIdentifier>()->name();
    if (identifier_name != "max_types")
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected identifier: {}. Dynamic data type argument should be in a form 'max_types=N'", identifier_name);

    auto * literal = argument->arguments->children[1]->as<ASTLiteral>();

    if (!literal || literal->value.getType() != Field::Types::UInt64 || literal->value.get<UInt64>() == 0 || literal->value.get<UInt64>() > 255)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "'max_types' argument for Dynamic type should be a positive integer between 1 and 255");

    return std::make_shared<DataTypeDynamic>(literal->value.get<UInt64>());
}

void registerDataTypeDynamic(DataTypeFactory & factory)
{
    factory.registerDataType("Dynamic", create);
}

std::unique_ptr<IDataType::SubstreamData> DataTypeDynamic::getDynamicSubcolumnData(std::string_view subcolumn_name, const DB::IDataType::SubstreamData & data, bool throw_if_null) const
{
    auto [subcolumn_type_name, subcolumn_nested_name] = Nested::splitName(subcolumn_name);
    /// Check if requested subcolumn is a valid data type.
    auto subcolumn_type = DataTypeFactory::instance().tryGet(String(subcolumn_type_name));
    if (!subcolumn_type)
    {
        if (throw_if_null)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Dynamic type doesn't have subcolumn '{}'", subcolumn_type_name);
        return nullptr;
    }

    std::unique_ptr<SubstreamData> res = std::make_unique<SubstreamData>(subcolumn_type->getDefaultSerialization());
    res->type = subcolumn_type;
    std::optional<ColumnVariant::Discriminator> discriminator;
    if (data.column)
    {
        /// If column was provided, we should extract subcolumn from Dynamic column.
        const auto & dynamic_column = assert_cast<const ColumnDynamic &>(*data.column);
        const auto & variant_info = dynamic_column.getVariantInfo();
        /// Check if provided Dynamic column has subcolumn of this type.
        auto it = variant_info.variant_name_to_discriminator.find(subcolumn_type->getName());
        if (it != variant_info.variant_name_to_discriminator.end())
        {
            discriminator = it->second;
            res->column = dynamic_column.getVariantColumn().getVariantPtrByGlobalDiscriminator(*discriminator);
        }
    }

    /// Extract nested subcolumn of requested dynamic subcolumn if needed.
    if (!subcolumn_nested_name.empty())
    {
        res = getSubcolumnData(subcolumn_nested_name, *res, throw_if_null);
        if (!res)
            return nullptr;
    }

    res->serialization = std::make_shared<SerializationDynamicElement>(res->serialization, subcolumn_type->getName());
    res->type = makeNullableOrLowCardinalityNullableSafe(res->type);
    if (data.column)
    {
        if (discriminator)
        {
            /// Provided Dynamic column has subcolumn of this type, we should use VariantSubcolumnCreator to
            /// create full subcolumn from variant according to discriminators.
            const auto & variant_column = assert_cast<const ColumnDynamic &>(*data.column).getVariantColumn();
            auto creator = SerializationVariantElement::VariantSubcolumnCreator(variant_column.getLocalDiscriminatorsPtr(), "", *discriminator, variant_column.localDiscriminatorByGlobal(*discriminator));
            res->column = creator.create(res->column);
        }
        else
        {
            /// Provided Dynamic column doesn't have subcolumn of this type, just create column filled with default values.
            auto column = res->type->createColumn();
            column->insertManyDefaults(data.column->size());
            res->column = std::move(column);
        }
    }

    return res;
}

}
