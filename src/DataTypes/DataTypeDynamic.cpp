#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <DataTypes/Serializations/SerializationDynamicElement.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <DataTypes/Serializations/SerializationVariantElementNullMap.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnVariant.h>
#include <Core/Field.h>
#include <Common/SipHash.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <base/find_symbols.h>
#include <IO/ReadBufferFromMemory.h>

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

void DataTypeDynamic::updateHashImpl(SipHash & hash) const
{
    hash.update(max_dynamic_types);
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

    const auto * identifier = argument->arguments->children[0]->as<ASTIdentifier>();
    if (!identifier)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected Dynamic type argument: {}. Expected expression 'max_types=N'", identifier->formatForErrorMessage());

    auto identifier_name = identifier->name();
    if (identifier_name != "max_types")
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected identifier: {}. Dynamic data type argument should be in a form 'max_types=N'", identifier_name);

    auto * literal = argument->arguments->children[1]->as<ASTLiteral>();

    if (!literal || literal->value.getType() != Field::Types::UInt64 || literal->value.safeGet<UInt64>() > ColumnDynamic::MAX_DYNAMIC_TYPES_LIMIT)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "'max_types' argument for Dynamic type should be a positive integer between 0 and {}", ColumnDynamic::MAX_DYNAMIC_TYPES_LIMIT);

    return std::make_shared<DataTypeDynamic>(literal->value.safeGet<UInt64>());
}

void registerDataTypeDynamic(DataTypeFactory & factory)
{
    factory.registerDataType("Dynamic", create);
}

namespace
{

/// Split Dynamic subcolumn name into 2 parts: type name and subcolumn of this type.
/// We cannot simply split by '.' because type name can also contain dots. For example: Tuple(`a.b` UInt32).
/// But in all such cases this '.' will be inside back quotes. To split subcolumn name correctly
/// we search for the first '.' that is not inside back quotes.
std::pair<std::string_view, std::string_view> splitSubcolumnName(std::string_view subcolumn_name)
{
    bool inside_quotes = false;
    const char * pos = subcolumn_name.data();
    const char * end = subcolumn_name.data() + subcolumn_name.size();
    while (true)
    {
        pos = find_first_symbols<'`', '.', '\\'>(pos, end);
        if (pos == end)
            break;

        if (*pos == '`')
        {
            inside_quotes = !inside_quotes;
            ++pos;
        }
        else if (*pos == '\\')
        {
            ++pos;
        }
        else if (*pos == '.')
        {
            if (inside_quotes)
                ++pos;
            else
                break;
        }
    }

    if (pos == end)
        return {subcolumn_name, {}};

    return {std::string_view(subcolumn_name.data(), pos), std::string_view(pos + 1, end)};  /// NOLINT(bugprone-suspicious-stringview-data-usage)
}

}

std::unique_ptr<IDataType::SubstreamData> DataTypeDynamic::getDynamicSubcolumnData(std::string_view subcolumn_name, const DB::IDataType::SubstreamData & data, bool throw_if_null) const
{
    auto [type_subcolumn_name, subcolumn_nested_name] = splitSubcolumnName(subcolumn_name);
    /// Check if requested subcolumn is a valid data type.
    auto subcolumn_type = DataTypeFactory::instance().tryGet(String(type_subcolumn_name));
    if (!subcolumn_type)
    {
        if (throw_if_null)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Dynamic type doesn't have subcolumn '{}'", type_subcolumn_name);
        return nullptr;
    }

    std::unique_ptr<SubstreamData> res = std::make_unique<SubstreamData>(subcolumn_type->getDefaultSerialization());
    res->type = subcolumn_type;
    std::optional<ColumnVariant::Discriminator> discriminator;
    ColumnPtr null_map_for_variant_from_shared_variant;
    if (data.column)
    {
        /// If column was provided, we should extract subcolumn from Dynamic column.
        const auto & dynamic_column = assert_cast<const ColumnDynamic &>(*data.column);
        const auto & variant_info = dynamic_column.getVariantInfo();
        const auto & variant_column = dynamic_column.getVariantColumn();
        const auto & shared_variant = dynamic_column.getSharedVariant();
        /// Check if provided Dynamic column has subcolumn of this type.
        String subcolumn_type_name = subcolumn_type->getName();
        auto it = variant_info.variant_name_to_discriminator.find(subcolumn_type_name);
        if (it != variant_info.variant_name_to_discriminator.end())
        {
            discriminator = it->second;
            res->column = variant_column.getVariantPtrByGlobalDiscriminator(*discriminator);
        }
        /// Otherwise if there is data in shared variant try to find requested type there.
        else if (!shared_variant.empty())
        {
            /// Create null map for resulting subcolumn to make it Nullable.
            auto null_map_column = ColumnUInt8::create();
            NullMap & null_map = assert_cast<ColumnUInt8 &>(*null_map_column).getData();
            null_map.reserve(variant_column.size());
            auto subcolumn = subcolumn_type->createColumn();
            auto shared_variant_local_discr = variant_column.localDiscriminatorByGlobal(dynamic_column.getSharedVariantDiscriminator());
            const auto & local_discriminators = variant_column.getLocalDiscriminators();
            const auto & offsets = variant_column.getOffsets();
            const FormatSettings format_settings;
            for (size_t i = 0; i != local_discriminators.size(); ++i)
            {
                if (local_discriminators[i] == shared_variant_local_discr)
                {
                    auto value = shared_variant.getDataAt(offsets[i]);
                    ReadBufferFromMemory buf(value.data, value.size);
                    auto type = decodeDataType(buf);
                    if (type->getName() == subcolumn_type_name)
                    {
                        subcolumn_type->getDefaultSerialization()->deserializeBinary(*subcolumn, buf, format_settings);
                        null_map.push_back(0);
                    }
                    else
                    {
                        null_map.push_back(1);
                    }
                }
                else
                {
                    null_map.push_back(1);
                }
            }

            res->column = std::move(subcolumn);
            null_map_for_variant_from_shared_variant = std::move(null_map_column);
        }
    }

    /// Extract nested subcolumn of requested dynamic subcolumn if needed.
    /// If requested subcolumn is null map, it's processed separately as there is no Nullable type yet.
    bool is_null_map_subcolumn = subcolumn_nested_name == "null";
    if (is_null_map_subcolumn)
    {
        res->type = std::make_shared<DataTypeUInt8>();
    }
    else if (!subcolumn_nested_name.empty())
    {
        res = getSubcolumnData(subcolumn_nested_name, *res, throw_if_null);
        if (!res)
            return nullptr;
    }

    res->serialization = std::make_shared<SerializationDynamicElement>(res->serialization, subcolumn_type->getName(), String(subcolumn_nested_name), is_null_map_subcolumn);
    /// Make resulting subcolumn Nullable only if type subcolumn can be inside Nullable or can be LowCardinality(Nullable()).
    bool make_subcolumn_nullable = subcolumn_type->canBeInsideNullable() || subcolumn_type->lowCardinality();
    if (!is_null_map_subcolumn && make_subcolumn_nullable)
        res->type = makeNullableOrLowCardinalityNullableSafe(res->type);

    if (data.column)
    {
        /// Check if provided Dynamic column has subcolumn of this type. In this case we should use VariantSubcolumnCreator/VariantNullMapSubcolumnCreator to
        /// create full subcolumn from variant according to discriminators.
        if (discriminator)
        {
            const auto & variant_column = assert_cast<const ColumnDynamic &>(*data.column).getVariantColumn();
            std::unique_ptr<ISerialization::ISubcolumnCreator> creator;
            if (is_null_map_subcolumn)
                creator = std::make_unique<SerializationVariantElementNullMap::VariantNullMapSubcolumnCreator>(
                    variant_column.getLocalDiscriminatorsPtr(),
                    "",
                    *discriminator,
                    variant_column.localDiscriminatorByGlobal(*discriminator));
            else
                creator = std::make_unique<SerializationVariantElement::VariantSubcolumnCreator>(
                    variant_column.getLocalDiscriminatorsPtr(),
                    "",
                    *discriminator,
                    variant_column.localDiscriminatorByGlobal(*discriminator),
                    make_subcolumn_nullable);
            res->column = creator->create(res->column);
        }
        /// Check if requested type was extracted from shared variant. In this case we should use
        /// VariantSubcolumnCreator to create full subcolumn from variant according to created null map.
        else if (null_map_for_variant_from_shared_variant)
        {
            if (is_null_map_subcolumn)
            {
                res->column = null_map_for_variant_from_shared_variant;
            }
            else
            {
                SerializationVariantElement::VariantSubcolumnCreator creator(
                    null_map_for_variant_from_shared_variant, "", 0, 0, make_subcolumn_nullable, null_map_for_variant_from_shared_variant);
                res->column = creator.create(res->column);
            }
        }
        /// Provided Dynamic column doesn't have subcolumn of this type, just create column filled with default values.
        else if (is_null_map_subcolumn)
        {
            /// Fill null map with 1 when there is no such Dynamic subcolumn.
            auto column = ColumnUInt8::create();
            assert_cast<ColumnUInt8 &>(*column).getData().resize_fill(data.column->size(), 1);
            res->column = std::move(column);
        }
        else
        {
            auto column = res->type->createColumn();
            column->insertManyDefaults(data.column->size());
            res->column = std::move(column);
        }
    }

    return res;
}

}
