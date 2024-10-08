#include <Columns/ColumnVariant.h>
#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationVariant.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/FieldToDataType.h>
#include <Common/assert_cast.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Parsers/IAST.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


DataTypeVariant::DataTypeVariant(const DataTypes & variants_)
{
    /// Sort nested types by their full names and squash identical types.
    std::map<String, DataTypePtr> name_to_type;
    for (const auto & type : variants_)
    {
        /// Nullable(...), LowCardinality(Nullable(...)) and Variant(...) types are not allowed inside Variant type.
        if (isNullableOrLowCardinalityNullable(type))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Nullable/LowCardinality(Nullable) types are not allowed inside Variant type");
        if (type->getTypeId() == TypeIndex::Variant)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Nested Variant types are not allowed");
        if (type->getTypeId() == TypeIndex::Dynamic)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dynamic type is not allowed inside Variant type");

        /// Don't use Nothing type as a variant.
        if (!isNothing(type))
            name_to_type[type->getName()] = type;
    }

    variants.reserve(name_to_type.size());
    for (const auto & [_, type] : name_to_type)
        variants.push_back(type);

    if (variants.size() > ColumnVariant::MAX_NESTED_COLUMNS)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Variant type with more than {} nested types is not allowed", ColumnVariant::MAX_NESTED_COLUMNS);
}

std::string DataTypeVariant::doGetName() const
{
    size_t size = variants.size();
    WriteBufferFromOwnString s;

    s << "Variant(";
    for (size_t i = 0; i < size; ++i)
    {
        if (i != 0)
            s << ", ";

        s << variants[i]->getName();
    }
    s << ")";

    return s.str();
}

std::string DataTypeVariant::doGetPrettyName(size_t indent) const
{
    size_t size = variants.size();
    WriteBufferFromOwnString s;
    s << "Variant(";

    for (size_t i = 0; i != size; ++i)
    {
        if (i != 0)
            s << ", ";

        s << variants[i]->getPrettyName(indent);
    }

    s << ')';
    return s.str();
}

MutableColumnPtr DataTypeVariant::createColumn() const
{
    size_t size = variants.size();
    MutableColumns nested_columns;
    nested_columns.reserve(size);
    for (size_t i = 0; i < size; ++i)
        nested_columns.push_back(variants[i]->createColumn());

    return ColumnVariant::create(std::move(nested_columns));
}

Field DataTypeVariant::getDefault() const
{
    return Null();
}

bool DataTypeVariant::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    const DataTypeVariant & rhs_variant = static_cast<const DataTypeVariant &>(rhs);

    size_t size = variants.size();
    if (size != rhs_variant.variants.size())
        return false;

    for (size_t i = 0; i < size; ++i)
    {
        if (!variants[i]->equals(*rhs_variant.variants[i]))
            return false;

        /// The same data types with different custom names considered different.
        /// For example, UInt8 and Bool.
        if ((variants[i]->hasCustomName() || rhs_variant.variants[i]->hasCustomName()) && variants[i]->getName() != rhs_variant.variants[i]->getName())
            return false;
    }

    return true;
}

bool DataTypeVariant::textCanContainOnlyValidUTF8() const
{
    return std::all_of(variants.begin(), variants.end(), [](auto && elem) { return elem->textCanContainOnlyValidUTF8(); });
}

bool DataTypeVariant::haveMaximumSizeOfValue() const
{
    return std::all_of(variants.begin(), variants.end(), [](auto && elem) { return elem->haveMaximumSizeOfValue(); });
}

bool DataTypeVariant::hasDynamicSubcolumnsDeprecated() const
{
    return std::any_of(variants.begin(), variants.end(), [](auto && elem) { return elem->hasDynamicSubcolumnsDeprecated(); });
}

std::optional<ColumnVariant::Discriminator> DataTypeVariant::tryGetVariantDiscriminator(const String & type_name) const
{
    for (size_t i = 0; i != variants.size(); ++i)
    {
        if (variants[i]->getName() == type_name)
            return i;
    }

    return std::nullopt;
}

size_t DataTypeVariant::getMaximumSizeOfValueInMemory() const
{
    size_t max_size = 0;
    for (const auto & elem : variants)
        max_size = std::max(max_size, elem->getMaximumSizeOfValueInMemory());
    return max_size;
}

SerializationPtr DataTypeVariant::doGetDefaultSerialization() const
{
    SerializationVariant::VariantSerializations serializations;
    serializations.reserve(variants.size());
    Names variant_names;
    variant_names.reserve(variants.size());

    for (const auto & variant : variants)
    {
        serializations.push_back(variant->getDefaultSerialization());
        variant_names.push_back(variant->getName());
    }

    return std::make_shared<SerializationVariant>(std::move(serializations), std::move(variant_names), SerializationVariant::getVariantsDeserializeTextOrder(variants), getName());
}

void DataTypeVariant::forEachChild(const DB::IDataType::ChildCallback & callback) const
{
    for (const auto & variant : variants)
    {
        callback(*variant);
        variant->forEachChild(callback);
    }
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        return std::make_shared<DataTypeVariant>(DataTypes{});

    DataTypes nested_types;
    nested_types.reserve(arguments->children.size());

    for (const ASTPtr & child : arguments->children)
        nested_types.emplace_back(DataTypeFactory::instance().get(child));

    return std::make_shared<DataTypeVariant>(nested_types);
}

bool isVariantExtension(const DataTypePtr & from_type, const DataTypePtr & to_type)
{
    const auto * from_variant = typeid_cast<const DataTypeVariant *>(from_type.get());
    const auto * to_variant = typeid_cast<const DataTypeVariant *>(to_type.get());
    if (!from_variant || !to_variant)
        return false;

    const auto & to_variants = to_variant->getVariants();
    std::unordered_set<String> to_variant_types;
    to_variant_types.reserve(to_variants.size());
    for (const auto & variant : to_variants)
        to_variant_types.insert(variant->getName());

    for (const auto & variant : from_variant->getVariants())
    {
        if (!to_variant_types.contains(variant->getName()))
            return false;
    }

    return true;
}


void registerDataTypeVariant(DataTypeFactory & factory)
{
    factory.registerDataType("Variant", create);
}

}
