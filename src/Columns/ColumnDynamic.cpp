#include <Columns/ColumnDynamic.h>

#include <Columns/ColumnCompressed.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/FieldToDataType.h>
#include <Common/Arena.h>
#include <Common/SipHash.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <Interpreters/castColumn.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int PARAMETER_OUT_OF_BOUND;
}


ColumnDynamic::ColumnDynamic(size_t max_dynamic_types_) : max_dynamic_types(max_dynamic_types_)
{
    /// Create empty Variant.
    variant_info.variant_type = std::make_shared<DataTypeVariant>(DataTypes{});
    variant_info.variant_name = variant_info.variant_type->getName();
    variant_column = variant_info.variant_type->createColumn();
}

ColumnDynamic::ColumnDynamic(
    MutableColumnPtr variant_column_, const VariantInfo & variant_info_, size_t max_dynamic_types_, const Statistics & statistics_)
    : variant_column(std::move(variant_column_))
    , variant_info(variant_info_)
    , max_dynamic_types(max_dynamic_types_)
    , statistics(statistics_)
{
}

ColumnDynamic::MutablePtr ColumnDynamic::create(MutableColumnPtr variant_column, const DataTypePtr & variant_type, size_t max_dynamic_types_, const Statistics & statistics_)
{
    VariantInfo variant_info;
    variant_info.variant_type = variant_type;
    variant_info.variant_name = variant_type->getName();
    const auto & variants = assert_cast<const DataTypeVariant &>(*variant_type).getVariants();
    variant_info.variant_names.reserve(variants.size());
    variant_info.variant_name_to_discriminator.reserve(variants.size());
    for (ColumnVariant::Discriminator discr = 0; discr != variants.size(); ++discr)
    {
        const auto & variant_name = variant_info.variant_names.emplace_back(variants[discr]->getName());
        variant_info.variant_name_to_discriminator[variant_name] = discr;
    }

    return create(std::move(variant_column), variant_info, max_dynamic_types_, statistics_);
}

bool ColumnDynamic::addNewVariant(const DB::DataTypePtr & new_variant)
{
    /// Check if we already have such variant.
    if (variant_info.variant_name_to_discriminator.contains(new_variant->getName()))
        return true;

    /// Check if we reached maximum number of variants.
    if (variant_info.variant_names.size() >= max_dynamic_types)
    {
        /// ColumnDynamic can have max_dynamic_types number of variants only when it has String as a variant.
        /// Otherwise we won't be able to cast new variants to Strings.
        if (!variant_info.variant_name_to_discriminator.contains("String"))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Maximum number of variants reached, but no String variant exists");

        return false;
    }

    /// If we have (max_dynamic_types - 1) number of variants and don't have String variant, we can add only String variant.
    if (variant_info.variant_names.size() == max_dynamic_types - 1 && new_variant->getName() != "String" && !variant_info.variant_name_to_discriminator.contains("String"))
        return false;

    const DataTypes & current_variants = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariants();
    DataTypes all_variants = current_variants;
    all_variants.push_back(new_variant);
    auto new_variant_type = std::make_shared<DataTypeVariant>(all_variants);
    updateVariantInfoAndExpandVariantColumn(new_variant_type);
    return true;
}

void ColumnDynamic::addStringVariant()
{
    if (!addNewVariant(std::make_shared<DataTypeString>()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add String variant to Dynamic column, it's a bug");
}

void ColumnDynamic::updateVariantInfoAndExpandVariantColumn(const DB::DataTypePtr & new_variant_type)
{
    const DataTypes & current_variants =  assert_cast<const DataTypeVariant *>(variant_info.variant_type.get())->getVariants();
    const DataTypes & new_variants = assert_cast<const DataTypeVariant *>(new_variant_type.get())->getVariants();

    Names new_variant_names;
    new_variant_names.reserve(new_variants.size());
    std::unordered_map<String, ColumnVariant::Discriminator> new_variant_name_to_discriminator;
    new_variant_name_to_discriminator.reserve(new_variants.size());
    std::vector<std::pair<MutableColumnPtr, ColumnVariant::Discriminator>> new_variant_columns_and_discriminators_to_add;
    new_variant_columns_and_discriminators_to_add.reserve(new_variants.size() - current_variants.size());
    std::vector<ColumnVariant::Discriminator> current_to_new_discriminators;
    current_to_new_discriminators.resize(current_variants.size());

    for (ColumnVariant::Discriminator discr = 0; discr != new_variants.size(); ++discr)
    {
        const auto & name = new_variant_names.emplace_back(new_variants[discr]->getName());
        new_variant_name_to_discriminator[name] = discr;

        auto current_it = variant_info.variant_name_to_discriminator.find(name);
        if (current_it == variant_info.variant_name_to_discriminator.end())
            new_variant_columns_and_discriminators_to_add.emplace_back(new_variants[discr]->createColumn(), discr);
        else
            current_to_new_discriminators[current_it->second] = discr;
    }

    variant_info.variant_type = new_variant_type;
    variant_info.variant_name = new_variant_type->getName();
    variant_info.variant_names = new_variant_names;
    variant_info.variant_name_to_discriminator = new_variant_name_to_discriminator;
    assert_cast<ColumnVariant &>(*variant_column).extend(current_to_new_discriminators, std::move(new_variant_columns_and_discriminators_to_add));
    /// Clear mappings cache because now with new Variant we will have new mappings.
    variant_mappings_cache.clear();
}

std::vector<ColumnVariant::Discriminator> * ColumnDynamic::combineVariants(const DB::ColumnDynamic::VariantInfo & other_variant_info)
{
    /// Check if we already have global discriminators mapping for other Variant in cache.
    /// It's used to not calculate the same mapping each call of insertFrom with the same columns.
    auto cache_it = variant_mappings_cache.find(other_variant_info.variant_name);
    if (cache_it != variant_mappings_cache.end())
        return &cache_it->second;

    /// Check if we already tried to combine these variants but failed due to max_dynamic_types limit.
    if (variants_with_failed_combination.contains(other_variant_info.variant_name))
        return nullptr;

    const DataTypes & other_variants = assert_cast<const DataTypeVariant &>(*other_variant_info.variant_type).getVariants();

    size_t num_new_variants = 0;
    for (size_t i = 0; i != other_variants.size(); ++i)
    {
        if (!variant_info.variant_name_to_discriminator.contains(other_variant_info.variant_names[i]))
            ++num_new_variants;
    }

    /// If we have new variants we need to update current variant info and extend Variant column
    if (num_new_variants)
    {
        const DataTypes & current_variants = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariants();

        /// We cannot combine Variants if total number of variants exceeds max_dynamic_types.
        if (current_variants.size() + num_new_variants > max_dynamic_types)
        {
            /// Remember that we cannot combine our variant with this one, so we will not try to do it again.
            variants_with_failed_combination.insert(other_variant_info.variant_name);
            return nullptr;
        }

        /// We cannot combine Variants if total number of variants reaches max_dynamic_types and we don't have String variant.
        if (current_variants.size() + num_new_variants == max_dynamic_types && !variant_info.variant_name_to_discriminator.contains("String") && !other_variant_info.variant_name_to_discriminator.contains("String"))
        {
            variants_with_failed_combination.insert(other_variant_info.variant_name);
            return nullptr;
        }

        DataTypes all_variants = current_variants;
        all_variants.insert(all_variants.end(), other_variants.begin(), other_variants.end());
        auto new_variant_type = std::make_shared<DataTypeVariant>(all_variants);
        updateVariantInfoAndExpandVariantColumn(new_variant_type);
    }

    /// Create a global discriminators mapping for other variant.
    std::vector<ColumnVariant::Discriminator> other_to_new_discriminators;
    other_to_new_discriminators.reserve(other_variants.size());
    for (size_t i = 0; i != other_variants.size(); ++i)
        other_to_new_discriminators.push_back(variant_info.variant_name_to_discriminator[other_variant_info.variant_names[i]]);

    /// Save mapping to cache to not calculate it again for the same Variants.
    auto [it, _] = variant_mappings_cache.emplace(other_variant_info.variant_name, std::move(other_to_new_discriminators));
    return &it->second;
}

void ColumnDynamic::insert(const DB::Field & x)
{
    /// Check if we can insert field without Variant extension.
    if (variant_column->tryInsert(x))
        return;

    /// If we cannot insert field into current variant column, extend it with new variant for this field from its type.
    if (addNewVariant(applyVisitor(FieldToDataType(), x)))
    {
        /// Now we should be able to insert this field into extended variant column.
        variant_column->insert(x);
    }
    else
    {
        /// We reached maximum number of variants and couldn't add new variant.
        /// This case should be really rare in real use cases.
        /// We should always be able to add String variant and cast inserted value to String.
        addStringVariant();
        variant_column->insert(toString(x));
    }
}

bool ColumnDynamic::tryInsert(const DB::Field & x)
{
    /// We can insert any value into Dynamic column.
    insert(x);
    return true;
}


void ColumnDynamic::insertFrom(const DB::IColumn & src_, size_t n)
{
    const auto & dynamic_src = assert_cast<const ColumnDynamic &>(src_);

    /// Check if we have the same variants in both columns.
    if (variant_info.variant_name == dynamic_src.variant_info.variant_name)
    {
        variant_column->insertFrom(*dynamic_src.variant_column, n);
        return;
    }

    auto & variant_col = assert_cast<ColumnVariant &>(*variant_column);

    /// If variants are different, we need to extend our variant with new variants.
    if (auto * global_discriminators_mapping = combineVariants(dynamic_src.variant_info))
    {
        variant_col.insertFrom(*dynamic_src.variant_column, n, *global_discriminators_mapping);
        return;
    }

    /// We cannot combine 2 Variant types as total number of variants exceeds the limit.
    /// We need to insert single value, try to add only corresponding variant.
    const auto & src_variant_col = assert_cast<const ColumnVariant &>(*dynamic_src.variant_column);
    auto src_global_discr = src_variant_col.globalDiscriminatorAt(n);

    /// NULL doesn't require Variant extension.
    if (src_global_discr == ColumnVariant::NULL_DISCRIMINATOR)
    {
        insertDefault();
        return;
    }

    auto variant_type = assert_cast<const DataTypeVariant &>(*dynamic_src.variant_info.variant_type).getVariants()[src_global_discr];
    if (addNewVariant(variant_type))
    {
        auto discr = variant_info.variant_name_to_discriminator[dynamic_src.variant_info.variant_names[src_global_discr]];
        variant_col.insertIntoVariantFrom(discr, src_variant_col.getVariantByGlobalDiscriminator(src_global_discr), src_variant_col.offsetAt(n));
        return;
    }

    /// We reached maximum number of variants and couldn't add new variant.
    /// We should always be able to add String variant and cast inserted value to String.
    addStringVariant();
    auto tmp_variant_column = src_variant_col.getVariantByGlobalDiscriminator(src_global_discr).cloneEmpty();
    tmp_variant_column->insertFrom(src_variant_col.getVariantByGlobalDiscriminator(src_global_discr), src_variant_col.offsetAt(n));
    auto tmp_string_column = castColumn(ColumnWithTypeAndName(tmp_variant_column->getPtr(), variant_type, ""), std::make_shared<DataTypeString>());
    auto string_variant_discr = variant_info.variant_name_to_discriminator["String"];
    variant_col.insertIntoVariantFrom(string_variant_discr, *tmp_string_column, 0);
}

void ColumnDynamic::insertRangeFrom(const DB::IColumn & src_, size_t start, size_t length)
{
    if (start + length > src_.size())
        throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND, "Parameter out of bound in ColumnDynamic::insertRangeFrom method. "
                                                            "[start({}) + length({}) > src.size()({})]", start, length, src_.size());

    const auto & dynamic_src = assert_cast<const ColumnDynamic &>(src_);

    /// Check if we have the same variants in both columns.
    if (variant_info.variant_names == dynamic_src.variant_info.variant_names)
    {
        variant_column->insertRangeFrom(*dynamic_src.variant_column, start, length);
        return;
    }

    auto & variant_col = assert_cast<ColumnVariant &>(*variant_column);

    /// If variants are different, we need to extend our variant with new variants.
    if (auto * global_discriminators_mapping = combineVariants(dynamic_src.variant_info))
    {
        variant_col.insertRangeFrom(*dynamic_src.variant_column, start, length, *global_discriminators_mapping);
        return;
    }

    /// We cannot combine 2 Variant types as total number of variants exceeds the limit.
    /// In this case we will add most frequent variants from this range and insert them as usual,
    /// all other variants will be converted to String.
    /// TODO: instead of keeping all current variants and just adding new most frequent variants
    ///       from source columns we can also try to replace rarest existing variants with frequent
    ///       variants from source column (so we will avoid casting new frequent variants to String
    ///       and keeping rare existing ones). It will require rewriting of existing data in Variant
    ///       column but will improve usability of Dynamic column for example during squashing blocks
    ///       during insert.

    const auto & src_variant_column = dynamic_src.getVariantColumn();

    /// Calculate ranges for each variant in current range.
    std::vector<std::pair<size_t, size_t>> variants_ranges(dynamic_src.variant_info.variant_names.size(), {0, 0});
    /// If we insert the whole column, no need to iterate through the range, we can just take variant sizes.
    if (start == 0 && length == dynamic_src.size())
    {
        for (size_t i = 0; i != dynamic_src.variant_info.variant_names.size(); ++i)
            variants_ranges[i] = {0, src_variant_column.getVariantByGlobalDiscriminator(i).size()};
    }
    /// Otherwise we need to iterate through discriminators and calculate the range for each variant.
    else
    {
        const auto & local_discriminators = src_variant_column.getLocalDiscriminators();
        const auto & offsets = src_variant_column.getOffsets();
        size_t end = start + length;
        for (size_t i = start; i != end; ++i)
        {
            auto discr = src_variant_column.globalDiscriminatorByLocal(local_discriminators[i]);
            if (discr != ColumnVariant::NULL_DISCRIMINATOR)
            {
                if (!variants_ranges[discr].second)
                    variants_ranges[discr].first = offsets[i];
                ++variants_ranges[discr].second;
            }
        }
    }

    const auto & src_variants = assert_cast<const DataTypeVariant &>(*dynamic_src.variant_info.variant_type).getVariants();
    /// List of variants that will be converted to String.
    std::vector<ColumnVariant::Discriminator> variants_to_convert_to_string;
    /// Mapping from global discriminators of src_variant to the new variant we will create.
    std::vector<ColumnVariant::Discriminator> other_to_new_discriminators;
    other_to_new_discriminators.reserve(dynamic_src.variant_info.variant_names.size());

    /// Check if we cannot add any more new variants. In this case we will convert all new variants to String.
    if (variant_info.variant_names.size() == max_dynamic_types || (variant_info.variant_names.size() == max_dynamic_types - 1 && !variant_info.variant_name_to_discriminator.contains("String")))
    {
        addStringVariant();
        for (size_t i = 0; i != dynamic_src.variant_info.variant_names.size(); ++i)
        {
            auto it = variant_info.variant_name_to_discriminator.find(dynamic_src.variant_info.variant_names[i]);
            if (it == variant_info.variant_name_to_discriminator.end())
            {
                variants_to_convert_to_string.push_back(i);
                other_to_new_discriminators.push_back(variant_info.variant_name_to_discriminator["String"]);
            }
            else
            {
                other_to_new_discriminators.push_back(it->second);
            }
        }
    }
    /// We still can add some new variants, but not all of them. Let's choose the most frequent variants in specified range.
    else
    {
        std::vector<std::pair<size_t, ColumnVariant::Discriminator>> new_variants_with_sizes;
        new_variants_with_sizes.reserve(dynamic_src.variant_info.variant_names.size());
        for (size_t i = 0; i != dynamic_src.variant_info.variant_names.size(); ++i)
        {
            const auto & variant_name = dynamic_src.variant_info.variant_names[i];
            if (variant_name != "String" && !variant_info.variant_name_to_discriminator.contains(variant_name))
                new_variants_with_sizes.emplace_back(variants_ranges[i].second, i);
        }

        std::sort(new_variants_with_sizes.begin(), new_variants_with_sizes.end(), std::greater());
        DataTypes new_variants = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariants();
        if (!variant_info.variant_name_to_discriminator.contains("String"))
            new_variants.push_back(std::make_shared<DataTypeString>());

        for (const auto & [_, discr] : new_variants_with_sizes)
        {
            if (new_variants.size() != max_dynamic_types)
                new_variants.push_back(src_variants[discr]);
            else
                variants_to_convert_to_string.push_back(discr);
        }

        auto new_variant_type = std::make_shared<DataTypeVariant>(new_variants);
        updateVariantInfoAndExpandVariantColumn(new_variant_type);
        auto string_variant_discriminator = variant_info.variant_name_to_discriminator.at("String");
        for (const auto & variant_name : dynamic_src.variant_info.variant_names)
        {
            auto it = variant_info.variant_name_to_discriminator.find(variant_name);
            if (it == variant_info.variant_name_to_discriminator.end())
                other_to_new_discriminators.push_back(string_variant_discriminator);
            else
                other_to_new_discriminators.push_back(it->second);
        }
    }

    /// Convert to String all variants that couldn't be added.
    std::unordered_map<ColumnVariant::Discriminator, ColumnPtr> variants_converted_to_string;
    variants_converted_to_string.reserve(variants_to_convert_to_string.size());
    for (auto discr : variants_to_convert_to_string)
    {
        auto [variant_start, variant_length] = variants_ranges[discr];
        const auto & variant = src_variant_column.getVariantPtrByGlobalDiscriminator(discr);
        if (variant_start == 0 && variant_length == variant->size())
            variants_converted_to_string[discr] = castColumn(ColumnWithTypeAndName(variant, src_variants[discr], ""), std::make_shared<DataTypeString>());
        else
            variants_converted_to_string[discr] = castColumn(ColumnWithTypeAndName(variant->cut(variant_start, variant_length), src_variants[discr], ""), std::make_shared<DataTypeString>());
    }

    const auto & src_local_discriminators = src_variant_column.getLocalDiscriminators();
    const auto & src_offsets = src_variant_column.getOffsets();
    const auto & src_variant_columns = src_variant_column.getVariants();
    size_t end = start + length;
    for (size_t i = start; i != end; ++i)
    {
        auto local_discr = src_local_discriminators[i];
        if (local_discr == ColumnVariant::NULL_DISCRIMINATOR)
        {
            variant_col.insertDefault();
        }
        else
        {
            auto global_discr = src_variant_column.globalDiscriminatorByLocal(local_discr);
            auto to_global_discr = other_to_new_discriminators[global_discr];
            auto it = variants_converted_to_string.find(global_discr);
            if (it == variants_converted_to_string.end())
            {
                variant_col.insertIntoVariantFrom(to_global_discr, *src_variant_columns[local_discr], src_offsets[i]);
            }
            else
            {
                variant_col.insertIntoVariantFrom(to_global_discr, *it->second, src_offsets[i] - variants_ranges[global_discr].first);
            }
        }
    }
}

void ColumnDynamic::insertManyFrom(const DB::IColumn & src_, size_t position, size_t length)
{
    const auto & dynamic_src = assert_cast<const ColumnDynamic &>(src_);

    /// Check if we have the same variants in both columns.
    if (variant_info.variant_names == dynamic_src.variant_info.variant_names)
    {
        variant_column->insertManyFrom(*dynamic_src.variant_column, position, length);
        return;
    }

    auto & variant_col = assert_cast<ColumnVariant &>(*variant_column);

    /// If variants are different, we need to extend our variant with new variants.
    if (auto * global_discriminators_mapping = combineVariants(dynamic_src.variant_info))
    {
        variant_col.insertManyFrom(*dynamic_src.variant_column, position, length, *global_discriminators_mapping);
        return;
    }

    /// We cannot combine 2 Variant types as total number of variants exceeds the limit.
    /// We need to insert single value, try to add only corresponding variant.
    const auto & src_variant_col = assert_cast<const ColumnVariant &>(*dynamic_src.variant_column);
    auto src_global_discr = src_variant_col.globalDiscriminatorAt(position);
    if (src_global_discr == ColumnVariant::NULL_DISCRIMINATOR)
    {
        insertDefault();
        return;
    }

    auto variant_type = assert_cast<const DataTypeVariant &>(*dynamic_src.variant_info.variant_type).getVariants()[src_global_discr];
    if (addNewVariant(variant_type))
    {
        auto discr = variant_info.variant_name_to_discriminator[dynamic_src.variant_info.variant_names[src_global_discr]];
        variant_col.insertManyIntoVariantFrom(discr, src_variant_col.getVariantByGlobalDiscriminator(src_global_discr), src_variant_col.offsetAt(position), length);
        return;
    }

    addStringVariant();
    auto tmp_variant_column = src_variant_col.getVariantByGlobalDiscriminator(src_global_discr).cloneEmpty();
    tmp_variant_column->insertFrom(src_variant_col.getVariantByGlobalDiscriminator(src_global_discr), src_variant_col.offsetAt(position));
    auto tmp_string_column = castColumn(ColumnWithTypeAndName(tmp_variant_column->getPtr(), variant_type, ""), std::make_shared<DataTypeString>());
    auto string_variant_discr = variant_info.variant_name_to_discriminator["String"];
    variant_col.insertManyIntoVariantFrom(string_variant_discr, *tmp_string_column, 0, length);
}


StringRef ColumnDynamic::serializeValueIntoArena(size_t n, DB::Arena & arena, const char *& begin) const
{
    /// We cannot use Variant serialization here as it serializes discriminator + value,
    /// but Dynamic doesn't have fixed mapping discriminator <-> variant type
    /// as different Dynamic column can have different Variants.
    /// Instead, we serialize null bit + variant type name (size + bytes) + value.
    const auto & variant_col = assert_cast<const ColumnVariant &>(*variant_column);
    auto discr = variant_col.globalDiscriminatorAt(n);
    StringRef res;
    UInt8 null_bit = discr == ColumnVariant::NULL_DISCRIMINATOR;
    if (null_bit)
    {
        char * pos = arena.allocContinue(sizeof(UInt8), begin);
        memcpy(pos, &null_bit, sizeof(UInt8));
        res.data = pos;
        res.size = sizeof(UInt8);
        return res;
    }

    const auto & variant_name = variant_info.variant_names[discr];
    size_t variant_name_size = variant_name.size();
    char * pos = arena.allocContinue(sizeof(UInt8) + sizeof(size_t) + variant_name.size(), begin);
    memcpy(pos, &null_bit, sizeof(UInt8));
    memcpy(pos + sizeof(UInt8), &variant_name_size, sizeof(size_t));
    memcpy(pos + sizeof(UInt8) + sizeof(size_t), variant_name.data(), variant_name.size());
    res.data = pos;
    res.size = sizeof(UInt8) + sizeof(size_t) + variant_name.size();

    auto value_ref = variant_col.getVariantByGlobalDiscriminator(discr).serializeValueIntoArena(variant_col.offsetAt(n), arena, begin);
    res.data = value_ref.data - res.size;
    res.size += value_ref.size;
    return res;
}

const char * ColumnDynamic::deserializeAndInsertFromArena(const char * pos)
{
    auto & variant_col = assert_cast<ColumnVariant &>(*variant_column);
    UInt8 null_bit = unalignedLoad<UInt8>(pos);
    pos += sizeof(UInt8);
    if (null_bit)
    {
        insertDefault();
        return pos;
    }

    /// Read variant type name.
    const size_t variant_name_size = unalignedLoad<size_t>(pos);
    pos += sizeof(variant_name_size);
    String variant_name;
    variant_name.resize(variant_name_size);
    memcpy(variant_name.data(), pos, variant_name_size);
    pos += variant_name_size;
    /// If we already have such variant, just deserialize it into corresponding variant column.
    auto it = variant_info.variant_name_to_discriminator.find(variant_name);
    if (it != variant_info.variant_name_to_discriminator.end())
    {
        auto discr = it->second;
        return variant_col.deserializeVariantAndInsertFromArena(discr, pos);
    }

    /// If we don't have such variant, add it.
    auto variant_type = DataTypeFactory::instance().get(variant_name);
    if (likely(addNewVariant(variant_type)))
    {
        auto discr = variant_info.variant_name_to_discriminator[variant_name];
        return variant_col.deserializeVariantAndInsertFromArena(discr, pos);
    }

    /// We reached maximum number of variants and couldn't add new variant.
    /// We should always be able to add String variant and cast inserted value to String.
    addStringVariant();
    /// Create temporary column of this variant type and deserialize value into it.
    auto tmp_variant_column = variant_type->createColumn();
    pos = tmp_variant_column->deserializeAndInsertFromArena(pos);
    /// Cast temporary column to String and insert this value into String variant.
    auto str_column = castColumn(ColumnWithTypeAndName(tmp_variant_column->getPtr(), variant_type, ""), std::make_shared<DataTypeString>());
    variant_col.insertIntoVariantFrom(variant_info.variant_name_to_discriminator["String"], *str_column, 0);
    return pos;
}

const char * ColumnDynamic::skipSerializedInArena(const char * pos) const
{
    UInt8 null_bit = unalignedLoad<UInt8>(pos);
    pos += sizeof(UInt8);
    if (null_bit)
        return pos;

    const size_t variant_name_size = unalignedLoad<size_t>(pos);
    pos += sizeof(variant_name_size);
    String variant_name;
    variant_name.resize(variant_name_size);
    memcpy(variant_name.data(), pos, variant_name_size);
    pos += variant_name_size;
    auto tmp_variant_column = DataTypeFactory::instance().get(variant_name)->createColumn();
    return tmp_variant_column->skipSerializedInArena(pos);
}

void ColumnDynamic::updateHashWithValue(size_t n, SipHash & hash) const
{
    const auto & variant_col = assert_cast<const ColumnVariant &>(*variant_column);
    auto discr = variant_col.globalDiscriminatorAt(n);
    if (discr == ColumnVariant::NULL_DISCRIMINATOR)
    {
        hash.update(discr);
        return;
    }

    hash.update(variant_info.variant_names[discr]);
    variant_col.getVariantByGlobalDiscriminator(discr).updateHashWithValue(variant_col.offsetAt(n), hash);
}

int ColumnDynamic::compareAt(size_t n, size_t m, const DB::IColumn & rhs, int nan_direction_hint) const
{
    const auto & left_variant = assert_cast<const ColumnVariant &>(*variant_column);
    const auto & right_dynamic = assert_cast<const ColumnDynamic &>(rhs);
    const auto & right_variant = assert_cast<const ColumnVariant &>(*right_dynamic.variant_column);

    auto left_discr = left_variant.globalDiscriminatorAt(n);
    auto right_discr = right_variant.globalDiscriminatorAt(m);

    /// Check if we have NULLs and return result based on nan_direction_hint.
    if (left_discr == ColumnVariant::NULL_DISCRIMINATOR && right_discr == ColumnVariant::NULL_DISCRIMINATOR)
        return 0;
    else if (left_discr == ColumnVariant::NULL_DISCRIMINATOR)
        return nan_direction_hint;
    else if (right_discr == ColumnVariant::NULL_DISCRIMINATOR)
        return -nan_direction_hint;

    /// If rows have different types, we compare type names.
    if (variant_info.variant_names[left_discr] != right_dynamic.variant_info.variant_names[right_discr])
        return variant_info.variant_names[left_discr] < right_dynamic.variant_info.variant_names[right_discr] ? -1 : 1;

    /// If rows have the same types, compare actual values from corresponding variants.
    return left_variant.getVariantByGlobalDiscriminator(left_discr).compareAt(left_variant.offsetAt(n), right_variant.offsetAt(m), right_variant.getVariantByGlobalDiscriminator(right_discr), nan_direction_hint);
}

ColumnPtr ColumnDynamic::compress() const
{
    ColumnPtr variant_compressed = variant_column->compress();
    size_t byte_size = variant_compressed->byteSize();
    return ColumnCompressed::create(size(), byte_size,
        [my_variant_compressed = std::move(variant_compressed), my_variant_info = variant_info, my_max_dynamic_types = max_dynamic_types, my_statistics = statistics]() mutable
        {
            return ColumnDynamic::create(my_variant_compressed->decompress(), my_variant_info, my_max_dynamic_types, my_statistics);
        });
}

void ColumnDynamic::takeDynamicStructureFromSourceColumns(const Columns & source_columns)
{
    if (!empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "takeDynamicStructureFromSourceColumns should be called only on empty Dynamic column");

    /// During serialization of Dynamic column in MergeTree all Dynamic columns
    /// in single part must have the same structure (the same variants). During merge
    /// resulting column is constructed by inserting from source columns,
    /// but it may happen that resulting column doesn't have rows from all source parts
    /// but only from subset of them, and as a result some variants could be missing
    /// and structures of resulting column may differ.
    /// To solve this problem, before merge we create empty resulting column and use this method
    /// to take dynamic structure from all source column even if we won't insert
    /// rows from some of them.

    /// We want to construct resulting variant with most frequent variants from source columns and convert the rarest
    /// variants to single String variant if we exceed the limit of variants.
    /// First, collect all variants from all source columns and calculate total sizes.
    std::unordered_map<String, size_t> total_sizes;
    DataTypes all_variants;

    for (const auto & source_column : source_columns)
    {
        const auto & source_dynamic = assert_cast<const ColumnDynamic &>(*source_column);
        const auto & source_variant_column = source_dynamic.getVariantColumn();
        const auto & source_variant_info = source_dynamic.getVariantInfo();
        const auto & source_variants = assert_cast<const DataTypeVariant &>(*source_variant_info.variant_type).getVariants();
        /// During deserialization from MergeTree we will have variant sizes statistics from the whole data part.
        const auto & source_statistics =  source_dynamic.getStatistics();
        for (size_t i = 0; i != source_variants.size(); ++i)
        {
            const auto & variant_name = source_variant_info.variant_names[i];
            auto it = total_sizes.find(variant_name);
            /// Add this variant to the list of all variants if we didn't see it yet.
            if (it == total_sizes.end())
            {
                all_variants.push_back(source_variants[i]);
                it = total_sizes.emplace(variant_name, 0).first;
            }
            auto statistics_it = source_statistics.data.find(variant_name);
            size_t size = statistics_it == source_statistics.data.end() ? source_variant_column.getVariantByGlobalDiscriminator(i).size() : statistics_it->second;
            it->second += size;
        }
    }

    DataTypePtr result_variant_type;
    /// Check if the number of all variants exceeds the limit.
    if (all_variants.size() > max_dynamic_types || (all_variants.size() == max_dynamic_types && !total_sizes.contains("String")))
    {
        /// Create list of variants with their sizes and sort it.
        std::vector<std::pair<size_t, DataTypePtr>> variants_with_sizes;
        variants_with_sizes.reserve(all_variants.size());
        for (const auto & variant : all_variants)
            variants_with_sizes.emplace_back(total_sizes[variant->getName()], variant);
        std::sort(variants_with_sizes.begin(), variants_with_sizes.end(), std::greater());

        /// Take first max_dynamic_types variants from sorted list.
        DataTypes result_variants;
        result_variants.reserve(max_dynamic_types);
        /// Add String variant in advance.
        result_variants.push_back(std::make_shared<DataTypeString>());
        for (const auto & [_, variant] : variants_with_sizes)
        {
            if (result_variants.size() == max_dynamic_types)
                break;

            if (variant->getName() != "String")
                result_variants.push_back(variant);
        }

        result_variant_type = std::make_shared<DataTypeVariant>(result_variants);
    }
    else
    {
        result_variant_type = std::make_shared<DataTypeVariant>(all_variants);
    }

    /// Now we have resulting Variant and can fill variant info.
    variant_info.variant_type = result_variant_type;
    variant_info.variant_name = result_variant_type->getName();
    const auto & result_variants = assert_cast<const DataTypeVariant &>(*result_variant_type).getVariants();
    variant_info.variant_names.clear();
    variant_info.variant_names.reserve(result_variants.size());
    variant_info.variant_name_to_discriminator.clear();
    variant_info.variant_name_to_discriminator.reserve(result_variants.size());
    statistics.data.clear();
    statistics.data.reserve(result_variants.size());
    statistics.source = Statistics::Source::MERGE;
    for (size_t i = 0; i != result_variants.size(); ++i)
    {
        auto variant_name = result_variants[i]->getName();
        variant_info.variant_names.push_back(variant_name);
        variant_info.variant_name_to_discriminator[variant_name] = i;
        statistics.data[variant_name] = total_sizes[variant_name];
    }

    variant_column = variant_info.variant_type->createColumn();

    /// Now we have the resulting Variant that will be used in all merged columns.
    /// Variants can also contain Dynamic columns inside, we should collect
    /// all source variants that will be used in the resulting merged column
    /// and call takeDynamicStructureFromSourceColumns on all resulting variants.
    std::vector<Columns> variants_source_columns;
    variants_source_columns.resize(variant_info.variant_names.size());
    for (const auto & source_column : source_columns)
    {
        const auto & source_dynamic_column = assert_cast<const ColumnDynamic &>(*source_column);
        const auto & source_variant_info = source_dynamic_column.getVariantInfo();
        for (size_t i = 0; i != variant_info.variant_names.size(); ++i)
        {
            /// Try to find this variant in current source column.
            auto it = source_variant_info.variant_name_to_discriminator.find(variant_info.variant_names[i]);
            if (it != source_variant_info.variant_name_to_discriminator.end())
                variants_source_columns[i].push_back(source_dynamic_column.getVariantColumn().getVariantPtrByGlobalDiscriminator(it->second));
        }
    }

    auto & variant_col = getVariantColumn();
    for (size_t i = 0; i != variant_info.variant_names.size(); ++i)
        variant_col.getVariantByGlobalDiscriminator(i).takeDynamicStructureFromSourceColumns(variants_source_columns[i]);
}

void ColumnDynamic::applyNullMap(const ColumnVector<UInt8>::Container & null_map)
{
    assert_cast<ColumnVariant &>(*variant_column).applyNullMap(null_map);
}

void ColumnDynamic::applyNegatedNullMap(const ColumnVector<UInt8>::Container & null_map)
{
    assert_cast<ColumnVariant &>(*variant_column).applyNegatedNullMap(null_map);
}

}
