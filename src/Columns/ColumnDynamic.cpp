#include <Columns/ColumnDynamic.h>

#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Common/Arena.h>
#include <Common/SipHash.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <Formats/FormatSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int PARAMETER_OUT_OF_BOUND;
}

namespace
{

/// Static default format settings to avoid creating it every time.
const FormatSettings & getFormatSettings()
{
    static const FormatSettings settings;
    return settings;
}

}

/// Shared variant will contain String values but we cannot use usual String type
/// because we can have regular variant with type String.
/// To solve it, we use String type with custom name for shared variant.
DataTypePtr ColumnDynamic::getSharedVariantDataType()
{
    return DataTypeFactory::instance().getCustom("String", std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeCustomFixedName>(getSharedVariantTypeName())));
}

ColumnDynamic::ColumnDynamic(size_t max_dynamic_types_) : max_dynamic_types(max_dynamic_types_), global_max_dynamic_types(max_dynamic_types)
{
    /// Create Variant with shared variant.
    setVariantType(std::make_shared<DataTypeVariant>(DataTypes{getSharedVariantDataType()}));
}

ColumnDynamic::ColumnDynamic(
    MutableColumnPtr variant_column_, const DataTypePtr & variant_type_, size_t max_dynamic_types_, size_t global_max_dynamic_types_, const StatisticsPtr & statistics_)
    : variant_column(std::move(variant_column_))
    , variant_column_ptr(assert_cast<ColumnVariant *>(variant_column.get()))
    , max_dynamic_types(max_dynamic_types_)
    , global_max_dynamic_types(global_max_dynamic_types_)
    , statistics(statistics_)
{
    createVariantInfo(variant_type_);
}

ColumnDynamic::ColumnDynamic(
    MutableColumnPtr variant_column_, const VariantInfo & variant_info_, size_t max_dynamic_types_, size_t global_max_dynamic_types_, const StatisticsPtr & statistics_)
    : variant_column(std::move(variant_column_))
    , variant_column_ptr(assert_cast<ColumnVariant *>(variant_column.get()))
    , variant_info(variant_info_)
    , max_dynamic_types(max_dynamic_types_)
    , global_max_dynamic_types(global_max_dynamic_types_)
    , statistics(statistics_)
{
}

void ColumnDynamic::setVariantType(const DataTypePtr & variant_type)
{
    if (variant_column && !empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Setting specific variant type is allowed only for empty dynamic column");

    variant_column = variant_type->createColumn();
    variant_column_ptr = assert_cast<ColumnVariant *>(variant_column.get());
    createVariantInfo(variant_type);
}

void ColumnDynamic::setMaxDynamicPaths(size_t max_dynamic_type_)
{
    if (variant_column && !empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Setting specific max_dynamic_type parameter is allowed only for empty dynamic column");

    max_dynamic_types = max_dynamic_type_;
}

void ColumnDynamic::createVariantInfo(const DataTypePtr & variant_type)
{
    variant_info.variant_type = variant_type;
    variant_info.variant_name = variant_type->getName();
    const auto & variants = assert_cast<const DataTypeVariant &>(*variant_type).getVariants();
    variant_info.variant_names.clear();
    variant_info.variant_names.reserve(variants.size());
    variant_info.variant_name_to_discriminator.clear();
    variant_info.variant_name_to_discriminator.reserve(variants.size());
    for (ColumnVariant::Discriminator discr = 0; discr != variants.size(); ++discr)
    {
        const auto & variant_name = variant_info.variant_names.emplace_back(variants[discr]->getName());
        variant_info.variant_name_to_discriminator[variant_name] = discr;
    }

    if (!variant_info.variant_name_to_discriminator.contains(getSharedVariantTypeName()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Variant in Dynamic column doesn't contain shared variant");
}

bool ColumnDynamic::addNewVariant(const DataTypePtr & new_variant, const String & new_variant_name)
{
    /// Check if we already have such variant.
    if (variant_info.variant_name_to_discriminator.contains(new_variant_name))
        return true;

    /// Check if we reached maximum number of variants.
    if (!canAddNewVariant())
    {
        /// Dynamic column should always have shared variant.
        if (!variant_info.variant_name_to_discriminator.contains(getSharedVariantTypeName()))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Maximum number of variants reached, but no shared variant exists");

        return false;
    }

    const DataTypes & current_variants = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariants();
    DataTypes all_variants = current_variants;
    all_variants.push_back(new_variant);
    auto new_variant_type = std::make_shared<DataTypeVariant>(all_variants);
    updateVariantInfoAndExpandVariantColumn(new_variant_type);
    return true;
}

void extendVariantColumn(
    IColumn & variant_column,
    const DataTypePtr & old_variant_type,
    const DataTypePtr & new_variant_type,
    std::unordered_map<String, UInt8> old_variant_name_to_discriminator)
{
    const DataTypes & current_variants =  assert_cast<const DataTypeVariant *>(old_variant_type.get())->getVariants();
    const DataTypes & new_variants = assert_cast<const DataTypeVariant *>(new_variant_type.get())->getVariants();

    std::vector<std::pair<MutableColumnPtr, ColumnVariant::Discriminator>> new_variant_columns_and_discriminators_to_add;
    new_variant_columns_and_discriminators_to_add.reserve(new_variants.size() - current_variants.size());
    std::vector<ColumnVariant::Discriminator> current_to_new_discriminators;
    current_to_new_discriminators.resize(current_variants.size());

    for (ColumnVariant::Discriminator discr = 0; discr != new_variants.size(); ++discr)
    {
        auto current_it = old_variant_name_to_discriminator.find(new_variants[discr]->getName());
        if (current_it == old_variant_name_to_discriminator.end())
            new_variant_columns_and_discriminators_to_add.emplace_back(new_variants[discr]->createColumn(), discr);
        else
            current_to_new_discriminators[current_it->second] = discr;
    }

    assert_cast<ColumnVariant &>(variant_column).extend(current_to_new_discriminators, std::move(new_variant_columns_and_discriminators_to_add));
}

void ColumnDynamic::updateVariantInfoAndExpandVariantColumn(const DataTypePtr & new_variant_type)
{
    extendVariantColumn(*variant_column, variant_info.variant_type, new_variant_type, variant_info.variant_name_to_discriminator);
    createVariantInfo(new_variant_type);

    /// Clear mappings cache because now with new Variant we will have new mappings.
    variant_mappings_cache.clear();
}

std::vector<ColumnVariant::Discriminator> * ColumnDynamic::combineVariants(const ColumnDynamic::VariantInfo & other_variant_info)
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
        if (!canAddNewVariants(num_new_variants))
        {
            /// Remember that we cannot combine our variant with this one, so we will not try to do it again.
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

void ColumnDynamic::insert(const Field & x)
{
    if (x.isNull())
    {
        insertDefault();
        return;
    }

    auto & variant_col = getVariantColumn();
    auto shared_variant_discr = getSharedVariantDiscriminator();
    /// Check if we can insert field into existing variants and avoid Variant extension.
    for (size_t i = 0; i != variant_col.getNumVariants(); ++i)
    {
        if (i != shared_variant_discr && variant_col.getVariantByGlobalDiscriminator(i).tryInsert(x))
        {
            variant_col.getLocalDiscriminators().push_back(variant_col.localDiscriminatorByGlobal(i));
            variant_col.getOffsets().push_back(variant_col.getVariantByGlobalDiscriminator(i).size() - 1);
            return;
        }
    }

    /// If we cannot insert field into current variant column, extend it with new variant for this field from its type.
    auto field_data_type = applyVisitor(FieldToDataType(), x);
    auto field_data_type_name = field_data_type->getName();
    if (addNewVariant(field_data_type, field_data_type_name))
    {
        /// Insert this field into newly added variant.
        auto discr = variant_info.variant_name_to_discriminator[field_data_type_name];
        variant_col.getVariantByGlobalDiscriminator(discr).insert(x);
        variant_col.getLocalDiscriminators().push_back(variant_col.localDiscriminatorByGlobal(discr));
        variant_col.getOffsets().push_back(variant_col.getVariantByGlobalDiscriminator(discr).size() - 1);
    }
    else
    {
        /// We reached maximum number of variants and couldn't add new variant.
        /// In this case we add the value of this new variant into special shared variant.
        /// We store values in shared variant in binary form with binary encoded type.
        auto & shared_variant = getSharedVariant();
        auto & chars = shared_variant.getChars();
        WriteBufferFromVector<ColumnString::Chars> value_buf(chars, AppendModeTag());
        encodeDataType(field_data_type, value_buf);
        getVariantSerialization(field_data_type, field_data_type_name)->serializeBinary(x, value_buf, getFormatSettings());
        value_buf.finalize();
        chars.push_back(0);
        shared_variant.getOffsets().push_back(chars.size());
        variant_col.getLocalDiscriminators().push_back(variant_col.localDiscriminatorByGlobal(shared_variant_discr));
        variant_col.getOffsets().push_back(shared_variant.size() - 1);
    }
}

bool ColumnDynamic::tryInsert(const Field & x)
{
    /// We can insert any value into Dynamic column.
    insert(x);
    return true;
}

Field ColumnDynamic::operator[](size_t n) const
{
    Field res;
    get(n, res);
    return res;
}

void ColumnDynamic::get(size_t n, Field & res) const
{
    const auto & variant_col = getVariantColumn();
    /// Check if value is not in shared variant.
    if (variant_col.globalDiscriminatorAt(n) != getSharedVariantDiscriminator())
    {
        variant_col.get(n, res);
        return;
    }

    /// We should deeserialize value from shared variant.
    const auto & shared_variant = getSharedVariant();
    auto value_data = shared_variant.getDataAt(variant_col.offsetAt(n));
    ReadBufferFromMemory buf(value_data.data, value_data.size);
    auto type = decodeDataType(buf);
    type->getDefaultSerialization()->deserializeBinary(res, buf, getFormatSettings());
}


#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnDynamic::insertFrom(const IColumn & src_, size_t n)
#else
void ColumnDynamic::doInsertFrom(const IColumn & src_, size_t n)
#endif
{
    const auto & dynamic_src = assert_cast<const ColumnDynamic &>(src_);

    /// Check if we have the same variants in both columns.
    if (variant_info.variant_name == dynamic_src.variant_info.variant_name)
    {
        variant_column_ptr->insertFrom(*dynamic_src.variant_column, n);
        return;
    }

    auto & variant_col = getVariantColumn();
    const auto & src_variant_col = dynamic_src.getVariantColumn();
    auto src_global_discr = src_variant_col.globalDiscriminatorAt(n);
    auto src_offset = src_variant_col.offsetAt(n);

    /// Check if we insert from shared variant and process it separately.
    if (src_global_discr == dynamic_src.getSharedVariantDiscriminator())
    {
        const auto & src_shared_variant = dynamic_src.getSharedVariant();
        auto value = src_shared_variant.getDataAt(src_offset);
        /// Decode data type of this value.
        ReadBufferFromMemory buf(value.data, value.size);
        auto type = decodeDataType(buf);
        auto type_name = type->getName();
        /// Check if we have this variant and deserialize value into variant from shared variant data.
        if (auto it = variant_info.variant_name_to_discriminator.find(type_name); it != variant_info.variant_name_to_discriminator.end())
            variant_col.deserializeBinaryIntoVariant(it->second, getVariantSerialization(type, type_name), buf, getFormatSettings());
        /// Otherwise just insert it into our shared variant.
        else
            variant_col.insertIntoVariantFrom(getSharedVariantDiscriminator(), src_shared_variant, src_offset);

        return;
    }

    /// If variants are different, we need to extend our variant with new variants.
    if (auto * global_discriminators_mapping = combineVariants(dynamic_src.variant_info))
    {
        variant_col.insertFrom(*dynamic_src.variant_column, n, *global_discriminators_mapping);
        return;
    }

    /// We cannot combine 2 Variant types as total number of variants exceeds the limit.
    /// We need to insert single value, try to add only corresponding variant.

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
    /// Insert this value into shared variant.
    insertValueIntoSharedVariant(
        src_variant_col.getVariantByGlobalDiscriminator(src_global_discr),
        variant_type,
        dynamic_src.variant_info.variant_names[src_global_discr],
        src_offset);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnDynamic::insertRangeFrom(const IColumn & src_, size_t start, size_t length)
#else
void ColumnDynamic::doInsertRangeFrom(const IColumn & src_, size_t start, size_t length)
#endif
{
    if (start + length > src_.size())
        throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND, "Parameter out of bound in ColumnDynamic::insertRangeFrom method. "
                                                            "[start({}) + length({}) > src.size()({})]", start, length, src_.size());

    const auto & dynamic_src = assert_cast<const ColumnDynamic &>(src_);
    auto & variant_col = getVariantColumn();

    /// Check if we have the same variants in both columns.
    if (variant_info.variant_names == dynamic_src.variant_info.variant_names)
    {
        variant_col.insertRangeFrom(*dynamic_src.variant_column, start, length);
        return;
    }

    /// If variants are different, we need to extend our variant with new variants.
    if (auto * global_discriminators_mapping = combineVariants(dynamic_src.variant_info))
    {
        size_t prev_size = variant_col.size();
        auto shared_variant_discr = getSharedVariantDiscriminator();
        variant_col.insertRangeFrom(*dynamic_src.variant_column, start, length, *global_discriminators_mapping, shared_variant_discr);

        /// We should process insertion from src shared variant separately, because it can contain
        /// values that should be extracted into our variants. insertRangeFrom above didn't insert
        /// values into our shared variant (we specified shared_variant_discr as special skip discriminator).

        /// Check if src shared variant is empty, nothing to do in this case.
        if (dynamic_src.getSharedVariant().empty())
            return;

        /// Iterate over src discriminators and process insertion from src shared variant.
        const auto & src_variant_column = dynamic_src.getVariantColumn();
        const auto src_shared_variant_discr = dynamic_src.getSharedVariantDiscriminator();
        const auto src_shared_variant_local_discr = src_variant_column.localDiscriminatorByGlobal(src_shared_variant_discr);
        const auto & src_local_discriminators = src_variant_column.getLocalDiscriminators();
        const auto & src_offsets = src_variant_column.getOffsets();
        const auto & src_shared_variant = assert_cast<const ColumnString &>(src_variant_column.getVariantByLocalDiscriminator(src_shared_variant_local_discr));

        auto & local_discriminators = variant_col.getLocalDiscriminators();
        auto & offsets = variant_col.getOffsets();
        const auto shared_variant_local_discr = variant_col.localDiscriminatorByGlobal(shared_variant_discr);
        auto & shared_variant = assert_cast<ColumnString &>(variant_col.getVariantByLocalDiscriminator(shared_variant_local_discr));
        for (size_t i = 0; i != length; ++i)
        {
            if (src_local_discriminators[start + i] == src_shared_variant_local_discr)
            {
                chassert(local_discriminators[prev_size + i] == shared_variant_local_discr);
                auto value = src_shared_variant.getDataAt(src_offsets[start + i]);
                ReadBufferFromMemory buf(value.data, value.size);
                auto type = decodeDataType(buf);
                auto type_name = type->getName();
                /// Check if we have variant with this type. In this case we should extract
                /// the value from src shared variant and insert it into this variant.
                if (auto it = variant_info.variant_name_to_discriminator.find(type_name); it != variant_info.variant_name_to_discriminator.end())
                {
                    auto local_discr = variant_col.localDiscriminatorByGlobal(it->second);
                    auto & variant = variant_col.getVariantByLocalDiscriminator(local_discr);
                    getVariantSerialization(type, type_name)->deserializeBinary(variant, buf, getFormatSettings());
                    /// Local discriminators were already filled in ColumnVariant::insertRangeFrom and this row should contain
                    /// shared_variant_local_discr. Change it to local discriminator of the found variant and update offsets.
                    local_discriminators[prev_size + i] = local_discr;
                    offsets[prev_size + i] = variant.size() - 1;
                }
                /// Otherwise, insert this value into shared variant.
                else
                {
                    shared_variant.insertData(value.data, value.size);
                    /// Update variant offset.
                    offsets[prev_size + i] = shared_variant.size() - 1;
                }
            }
        }

        return;
    }

    /// We cannot combine 2 Variant types as total number of variants exceeds the limit.
    /// In this case we will add most frequent variants and insert them as usual,
    /// all other variants will be inserted into shared variant.
    const auto & src_variants = assert_cast<const DataTypeVariant &>(*dynamic_src.variant_info.variant_type).getVariants();
    /// Mapping from global discriminators of src_variant to the new variant we will create.
    std::vector<ColumnVariant::Discriminator> other_to_new_discriminators;
    other_to_new_discriminators.reserve(dynamic_src.variant_info.variant_names.size());

    /// Check if we cannot add any more new variants. In this case we will insert all new variants into shared variant.
    if (!canAddNewVariant())
    {
        auto shared_variant_discr = getSharedVariantDiscriminator();
        for (const auto & variant_name : dynamic_src.variant_info.variant_names)
        {
            auto it = variant_info.variant_name_to_discriminator.find(variant_name);
            if (it == variant_info.variant_name_to_discriminator.end())
                other_to_new_discriminators.push_back(shared_variant_discr);
            else
                other_to_new_discriminators.push_back(it->second);
        }
    }
    /// We still can add some new variants, but not all of them. Let's choose the most frequent variants.
    else
    {
        /// Create list of pairs <size, discriminator> and sort it.
        std::vector<std::pair<size_t, ColumnVariant::Discriminator>> new_variants_with_sizes;
        new_variants_with_sizes.reserve(dynamic_src.variant_info.variant_names.size());
        const auto & src_variant_column = dynamic_src.getVariantColumn();
        for (const auto & [name, discr] : dynamic_src.variant_info.variant_name_to_discriminator)
        {
            if (!variant_info.variant_name_to_discriminator.contains(name))
                new_variants_with_sizes.emplace_back(src_variant_column.getVariantByGlobalDiscriminator(discr).size(), discr);
        }

        std::sort(new_variants_with_sizes.begin(), new_variants_with_sizes.end(), std::greater());
        DataTypes new_variants = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariants();
        /// Add new variants from sorted list until we reach max_dynamic_types.
        for (const auto & [_, discr] : new_variants_with_sizes)
        {
            if (!canAddNewVariant(new_variants.size()))
                break;
            new_variants.push_back(src_variants[discr]);
        }

        auto new_variant_type = std::make_shared<DataTypeVariant>(new_variants);
        updateVariantInfoAndExpandVariantColumn(new_variant_type);
        auto shared_variant_discr = getSharedVariantDiscriminator();
        for (const auto & variant_name : dynamic_src.variant_info.variant_names)
        {
            auto it = variant_info.variant_name_to_discriminator.find(variant_name);
            if (it == variant_info.variant_name_to_discriminator.end())
                other_to_new_discriminators.push_back(shared_variant_discr);
            else
                other_to_new_discriminators.push_back(it->second);
        }
    }

    /// Iterate over the range and perform insertion.
    const auto & src_variant_column = dynamic_src.getVariantColumn();
    const auto & src_local_discriminators = src_variant_column.getLocalDiscriminators();
    const auto & src_offsets = src_variant_column.getOffsets();
    const auto & src_variant_columns = src_variant_column.getVariants();
    const auto src_shared_variant_discr = dynamic_src.getSharedVariantDiscriminator();
    const auto src_shared_variant_local_discr = src_variant_column.localDiscriminatorByGlobal(src_shared_variant_discr);
    const auto & src_shared_variant = assert_cast<const ColumnString &>(*src_variant_columns[src_shared_variant_local_discr]);
    auto & local_discriminators = variant_col.getLocalDiscriminators();
    local_discriminators.reserve(local_discriminators.size() + length);
    auto & offsets = variant_col.getOffsets();
    offsets.reserve(offsets.size() + length);
    auto & variant_columns = variant_col.getVariants();
    const auto shared_variant_discr = getSharedVariantDiscriminator();
    const auto shared_variant_local_discr = variant_col.localDiscriminatorByGlobal(shared_variant_discr);
    auto & shared_variant = assert_cast<ColumnString &>(*variant_columns[shared_variant_local_discr]);
    size_t end = start + length;
    for (size_t i = start; i != end; ++i)
    {
        auto src_local_discr = src_local_discriminators[i];
        auto src_offset = src_offsets[i];
        if (src_local_discr == ColumnVariant::NULL_DISCRIMINATOR)
        {
            local_discriminators.push_back(ColumnVariant::NULL_DISCRIMINATOR);
            offsets.emplace_back();
        }
        else
        {
            /// Process insertion from src shared variant separately.
            if (src_local_discr == src_shared_variant_local_discr)
            {
                auto value = src_shared_variant.getDataAt(src_offset);
                ReadBufferFromMemory buf(value.data, value.size);
                auto type = decodeDataType(buf);
                auto type_name = type->getName();
                /// Check if we have variant with this type. In this case we should extract
                /// the value from src shared variant and insert it into this variant.
                if (auto it = variant_info.variant_name_to_discriminator.find(type_name); it != variant_info.variant_name_to_discriminator.end())
                {
                    auto local_discr = variant_col.localDiscriminatorByGlobal(it->second);
                    getVariantSerialization(type, type_name)->deserializeBinary(*variant_columns[local_discr], buf, getFormatSettings());
                    local_discriminators.push_back(local_discr);
                    offsets.push_back(variant_columns[local_discr]->size() - 1);
                }
                /// Otherwise, insert this value into shared variant.
                else
                {
                    shared_variant.insertData(value.data, value.size);
                    local_discriminators.push_back(shared_variant_local_discr);
                    offsets.push_back(shared_variant.size() - 1);
                }
            }
            /// Insertion from usual variant.
            else
            {
                auto src_global_discr = src_variant_column.globalDiscriminatorByLocal(src_local_discr);
                auto global_discr = other_to_new_discriminators[src_global_discr];
                /// Check if we need to insert this value into shared variant.
                if (global_discr == shared_variant_discr)
                {
                    serializeValueIntoSharedVariant(
                        shared_variant,
                        *src_variant_columns[src_local_discr],
                        src_variants[src_global_discr],
                        getVariantSerialization(src_variants[src_global_discr], dynamic_src.variant_info.variant_names[src_global_discr]),
                        src_offset);
                    local_discriminators.push_back(shared_variant_local_discr);
                    offsets.push_back(shared_variant.size() - 1);
                }
                else
                {
                    auto local_discr = variant_col.localDiscriminatorByGlobal(global_discr);
                    variant_columns[local_discr]->insertFrom(*src_variant_columns[src_local_discr], src_offset);
                    local_discriminators.push_back(local_discr);
                    offsets.push_back(variant_columns[local_discr]->size() - 1);
                }
            }
        }
    }
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnDynamic::insertManyFrom(const IColumn & src_, size_t position, size_t length)
#else
void ColumnDynamic::doInsertManyFrom(const IColumn & src_, size_t position, size_t length)
#endif
{
    const auto & dynamic_src = assert_cast<const ColumnDynamic &>(src_);
    auto & variant_col = getVariantColumn();

    /// Check if we have the same variants in both columns.
    if (variant_info.variant_names == dynamic_src.variant_info.variant_names)
    {
        variant_col.insertManyFrom(*dynamic_src.variant_column, position, length);
        return;
    }

    const auto & src_variant_col = assert_cast<const ColumnVariant &>(*dynamic_src.variant_column);
    auto src_global_discr = src_variant_col.globalDiscriminatorAt(position);
    auto src_offset = src_variant_col.offsetAt(position);

    /// Check if we insert from shared variant and process it separately.
    if (src_global_discr == dynamic_src.getSharedVariantDiscriminator())
    {
        const auto & src_shared_variant = dynamic_src.getSharedVariant();
        auto value = src_shared_variant.getDataAt(src_offset);
        /// Decode data type of this value.
        ReadBufferFromMemory buf(value.data, value.size);
        auto type = decodeDataType(buf);
        auto type_name = type->getName();
        /// Check if we have this variant and deserialize value into variant from shared variant data.
        if (auto it = variant_info.variant_name_to_discriminator.find(type_name); it != variant_info.variant_name_to_discriminator.end())
        {
            /// Deserialize value into temporary column and use it in insertManyIntoVariantFrom.
            auto tmp_column = type->createColumn();
            tmp_column->reserve(1);
            getVariantSerialization(type, type_name)->deserializeBinary(*tmp_column, buf, getFormatSettings());
            variant_col.insertManyIntoVariantFrom(it->second, *tmp_column, 0, length);
        }
        /// Otherwise just insert it into our shared variant.
        else
        {
            variant_col.insertManyIntoVariantFrom(getSharedVariantDiscriminator(), src_shared_variant, src_offset, length);
        }

        return;
    }

    /// If variants are different, we need to extend our variant with new variants.
    if (auto * global_discriminators_mapping = combineVariants(dynamic_src.variant_info))
    {
        variant_col.insertManyFrom(*dynamic_src.variant_column, position, length, *global_discriminators_mapping);
        return;
    }

    /// We cannot combine 2 Variant types as total number of variants exceeds the limit.
    /// We need to insert single value, try to add only corresponding variant.
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

    /// We reached maximum number of variants and couldn't add new variant.
    /// Insert this value into shared variant.
    /// Create temporary string column, serialize value into it and use it in insertManyIntoVariantFrom.
    auto tmp_shared_variant = ColumnString::create();
    serializeValueIntoSharedVariant(
        *tmp_shared_variant,
        src_variant_col.getVariantByGlobalDiscriminator(src_global_discr),
        variant_type,
        getVariantSerialization(variant_type, dynamic_src.variant_info.variant_names[src_global_discr]),
        src_offset);

    variant_col.insertManyIntoVariantFrom(getSharedVariantDiscriminator(), *tmp_shared_variant, 0, length);
}

void ColumnDynamic::insertValueIntoSharedVariant(const IColumn & src, const DataTypePtr & type, const String & type_name, size_t n)
{
    auto & variant_col = getVariantColumn();
    auto & shared_variant = getSharedVariant();
    serializeValueIntoSharedVariant(shared_variant, src, type, getVariantSerialization(type, type_name), n);
    variant_col.getLocalDiscriminators().push_back(variant_col.localDiscriminatorByGlobal(getSharedVariantDiscriminator()));
    variant_col.getOffsets().push_back(shared_variant.size() - 1);
}

void ColumnDynamic::serializeValueIntoSharedVariant(
    ColumnString & shared_variant,
    const IColumn & src,
    const DataTypePtr & type,
    const SerializationPtr & serialization,
    size_t n)
{
    auto & chars = shared_variant.getChars();
    WriteBufferFromVector<ColumnString::Chars> value_buf(chars, AppendModeTag());
    encodeDataType(type, value_buf);
    serialization->serializeBinary(src, n, value_buf, getFormatSettings());
    value_buf.finalize();
    chars.push_back(0);
    shared_variant.getOffsets().push_back(chars.size());
}

StringRef ColumnDynamic::serializeValueIntoArena(size_t n, Arena & arena, const char *& begin) const
{
    /// We cannot use Variant serialization here as it serializes discriminator + value,
    /// but Dynamic doesn't have fixed mapping discriminator <-> variant type
    /// as different Dynamic column can have different Variants.
    /// Instead, we serialize null bit + variant type and value in binary format (size + data).
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

    WriteBufferFromOwnString buf;
    StringRef type_and_value;
    /// If we have value from shared variant, it's already stored in the desired format.
    if (discr == getSharedVariantDiscriminator())
    {
        type_and_value = getSharedVariant().getDataAt(variant_col.offsetAt(n));
    }
    /// For regular variants serialize its type and value in binary format.
    else
    {
        const auto & variant_type = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariant(discr);
        encodeDataType(variant_type, buf);
        variant_type->getDefaultSerialization()->serializeBinary(variant_col.getVariantByGlobalDiscriminator(discr), variant_col.offsetAt(n), buf, getFormatSettings());
        type_and_value = buf.str();
    }

    char * pos = arena.allocContinue(sizeof(UInt8) + sizeof(size_t) + type_and_value.size, begin);
    memcpy(pos, &null_bit, sizeof(UInt8));
    memcpy(pos + sizeof(UInt8), &type_and_value.size, sizeof(size_t));
    memcpy(pos + sizeof(UInt8) + sizeof(size_t), type_and_value.data, type_and_value.size);
    res.data = pos;
    res.size = sizeof(UInt8) + sizeof(size_t) + type_and_value.size;
    return res;
}

const char * ColumnDynamic::deserializeAndInsertFromArena(const char * pos)
{
    auto & variant_col = getVariantColumn();
    UInt8 null_bit = unalignedLoad<UInt8>(pos);
    pos += sizeof(UInt8);
    if (null_bit)
    {
        insertDefault();
        return pos;
    }

    /// Read variant type and value in binary format.
    const size_t type_and_value_size = unalignedLoad<size_t>(pos);
    pos += sizeof(type_and_value_size);
    std::string_view type_and_value(pos, type_and_value_size);
    pos += type_and_value_size;

    ReadBufferFromMemory buf(type_and_value.data(), type_and_value.size());
    auto variant_type = decodeDataType(buf);
    auto variant_name = variant_type->getName();
    /// If we already have such variant, just deserialize it into corresponding variant column.
    auto it = variant_info.variant_name_to_discriminator.find(variant_name);
    if (it != variant_info.variant_name_to_discriminator.end())
    {
        variant_col.deserializeBinaryIntoVariant(it->second, getVariantSerialization(variant_type, variant_name), buf, getFormatSettings());
    }
    /// If we don't have such variant, try to add it.
    else if (likely(addNewVariant(variant_type)))
    {
        auto discr = variant_info.variant_name_to_discriminator[variant_name];
        variant_col.deserializeBinaryIntoVariant(discr, getVariantSerialization(variant_type, variant_name), buf, getFormatSettings());
    }
    /// Otherwise insert this value into shared variant.
    else
    {
        auto & shared_variant = getSharedVariant();
        shared_variant.insertData(type_and_value.data(), type_and_value.size());
        variant_col.getLocalDiscriminators().push_back(variant_col.localDiscriminatorByGlobal(getSharedVariantDiscriminator()));
        variant_col.getOffsets().push_back(shared_variant.size() - 1);
    }

    return pos;
}

const char * ColumnDynamic::skipSerializedInArena(const char * pos) const
{
    UInt8 null_bit = unalignedLoad<UInt8>(pos);
    pos += sizeof(UInt8);
    if (null_bit)
        return pos;

    const size_t type_and_value_size = unalignedLoad<size_t>(pos);
    pos += sizeof(type_and_value_size);
    pos += type_and_value_size;
    return pos;
}

void ColumnDynamic::updateHashWithValue(size_t n, SipHash & hash) const
{
    const auto & variant_col = getVariantColumn();
    auto discr = variant_col.globalDiscriminatorAt(n);
    if (discr == ColumnVariant::NULL_DISCRIMINATOR)
    {
        hash.update(discr);
        return;
    }

    /// If it's not null we update hash with the type name and the actual value.

    /// If value in this row is in shared variant, deserialize type and value and
    /// update hash with it.
    if (discr == getSharedVariantDiscriminator())
    {
        auto value = getSharedVariant().getDataAt(variant_col.offsetAt(n));
        ReadBufferFromMemory buf(value.data, value.size);
        auto type = decodeDataType(buf);
        hash.update(type->getName());
        auto tmp_column = type->createColumn();
        type->getDefaultSerialization()->deserializeBinary(*tmp_column, buf, getFormatSettings());
        tmp_column->updateHashWithValue(0, hash);
        return;
    }

    hash.update(variant_info.variant_names[discr]);
    variant_col.getVariantByGlobalDiscriminator(discr).updateHashWithValue(variant_col.offsetAt(n), hash);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
int ColumnDynamic::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#else
int ColumnDynamic::doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#endif
{
    const auto & left_variant = getVariantColumn();
    const auto & right_dynamic = assert_cast<const ColumnDynamic &>(rhs);
    const auto & right_variant = right_dynamic.getVariantColumn();

    auto left_discr = left_variant.globalDiscriminatorAt(n);
    auto left_shared_variant_discr = getSharedVariantDiscriminator();
    auto right_discr = right_variant.globalDiscriminatorAt(m);
    auto right_shared_variant_discr = right_dynamic.getSharedVariantDiscriminator();

    /// Check if we have NULLs and return result based on nan_direction_hint.
    if (left_discr == ColumnVariant::NULL_DISCRIMINATOR && right_discr == ColumnVariant::NULL_DISCRIMINATOR)
        return 0;
    if (left_discr == ColumnVariant::NULL_DISCRIMINATOR)
        return nan_direction_hint;
    if (right_discr == ColumnVariant::NULL_DISCRIMINATOR)
        return -nan_direction_hint;

    /// Check if both values are in shared variant.
    if (left_discr == left_shared_variant_discr && right_discr == right_shared_variant_discr)
    {
        /// First check if both type and value are equal.
        auto left_value = getSharedVariant().getDataAt(left_variant.offsetAt(n));
        auto right_value = right_dynamic.getSharedVariant().getDataAt(right_variant.offsetAt(m));
        if (left_value == right_value)
            return 0;

        /// Extract type names from both values.
        ReadBufferFromMemory buf_left(left_value.data, left_value.size);
        auto left_data_type = decodeDataType(buf_left);
        auto left_data_type_name = left_data_type->getName();

        ReadBufferFromMemory buf_right(right_value.data, right_value.size);
        auto right_data_type = decodeDataType(buf_right);
        auto right_data_type_name = right_data_type->getName();

        /// If rows have different types, we compare type names.
        if (left_data_type_name != right_data_type_name)
            return left_data_type_name < right_data_type_name ? -1 : 1;

        /// If rows have the same type, we compare actual values.
        /// We have both values serialized in binary format, so we need to
        /// create temporary column, insert both values into it and compare.
        auto tmp_column = left_data_type->createColumn();
        const auto & serialization = left_data_type->getDefaultSerialization();
        serialization->deserializeBinary(*tmp_column, buf_left, getFormatSettings());
        serialization->deserializeBinary(*tmp_column, buf_right, getFormatSettings());
        return tmp_column->compareAt(0, 1, *tmp_column, nan_direction_hint);
    }
    /// Check if only left value is in shared data.
    if (left_discr == left_shared_variant_discr)
    {
        /// Extract left type name from the value.
        auto left_value = getSharedVariant().getDataAt(left_variant.offsetAt(n));
        ReadBufferFromMemory buf_left(left_value.data, left_value.size);
        auto left_data_type = decodeDataType(buf_left);
        auto left_data_type_name = left_data_type->getName();

        /// If rows have different types, we compare type names.
        if (left_data_type_name != right_dynamic.variant_info.variant_names[right_discr])
            return left_data_type_name < right_dynamic.variant_info.variant_names[right_discr] ? -1 : 1;

        /// If rows have the same type, we compare actual values.
        /// We have left value serialized in binary format, we need to
        /// create temporary column, insert the value into it and compare.
        auto tmp_column = left_data_type->createColumn();
        left_data_type->getDefaultSerialization()->deserializeBinary(*tmp_column, buf_left, getFormatSettings());
        return tmp_column->compareAt(
            0, right_variant.offsetAt(m), right_variant.getVariantByGlobalDiscriminator(right_discr), nan_direction_hint);
    }
    /// Check if only right value is in shared data.
    if (right_discr == right_shared_variant_discr)
    {
        /// Extract right type name from the value.
        auto right_value = right_dynamic.getSharedVariant().getDataAt(right_variant.offsetAt(m));
        ReadBufferFromMemory buf_right(right_value.data, right_value.size);
        auto right_data_type = decodeDataType(buf_right);
        auto right_data_type_name = right_data_type->getName();

        /// If rows have different types, we compare type names.
        if (variant_info.variant_names[left_discr] != right_data_type_name)
            return variant_info.variant_names[left_discr] < right_data_type_name ? -1 : 1;

        /// If rows have the same type, we compare actual values.
        /// We have right value serialized in binary format, we need to
        /// create temporary column, insert the value into it and compare.
        auto tmp_column = right_data_type->createColumn();
        right_data_type->getDefaultSerialization()->deserializeBinary(*tmp_column, buf_right, getFormatSettings());
        return left_variant.getVariantByGlobalDiscriminator(left_discr)
            .compareAt(left_variant.offsetAt(n), 0, *tmp_column, nan_direction_hint);
    }
    /// Otherwise both values are regular variants.

    /// If rows have different types, we compare type names.
    if (variant_info.variant_names[left_discr] != right_dynamic.variant_info.variant_names[right_discr])
        return variant_info.variant_names[left_discr] < right_dynamic.variant_info.variant_names[right_discr] ? -1 : 1;

    /// If rows have the same types, compare actual values from corresponding variants.
    return left_variant.getVariantByGlobalDiscriminator(left_discr)
        .compareAt(
            left_variant.offsetAt(n),
            right_variant.offsetAt(m),
            right_variant.getVariantByGlobalDiscriminator(right_discr),
            nan_direction_hint);
}

struct ColumnDynamic::ComparatorBase
{
    const ColumnDynamic & parent;
    int nan_direction_hint;

    ComparatorBase(const ColumnDynamic & parent_, int nan_direction_hint_)
        : parent(parent_), nan_direction_hint(nan_direction_hint_)
    {
    }

    ALWAYS_INLINE int compare(size_t lhs, size_t rhs) const
    {
        return parent.compareAt(lhs, rhs, parent, nan_direction_hint);
    }
};

void ColumnDynamic::getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability, size_t limit, int nan_direction_hint, IColumn::Permutation & res) const
{
    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorAscendingUnstable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorAscendingStable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorDescendingUnstable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorDescendingStable(*this, nan_direction_hint), DefaultSort(), DefaultPartialSort());
}

void ColumnDynamic::updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability, size_t limit, int nan_direction_hint, IColumn::Permutation & res, DB::EqualRanges & equal_ranges) const
{
    auto comparator_equal = ComparatorEqual(*this, nan_direction_hint);

    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingUnstable(*this, nan_direction_hint), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingStable(*this, nan_direction_hint), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingUnstable(*this, nan_direction_hint), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingStable(*this, nan_direction_hint), comparator_equal, DefaultSort(), DefaultPartialSort());
}

ColumnPtr ColumnDynamic::compress() const
{
    ColumnPtr variant_compressed = variant_column_ptr->compress();
    size_t byte_size = variant_compressed->byteSize();
    return ColumnCompressed::create(size(), byte_size,
        [my_variant_compressed = std::move(variant_compressed), my_variant_info = variant_info, my_max_dynamic_types = max_dynamic_types, my_global_max_dynamic_types = global_max_dynamic_types, my_statistics = statistics]() mutable
        {
            return ColumnDynamic::create(my_variant_compressed->decompress(), my_variant_info, my_max_dynamic_types, my_global_max_dynamic_types, my_statistics);
        });
}

String ColumnDynamic::getTypeNameAt(size_t row_num) const
{
    const auto & variant_col = getVariantColumn();
    const size_t discr = variant_col.globalDiscriminatorAt(row_num);
    if (discr == ColumnVariant::NULL_DISCRIMINATOR)
        return "";

    if (discr == getSharedVariantDiscriminator())
    {
        const auto value = getSharedVariant().getDataAt(variant_col.offsetAt(row_num));
        ReadBufferFromMemory buf(value.data, value.size);
        return decodeDataType(buf)->getName();
    }

    return variant_info.variant_names[discr];
}

void ColumnDynamic::getAllTypeNamesInto(std::unordered_set<String> & names) const
{
    auto shared_variant_discr = getSharedVariantDiscriminator();
    for (size_t i = 0; i != variant_info.variant_names.size(); ++i)
    {
        if (i != shared_variant_discr && !variant_column_ptr->getVariantByGlobalDiscriminator(i).empty())
            names.insert(variant_info.variant_names[i]);
    }

    const auto & shared_variant = getSharedVariant();
    for (size_t i = 0; i != shared_variant.size(); ++i)
    {
        const auto value = shared_variant.getDataAt(i);
        ReadBufferFromMemory buf(value.data, value.size);
        names.insert(decodeDataType(buf)->getName());
    }
}

void ColumnDynamic::prepareForSquashing(const Columns & source_columns)
{
    if (source_columns.empty())
        return;

    /// Internal variants of source dynamic columns may differ.
    /// We want to preallocate memory for all variants we will have after squashing.
    /// It may happen that the total number of variants in source columns will
    /// exceed the limit, in this case we will choose the most frequent variants
    /// and insert the rest types into the shared variant.

    /// First, preallocate memory for variant discriminators and offsets.
    size_t new_size = size();
    for (const auto & source_column : source_columns)
        new_size += source_column->size();
    auto & variant_col = getVariantColumn();
    variant_col.getLocalDiscriminators().reserve_exact(new_size);
    variant_col.getOffsets().reserve_exact(new_size);

    /// Second, preallocate memory for variants.
    prepareVariantsForSquashing(source_columns);
}

void ColumnDynamic::prepareVariantsForSquashing(const Columns & source_columns)
{
    /// Internal variants of source dynamic columns may differ.
    /// We want to preallocate memory for all variants we will have after squashing.
    /// It may happen that the total number of variants in source columns will
    /// exceed the limit, in this case we will choose the most frequent variants.

    /// Collect all variants and their total sizes.
    std::unordered_map<String, size_t> total_variant_sizes;
    DataTypes all_variants;

    auto add_variants = [&](const ColumnDynamic & source_dynamic)
    {
        const auto & source_variant_column = source_dynamic.getVariantColumn();
        const auto & source_variant_info = source_dynamic.getVariantInfo();
        const auto & source_variants = assert_cast<const DataTypeVariant &>(*source_variant_info.variant_type).getVariants();

        for (size_t i = 0; i != source_variants.size(); ++i)
        {
            const auto & variant_name = source_variant_info.variant_names[i];
            auto it = total_variant_sizes.find(variant_name);
            /// Add this variant to the list of all variants if we didn't see it yet.
            if (it == total_variant_sizes.end())
            {
                all_variants.push_back(source_variants[i]);
                it = total_variant_sizes.emplace(variant_name, 0).first;
            }

            it->second += source_variant_column.getVariantByGlobalDiscriminator(i).size();
        }
    };

    for (const auto & source_column : source_columns)
        add_variants(assert_cast<const ColumnDynamic &>(*source_column));

    /// Add variants from this dynamic column.
    add_variants(*this);

    DataTypePtr result_variant_type;
    /// Check if the number of all variants exceeds the limit.
    if (!canAddNewVariants(0, all_variants.size()))
    {
        /// We want to keep the most frequent variants in the resulting dynamic column.
        DataTypes result_variants;
        result_variants.reserve(max_dynamic_types + 1); /// +1 for shared variant.
        /// Add variants from current variant column as we will not rewrite it.
        for (const auto & variant : assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariants())
            result_variants.push_back(variant);

        /// Create list of remaining variants with their sizes and sort it.
        std::vector<std::pair<size_t, DataTypePtr>> variants_with_sizes;
        variants_with_sizes.reserve(all_variants.size() - variant_info.variant_names.size());
        for (const auto & variant : all_variants)
        {
            /// Add variant to the list only of we didn't add it yet.
            auto variant_name = variant->getName();
            if (!variant_info.variant_name_to_discriminator.contains(variant_name))
                variants_with_sizes.emplace_back(total_variant_sizes[variant_name], variant);
        }

        std::sort(variants_with_sizes.begin(), variants_with_sizes.end(), std::greater());
        /// Add the most frequent variants until we reach max_dynamic_types.
        for (const auto & [_, new_variant] : variants_with_sizes)
        {
            if (!canAddNewVariant(result_variants.size()))
                break;
            result_variants.push_back(new_variant);
        }

        result_variant_type = std::make_shared<DataTypeVariant>(result_variants);
    }
    else
    {
        result_variant_type = std::make_shared<DataTypeVariant>(all_variants);
    }

    if (!result_variant_type->equals(*variant_info.variant_type))
        updateVariantInfoAndExpandVariantColumn(result_variant_type);

    /// Now current dynamic column has all resulting variants and we can call
    /// prepareForSquashing on them to preallocate the memory.
    auto & variant_col = getVariantColumn();
    for (size_t i = 0; i != variant_info.variant_names.size(); ++i)
    {
        Columns source_variant_columns;
        source_variant_columns.reserve(source_columns.size());
        for (const auto & source_column : source_columns)
        {
            const auto & source_dynamic_column = assert_cast<const ColumnDynamic &>(*source_column);
            const auto & source_variant_info = source_dynamic_column.getVariantInfo();
            /// Try to find this variant in the current source column.
            auto it = source_variant_info.variant_name_to_discriminator.find(variant_info.variant_names[i]);
            if (it != source_variant_info.variant_name_to_discriminator.end())
                source_variant_columns.push_back(source_dynamic_column.getVariantColumn().getVariantPtrByGlobalDiscriminator(it->second));
        }

        variant_col.getVariantByGlobalDiscriminator(i).prepareForSquashing(source_variant_columns);
    }
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
    /// Add shared variant type in advance;
    all_variants.push_back(getSharedVariantDataType());
    total_sizes[getSharedVariantTypeName()] = 0;

    for (const auto & source_column : source_columns)
    {
        const auto & source_dynamic = assert_cast<const ColumnDynamic &>(*source_column);
        const auto & source_variant_column = source_dynamic.getVariantColumn();
        const auto & source_variant_info = source_dynamic.getVariantInfo();
        const auto & source_variants = assert_cast<const DataTypeVariant &>(*source_variant_info.variant_type).getVariants();
        /// During deserialization from MergeTree we will have variant sizes statistics from the whole data part.
        const auto & source_statistics = source_dynamic.getStatistics();
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
            size_t size = source_variant_column.getVariantByGlobalDiscriminator(i).size();
            if (source_statistics)
            {
                auto statistics_it = source_statistics->variants_statistics.find(variant_name);
                if (statistics_it != source_statistics->variants_statistics.end())
                    size = statistics_it->second;
            }

            it->second += size;
        }

        /// Use add variants from shared variant statistics. It can help extracting
        /// frequent variants from shared variant to usual variants.
        if (source_statistics)
        {
            for (const auto & [variant_name, size] : source_statistics->shared_variants_statistics)
            {
                auto it = total_sizes.find(variant_name);
                /// Add this variant to the list of all variants if we didn't see it yet.
                if (it == total_sizes.end())
                {
                    all_variants.push_back(DataTypeFactory::instance().get(variant_name));
                    it = total_sizes.emplace(variant_name, 0).first;
                }
                it->second += size;
            }
        }
    }

    DataTypePtr result_variant_type;
    Statistics new_statistics(Statistics::Source::MERGE);
    /// Reset max_dynamic_types to global_max_dynamic_types.
    max_dynamic_types = global_max_dynamic_types;
    /// Check if the number of all dynamic types exceeds the limit.
    if (!canAddNewVariants(0, all_variants.size()))
    {
        /// Create a list of variants with their sizes and names and then sort it.
        std::vector<std::tuple<size_t, String, DataTypePtr>> variants_with_sizes;
        variants_with_sizes.reserve(all_variants.size());
        for (const auto & variant : all_variants)
        {
            auto variant_name = variant->getName();
            if (variant_name != getSharedVariantTypeName())
                variants_with_sizes.emplace_back(total_sizes[variant_name], variant_name, variant);
        }
        std::sort(variants_with_sizes.begin(), variants_with_sizes.end(), std::greater());

        /// Take first max_dynamic_types variants from sorted list and fill shared_variants_statistics with the rest.
        DataTypes result_variants;
        result_variants.reserve(max_dynamic_types + 1); /// +1 for shared variant.
        /// Add shared variant.
        result_variants.push_back(getSharedVariantDataType());
        for (const auto & [size, variant_name, variant_type] : variants_with_sizes)
        {
            /// Add variant to the resulting variants list until we reach max_dynamic_types.
            if (canAddNewVariant(result_variants.size()))
                result_variants.push_back(variant_type);
            /// Add all remaining variants into shared_variants_statistics until we reach its max size.
            else if (new_statistics.shared_variants_statistics.size() < Statistics::MAX_SHARED_VARIANT_STATISTICS_SIZE)
                new_statistics.shared_variants_statistics[variant_name] = size;
            else
                break;
        }

        result_variant_type = std::make_shared<DataTypeVariant>(result_variants);
    }
    else
    {
        result_variant_type = std::make_shared<DataTypeVariant>(all_variants);
    }

    /// Now we have resulting Variant and can fill variant info and create merge statistics.
    setVariantType(result_variant_type);
    new_statistics.variants_statistics.reserve(variant_info.variant_names.size());
    for (const auto & variant_name : variant_info.variant_names)
        new_statistics.variants_statistics[variant_name] = total_sizes[variant_name];
    statistics = std::make_shared<const Statistics>(std::move(new_statistics));

    /// Reduce max_dynamic_types to the number of selected variants, so there will be no possibility
    /// to extend selected variants on inerts into this column during merges.
    /// -1 because we don't count shared variant in the limit.
    max_dynamic_types = variant_info.variant_names.size() - 1;

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
            if (it != source_variant_info.variant_name_to_discriminator.end())        /// Add shared variant.
                variants_source_columns[i].push_back(source_dynamic_column.getVariantColumn().getVariantPtrByGlobalDiscriminator(it->second));
        }
    }

    auto & variant_col = getVariantColumn();
    for (size_t i = 0; i != variant_info.variant_names.size(); ++i)
        variant_col.getVariantByGlobalDiscriminator(i).takeDynamicStructureFromSourceColumns(variants_source_columns[i]);
}

void ColumnDynamic::applyNullMap(const ColumnVector<UInt8>::Container & null_map)
{
    variant_column_ptr->applyNullMap(null_map);
}

void ColumnDynamic::applyNegatedNullMap(const ColumnVector<UInt8>::Container & null_map)
{
    variant_column_ptr->applyNegatedNullMap(null_map);
}

}
