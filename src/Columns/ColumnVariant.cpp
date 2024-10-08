#include <Columns/ColumnVariant.h>

#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnsCommon.h>
#include <Core/Field.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Common/WeakHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Common/Arena.h>
#include <Common/SipHash.h>
#include <Common/HashTable/Hash.h>
#include <Columns/MaskOperations.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int SIZES_OF_NESTED_COLUMNS_ARE_INCONSISTENT;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

std::string ColumnVariant::getName() const
{
    WriteBufferFromOwnString res;
    res << "Variant(";
    bool is_first = true;
    for (const auto & local_variant : global_to_local_discriminators)
    {
        if (!is_first)
            res << ", ";
        is_first = false;
        res << variants[local_variant]->getName();
    }
    res << ")";
    return res.str();
}


void ColumnVariant::initIdentityGlobalToLocalDiscriminatorsMapping()
{
    local_to_global_discriminators.reserve(variants.size());
    global_to_local_discriminators.reserve(variants.size());
    for (size_t i = 0; i != variants.size(); ++i)
    {
        local_to_global_discriminators.push_back(i);
        global_to_local_discriminators.push_back(i);
    }
}

ColumnVariant::ColumnVariant(MutableColumns && variants_) : ColumnVariant(std::move(variants_), {})
{
}

ColumnVariant::ColumnVariant(MutableColumns && variants_, const std::vector<Discriminator> & local_to_global_discriminators_)
{
    /// Empty local_to_global_discriminators mapping means that variants are already in the global order.
    if (!local_to_global_discriminators_.empty() && local_to_global_discriminators_.size() != variants_.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The number of values in local_to_global_discriminators mapping ({}) doesn't match the number of variants ({})",
            local_to_global_discriminators_.size(),
            variants_.size());

    /// As variants are empty, column with local discriminators will be also empty and we can reorder variants according to global discriminators.
    variants.resize(variants_.size());
    for (size_t i = 0; i != variants_.size(); ++i)
    {
        if (isColumnConst(*variants_[i]))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ColumnVariant cannot have ColumnConst as its element");

        if (!variants_[i]->empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Not empty column passed to ColumnVariant, but no local_discriminators passed");

        if (!local_to_global_discriminators_.empty() && local_to_global_discriminators_[i] > variants_.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid global discriminator {}. The number of variants: {}", UInt64(local_to_global_discriminators_[i]), variants_.size());

        if (local_to_global_discriminators_.empty())
            variants[i] = std::move(variants_[i]);
        else
            variants[local_to_global_discriminators_[i]] = std::move(variants_[i]);
    }

    local_discriminators = ColumnDiscriminators::create();
    offsets = ColumnOffsets::create();

    /// Now global and local discriminators are the same.
    initIdentityGlobalToLocalDiscriminatorsMapping();
}

ColumnVariant::ColumnVariant(MutableColumnPtr local_discriminators_, MutableColumns && variants_) : ColumnVariant(std::move(local_discriminators_), nullptr, std::move(variants_), {})
{
}

ColumnVariant::ColumnVariant(MutableColumnPtr local_discriminators_, MutableColumns && variants_, const std::vector<Discriminator> & local_to_global_discriminators_) : ColumnVariant(std::move(local_discriminators_), nullptr, std::move(variants_), local_to_global_discriminators_)
{
}

ColumnVariant::ColumnVariant(DB::MutableColumnPtr local_discriminators_, DB::MutableColumnPtr offsets_, DB::MutableColumns && variants_) : ColumnVariant(std::move(local_discriminators_), std::move(offsets_), std::move(variants_), {})
{
}

ColumnVariant::ColumnVariant(DB::MutableColumnPtr local_discriminators_, DB::MutableColumnPtr offsets_, DB::MutableColumns && variants_, const std::vector<Discriminator> & local_to_global_discriminators_)
{
    if (variants_.size() > MAX_NESTED_COLUMNS)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Variant type with more than {} nested types is not allowed", ColumnVariant::MAX_NESTED_COLUMNS);

    local_discriminators = std::move(local_discriminators_);
    const ColumnDiscriminators * discriminators_concrete = typeid_cast<const ColumnDiscriminators *>(local_discriminators.get());
    if (!discriminators_concrete)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "discriminator column must be a ColumnUInt8");

    variants.reserve(variants_.size());
    size_t total_size = 0;
    for (auto & variant : variants_)
    {
        if (isColumnConst(*variant))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ColumnVariant cannot have ColumnConst as its element");

        total_size += variant->size();
        variants.push_back(std::move(variant));
    }

    /// We can have more discriminators than values in columns
    /// (because of NULL discriminators), but not less.
    if (total_size > local_discriminators->size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Nested columns sizes are inconsistent with local_discriminators column size. Total column sizes: {}, local_discriminators size: {}", total_size, local_discriminators->size());

    if (offsets_)
    {
        if (!typeid_cast<const ColumnOffsets *>(offsets_.get()))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "offsets column must be a ColumnUInt64");

        offsets = std::move(offsets_);
    }
    else
    {
        /// If no offsets column was provided, construct offsets based on discriminators.
        offsets = ColumnOffsets::create();
        Offsets & offsets_data = typeid_cast<ColumnOffsets *>(offsets.get())->getData();
        offsets_data.reserve(discriminators_concrete->size());
        /// If we have only NULLs, offsets column will not contain any real offsets.
        if (hasOnlyNulls())
        {
            offsets_data.resize(discriminators_concrete->size());
        }
        /// If we have only one non empty variant and no NULLs,
        /// offsets column will contain just sequential offsets 0, 1, 2, ...
        else if (getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls())
        {
            for (size_t i = 0; i != discriminators_concrete->size(); ++i)
                offsets_data.push_back(i);
        }
        /// Otherwise we should iterate through discriminators and
        /// remember current offset for each variant column.
        else
        {
            std::vector<Offset> nested_offsets;
            nested_offsets.resize(variants.size());
            for (Discriminator discr : discriminators_concrete->getData())
            {
                if (discr == NULL_DISCRIMINATOR)
                    offsets_data.emplace_back();
                else
                    offsets_data.push_back(nested_offsets[discr]++);
            }
        }
    }

    /// Empty global_discriminators means that variants are already in global order.
    if (local_to_global_discriminators_.empty())
    {
        initIdentityGlobalToLocalDiscriminatorsMapping();
    }
    else
    {
        if (local_to_global_discriminators_.size() != variants.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The number of values in local_to_global_discriminators mapping ({}) doesn't match the number of variants ({})",
                local_to_global_discriminators_.size(),
                variants.size());

        local_to_global_discriminators = local_to_global_discriminators_;
        global_to_local_discriminators.resize(local_to_global_discriminators.size());
        /// Create mapping global discriminator -> local discriminator
        for (size_t i = 0; i != local_to_global_discriminators.size(); ++i)
        {
            if (local_to_global_discriminators[i] > variants.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid global discriminator {}. The number of variants: {}", UInt64(local_to_global_discriminators[i]), variants_.size());

            global_to_local_discriminators[local_to_global_discriminators[i]] = i;
        }
    }
}

namespace
{

MutableColumns getVariantsAssumeMutable(const Columns & variants)
{
    MutableColumns mutable_variants;

    for (const auto & variant : variants)
    {
        if (isColumnConst(*variant))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ColumnVariant cannot have ColumnConst as its element");
        mutable_variants.emplace_back(variant->assumeMutable());
    }

    return mutable_variants;
}

}

ColumnVariant::Ptr ColumnVariant::create(const Columns & variants, const std::vector<Discriminator> & local_to_global_discriminators)
{
    return ColumnVariant::create(getVariantsAssumeMutable(variants), local_to_global_discriminators);
}

ColumnVariant::Ptr ColumnVariant::create(const DB::ColumnPtr & local_discriminators, const DB::Columns & variants, const std::vector<Discriminator> & local_to_global_discriminators)
{
    return ColumnVariant::create(local_discriminators->assumeMutable(), getVariantsAssumeMutable(variants), local_to_global_discriminators);
}

ColumnVariant::Ptr ColumnVariant::create(const DB::ColumnPtr & local_discriminators, const DB::ColumnPtr & offsets, const DB::Columns & variants, const std::vector<Discriminator> & local_to_global_discriminators)
{
    return ColumnVariant::create(local_discriminators->assumeMutable(), offsets->assumeMutable(), getVariantsAssumeMutable(variants), local_to_global_discriminators);
}

MutableColumnPtr ColumnVariant::cloneEmpty() const
{
    MutableColumns new_variants;
    new_variants.reserve(variants.size());
    for (const auto & variant : variants)
        new_variants.emplace_back(variant->cloneEmpty());

    return ColumnVariant::create(std::move(new_variants), local_to_global_discriminators);
}

MutableColumnPtr ColumnVariant::cloneResized(size_t new_size) const
{
    if (new_size == 0)
        return cloneEmpty();

    const size_t num_variants = variants.size();
    size_t size = local_discriminators->size();
    /// If new size is bigger than the old one, just clone column and append default values.
    if (new_size >= size)
    {
        MutableColumns new_variants;
        new_variants.reserve(num_variants);
        for (const auto & variant : variants)
            new_variants.emplace_back(IColumn::mutate(variant));

        auto res = ColumnVariant::create(IColumn::mutate(local_discriminators), IColumn::mutate(offsets), std::move(new_variants), local_to_global_discriminators);
        res->insertManyDefaults(new_size - size);
        return res;
    }

    /// If new size is less than current size, we should find the new size for all variants.

    /// Optimization for case when we have only NULLs. In this case we should just resize discriminators and offsets.
    if (hasOnlyNulls())
    {
        MutableColumns new_variants;
        new_variants.reserve(num_variants);
        for (const auto & variant : variants)
            new_variants.emplace_back(IColumn::mutate(variant));

        return ColumnVariant::create(local_discriminators->cloneResized(new_size), offsets->cloneResized(new_size), std::move(new_variants), local_to_global_discriminators);
    }

    /// Optimization for case when there is only 1 non-empty variant and no NULLs.
    /// In this case we can simply call cloneResized on this single variant, discriminators and offsets.
    if (auto non_empty_local_discr = getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls())
    {
        MutableColumns new_variants;
        new_variants.reserve(num_variants);
        for (size_t i = 0; i != variants.size(); ++i)
        {
            if (i == *non_empty_local_discr)
                new_variants.emplace_back(variants[i]->cloneResized(new_size));
            else
                new_variants.emplace_back(variants[i]->cloneEmpty());
        }

        return ColumnVariant::create(local_discriminators->cloneResized(new_size), offsets->cloneResized(new_size), std::move(new_variants), local_to_global_discriminators);
    }

    const auto & local_discriminators_data = getLocalDiscriminators();
    const auto & offsets_data = getOffsets();

    /// We can find all variants sizes by scanning all new_size local_discriminators and calculating sizes for all new variants.
    /// But instead we are trying to optimize it using offsets column:
    /// For all non-empty variants we are trying to find last occurrence of its discriminator in local_discriminators[:new_size] or
    /// first occurrence in local_discriminators[new_size:] depending on what range is smaller. The same row in offsets column will
    /// contain the desired size (or size - 1) of variant.
    /// All empty variants will remain empty.
    /// Not sure how good this optimization is, feel free to remove it and use simpler version without using offsets.

    MutableColumns new_variants(num_variants);
    std::vector<UInt8> seen_variants(num_variants, 0);
    size_t number_of_seen_variants = 0;
    /// First, check which variants are empty. They will remain empty.
    for (Discriminator i = 0; i != num_variants; ++i)
    {
        if (variants[i]->empty())
        {
            seen_variants[i] = 1;
            ++number_of_seen_variants;
            new_variants[i] = variants[i]->cloneEmpty();
        }
    }

    /// Now, choose what range is smaller and use it.
    /// [0, new_size)
    if (2 * new_size <= size)
    {
        for (ssize_t i = new_size - 1; i > -1; --i)
        {
            Discriminator discr = local_discriminators_data[i];
            if (discr != NULL_DISCRIMINATOR)
            {
                /// If this is the first occurrence of this discriminator,
                /// we can get new size for this variant.
                if (!seen_variants[discr])
                {
                    seen_variants[discr] = 1;
                    ++number_of_seen_variants;
                    new_variants[discr] = variants[discr]->cloneResized(offsets_data[i] + 1);
                    /// Break if we found sizes for all variants.
                    if (number_of_seen_variants == num_variants)
                        break;
                }
            }
        }

        /// All variants that weren't found in range [0, new_size] will be empty in the result column.
        if (number_of_seen_variants != num_variants)
        {
            for (size_t discr = 0; discr != num_variants; ++discr)
                if (!seen_variants[discr])
                    new_variants[discr] = variants[discr]->cloneEmpty();
        }
    }
    /// [new_size, size)
    else
    {
        for (size_t i = new_size; i < size; ++i)
        {
            Discriminator discr = local_discriminators_data[i];
            if (discr != NULL_DISCRIMINATOR)
            {
                /// If this is the first occurrence of this discriminator,
                /// we can get new size for this variant.
                if (!seen_variants[discr])
                {
                    seen_variants[discr] = 1;
                    ++number_of_seen_variants;
                    new_variants[discr] = variants[discr]->cloneResized(offsets_data[i]);
                    /// Break if we found sizes for all variants.
                    if (number_of_seen_variants == num_variants)
                        break;
                }
            }
        }

        if (number_of_seen_variants != num_variants)
        {
            /// All variants that weren't found in range [new_size, size) will not change their sizes.
            for (size_t discr = 0; discr != num_variants; ++discr)
                if (!seen_variants[discr])
                    new_variants[discr] = IColumn::mutate(variants[discr]);
        }
    }

    return ColumnVariant::create(local_discriminators->cloneResized(new_size), offsets->cloneResized(new_size), std::move(new_variants), local_to_global_discriminators);
}

Field ColumnVariant::operator[](size_t n) const
{
    Discriminator discr = localDiscriminatorAt(n);
    if (discr == NULL_DISCRIMINATOR)
        return Null();
    return (*variants[discr])[offsetAt(n)];
}

void ColumnVariant::get(size_t n, Field & res) const
{
    Discriminator discr = localDiscriminatorAt(n);
    if (discr == NULL_DISCRIMINATOR)
        res = Null();
    else
        variants[discr]->get(offsetAt(n), res);
}

bool ColumnVariant::isDefaultAt(size_t n) const
{
    return localDiscriminatorAt(n) == NULL_DISCRIMINATOR;
}

bool ColumnVariant::isNullAt(size_t n) const
{
    return localDiscriminatorAt(n) == NULL_DISCRIMINATOR;
}

StringRef ColumnVariant::getDataAt(size_t) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getDataAt is not supported for {}", getName());
}

void ColumnVariant::insertData(const char *, size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method insertData is not supported for {}", getName());
}

void ColumnVariant::insert(const Field & x)
{
    if (!tryInsert(x))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot insert field {} into column {}", toString(x), getName());
}

bool ColumnVariant::tryInsert(const DB::Field & x)
{
    if (x.isNull())
    {
        insertDefault();
        return true;
    }

    for (size_t i = 0; i != variants.size(); ++i)
    {
        if (variants[i]->tryInsert(x))
        {
            getLocalDiscriminators().push_back(i);
            getOffsets().push_back(variants[i]->size() - 1);
            return true;
        }
    }

    return false;
}

void ColumnVariant::insertFromImpl(const DB::IColumn & src_, size_t n, const std::vector<ColumnVariant::Discriminator> * global_discriminators_mapping)
{
    const size_t num_variants = variants.size();
    const ColumnVariant & src = assert_cast<const ColumnVariant &>(src_);

    if (!global_discriminators_mapping && src.variants.size() != num_variants)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert value of Variant type with different number of types");

    Discriminator src_global_discr = src.globalDiscriminatorAt(n);
    Discriminator global_discr = src_global_discr;
    if (global_discriminators_mapping && src_global_discr != NULL_DISCRIMINATOR)
        global_discr = (*global_discriminators_mapping)[src_global_discr];
    Discriminator local_discr = localDiscriminatorByGlobal(global_discr);
    getLocalDiscriminators().push_back(local_discr);
    if (local_discr == NULL_DISCRIMINATOR)
    {
        getOffsets().emplace_back();
    }
    else
    {
        getOffsets().push_back(variants[local_discr]->size());
        variants[local_discr]->insertFrom(src.getVariantByGlobalDiscriminator(src_global_discr), src.offsetAt(n));
    }
}

void ColumnVariant::insertRangeFromImpl(const DB::IColumn & src_, size_t start, size_t length, const std::vector<ColumnVariant::Discriminator> * global_discriminators_mapping, const Discriminator * skip_discriminator)
{
    const size_t num_variants = variants.size();
    const auto & src = assert_cast<const ColumnVariant &>(src_);
    if (!global_discriminators_mapping && src.variants.size() != num_variants)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert value of Variant type with different number of types");

    if (start + length > src.getLocalDiscriminators().size())
        throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND, "Parameter out of bound in ColumnVariant::insertRangeFrom method. "
                                                            "[start({}) + length({}) > local_discriminators.size({})]", start, length, src.getLocalDiscriminators().size());

    /// If src column contains only NULLs, just insert NULLs.
    if (src.hasOnlyNulls())
    {
        insertManyDefaults(length);
        return;
    }

    /// Optimization for case when there is only 1 non-empty variant and no NULLs in src column.
    /// In this case we can simply call insertRangeFrom on this single variant.
    if (auto non_empty_src_local_discr = src.getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls())
    {
        Discriminator src_global_discr = src.globalDiscriminatorByLocal(*non_empty_src_local_discr);
        Discriminator global_discr = src_global_discr;
        if (global_discriminators_mapping && src_global_discr != NULL_DISCRIMINATOR)
            global_discr = (*global_discriminators_mapping)[src_global_discr];

        Discriminator local_discr = localDiscriminatorByGlobal(global_discr);
        size_t offset = variants[local_discr]->size();
        variants[local_discr]->insertRangeFrom(*src.variants[*non_empty_src_local_discr], start, length);
        getLocalDiscriminators().resize_fill(local_discriminators->size() + length, local_discr);
        auto & offsets_data = getOffsets();
        offsets_data.reserve(offsets_data.size() + length);
        for (size_t i = 0; i != length; ++i)
            offsets_data.push_back(offset++);
        return;
    }

    /// Iterate through src local_discriminators in range [start, start + length],
    /// collect ranges we need to insert for all variants and update offsets.
    /// nested_ranges[i].first - offset in src.variants[i]
    /// nested_ranges[i].second - length in src.variants[i]
    std::vector<std::pair<size_t, size_t>> nested_ranges(src.variants.size(), {0, 0});
    auto & offsets_data = getOffsets();
    offsets_data.reserve(offsets_data.size() + length);
    auto & local_discriminators_data = getLocalDiscriminators();
    local_discriminators_data.reserve(local_discriminators_data.size() + length);
    const auto & src_offsets_data = src.getOffsets();
    const auto & src_local_discriminators_data = src.getLocalDiscriminators();
    for (size_t i = start; i != start + length; ++i)
    {
        /// We insert from src.variants[src_local_discr] to variants[local_discr]
        Discriminator src_local_discr = src_local_discriminators_data[i];
        Discriminator src_global_discr = src.globalDiscriminatorByLocal(src_local_discr);
        Discriminator global_discr = src_global_discr;
        if (global_discriminators_mapping && src_global_discr != NULL_DISCRIMINATOR)
            global_discr = (*global_discriminators_mapping)[src_global_discr];
        Discriminator local_discr = localDiscriminatorByGlobal(global_discr);
        local_discriminators_data.push_back(local_discr);
        if (local_discr == NULL_DISCRIMINATOR)
        {
            offsets_data.emplace_back();
        }
        else
        {
            /// If we see this discriminator for the first time, set its range start.
            if (!nested_ranges[src_local_discr].second)
                nested_ranges[src_local_discr].first = src_offsets_data[i];
            /// Update offsets column with correct offset.
            offsets_data.push_back(variants[local_discr]->size() + nested_ranges[src_local_discr].second);
            ++nested_ranges[src_local_discr].second;
        }
    }

    for (size_t src_local_discr = 0; src_local_discr != nested_ranges.size(); ++src_local_discr)
    {
        auto [nested_start, nested_length] = nested_ranges[src_local_discr];
        Discriminator src_global_discr = src.globalDiscriminatorByLocal(src_local_discr);
        Discriminator global_discr = src_global_discr;
        if (global_discriminators_mapping && src_global_discr != NULL_DISCRIMINATOR)
            global_discr = (*global_discriminators_mapping)[src_global_discr];
        if (!skip_discriminator || global_discr != *skip_discriminator)
        {
            Discriminator local_discr = localDiscriminatorByGlobal(global_discr);
            if (nested_length)
                variants[local_discr]->insertRangeFrom(*src.variants[src_local_discr], nested_start, nested_length);
        }
    }
}

void ColumnVariant::insertManyFromImpl(const DB::IColumn & src_, size_t position, size_t length, const std::vector<ColumnVariant::Discriminator> * global_discriminators_mapping)
{
    const size_t num_variants = variants.size();
    const auto & src = assert_cast<const ColumnVariant &>(src_);
    if (!global_discriminators_mapping && src.variants.size() != num_variants)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot insert value of Variant type with different number of types");

    Discriminator src_local_discr = src.localDiscriminatorAt(position);
    Discriminator src_global_discr = src.globalDiscriminatorByLocal(src_local_discr);
    Discriminator global_discr = src_global_discr;
    if (global_discriminators_mapping && src_global_discr != NULL_DISCRIMINATOR)
        global_discr = (*global_discriminators_mapping)[src_global_discr];
    Discriminator local_discr = localDiscriminatorByGlobal(global_discr);
    auto & local_discriminators_data = getLocalDiscriminators();
    local_discriminators_data.resize_fill(local_discriminators_data.size() + length, local_discr);

    auto & offsets_data = getOffsets();
    if (local_discr == NULL_DISCRIMINATOR)
    {
        offsets_data.resize_fill(offsets_data.size() + length);
    }
    else
    {
        size_t prev_offset = variants[local_discr]->size();
        offsets_data.reserve(offsets_data.size() + length);
        for (size_t i = 0; i != length; ++i)
            offsets_data.push_back(prev_offset + i);

        variants[local_discr]->insertManyFrom(*src.variants[src_local_discr], src.offsetAt(position), length);
    }
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnVariant::insertFrom(const IColumn & src_, size_t n)
#else
void ColumnVariant::doInsertFrom(const IColumn & src_, size_t n)
#endif
{
    insertFromImpl(src_, n, nullptr);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnVariant::insertRangeFrom(const IColumn & src_, size_t start, size_t length)
#else
void ColumnVariant::doInsertRangeFrom(const IColumn & src_, size_t start, size_t length)
#endif
{
    insertRangeFromImpl(src_, start, length, nullptr, nullptr);
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnVariant::insertManyFrom(const DB::IColumn & src_, size_t position, size_t length)
#else
void ColumnVariant::doInsertManyFrom(const DB::IColumn & src_, size_t position, size_t length)
#endif
{
    insertManyFromImpl(src_, position, length, nullptr);
}

void ColumnVariant::insertFrom(const DB::IColumn & src_, size_t n, const std::vector<ColumnVariant::Discriminator> & global_discriminators_mapping)
{
    insertFromImpl(src_, n, &global_discriminators_mapping);
}

void ColumnVariant::insertRangeFrom(const IColumn & src_, size_t start, size_t length, const std::vector<ColumnVariant::Discriminator> & global_discriminators_mapping, Discriminator skip_discriminator)
{
    insertRangeFromImpl(src_, start, length, &global_discriminators_mapping, &skip_discriminator);
}

void ColumnVariant::insertManyFrom(const DB::IColumn & src_, size_t position, size_t length, const std::vector<ColumnVariant::Discriminator> & global_discriminators_mapping)
{
    insertManyFromImpl(src_, position, length, &global_discriminators_mapping);
}

void ColumnVariant::insertIntoVariantFrom(DB::ColumnVariant::Discriminator global_discr, const DB::IColumn & src_, size_t n)
{
    Discriminator local_discr = localDiscriminatorByGlobal(global_discr);
    getLocalDiscriminators().push_back(local_discr);
    getOffsets().push_back(variants[local_discr]->size());
    variants[local_discr]->insertFrom(src_, n);
}

void ColumnVariant::insertRangeIntoVariantFrom(DB::ColumnVariant::Discriminator global_discr, const DB::IColumn & src_, size_t start, size_t length)
{
    Discriminator local_discr = localDiscriminatorByGlobal(global_discr);
    auto & local_discriminators_data = getLocalDiscriminators();
    local_discriminators_data.resize_fill(local_discriminators_data.size() + length, local_discr);
    auto & offsets_data = getOffsets();
    size_t offset = variants[local_discr]->size();
    offsets_data.reserve(offsets_data.size() + length);
    for (size_t i = 0; i != length; ++i)
        offsets_data.push_back(offset + i);

    variants[local_discr]->insertRangeFrom(src_, start, length);
}

void ColumnVariant::insertManyIntoVariantFrom(DB::ColumnVariant::Discriminator global_discr, const DB::IColumn & src_, size_t position, size_t length)
{
    Discriminator local_discr = localDiscriminatorByGlobal(global_discr);
    auto & local_discriminators_data = getLocalDiscriminators();
    local_discriminators_data.resize_fill(local_discriminators_data.size() + length, local_discr);
    auto & offsets_data = getOffsets();
    size_t offset = variants[local_discr]->size();
    offsets_data.reserve(offsets_data.size() + length);
    for (size_t i = 0; i != length; ++i)
        offsets_data.push_back(offset + i);

    variants[local_discr]->insertManyFrom(src_, position, length);
}

void ColumnVariant::deserializeBinaryIntoVariant(ColumnVariant::Discriminator global_discr, const SerializationPtr & serialization, ReadBuffer & buf, const FormatSettings & format_settings)
{
    auto local_discr = localDiscriminatorByGlobal(global_discr);
    serialization->deserializeBinary(*variants[local_discr], buf, format_settings);
    getLocalDiscriminators().push_back(local_discr);
    getOffsets().push_back(variants[local_discr]->size() - 1);
}

void ColumnVariant::insertDefault()
{
    getLocalDiscriminators().push_back(NULL_DISCRIMINATOR);
    getOffsets().emplace_back();
}

void ColumnVariant::insertManyDefaults(size_t length)
{
    size_t size = local_discriminators->size();
    getLocalDiscriminators().resize_fill(size + length, NULL_DISCRIMINATOR);
    getOffsets().resize_fill(size + length);
}

void ColumnVariant::popBack(size_t n)
{
    /// If we have only NULLs, just pop back from local_discriminators and offsets.
    if (hasOnlyNulls())
    {
        local_discriminators->popBack(n);
        offsets->popBack(n);
        return;
    }

    /// Optimization for case when there is only 1 non-empty variant and no NULLs.
    /// In this case we can just popBack n elements from this variant.
    if (auto non_empty_local_discr = getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls())
    {
        variants[*non_empty_local_discr]->popBack(n);
        local_discriminators->popBack(n);
        offsets->popBack(n);
        return;
    }

    /// Calculate how many rows we need to pop from each variant
    auto & local_discriminators_data = getLocalDiscriminators();
    size_t size = local_discriminators_data.size();
    const size_t num_variants = variants.size();
    std::vector<size_t> nested_n(num_variants, 0);
    for (size_t i = size - n; i < size; ++i)
    {
        Discriminator discr = local_discriminators_data[i];
        if (discr != NULL_DISCRIMINATOR)
            ++nested_n[discr];
    }

    for (size_t i = 0; i != num_variants; ++i)
    {
        if (nested_n[i])
            variants[i]->popBack(nested_n[i]);
    }

    local_discriminators->popBack(n);
    offsets->popBack(n);
}

StringRef ColumnVariant::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    /// During any serialization/deserialization we should always use global discriminators.
    Discriminator global_discr = globalDiscriminatorAt(n);
    char * pos = arena.allocContinue(sizeof(global_discr), begin);
    memcpy(pos, &global_discr, sizeof(global_discr));
    StringRef res(pos, sizeof(global_discr));

    if (global_discr == NULL_DISCRIMINATOR)
        return res;

    auto value_ref = variants[localDiscriminatorByGlobal(global_discr)]->serializeValueIntoArena(offsetAt(n), arena, begin);
    res.data = value_ref.data - res.size;
    res.size += value_ref.size;

    return res;
}

const char * ColumnVariant::deserializeAndInsertFromArena(const char * pos)
{
    /// During any serialization/deserialization we should always use global discriminators.
    Discriminator global_discr = unalignedLoad<Discriminator>(pos);
    pos += sizeof(global_discr);
    Discriminator local_discr = localDiscriminatorByGlobal(global_discr);
    getLocalDiscriminators().push_back(local_discr);
    if (local_discr == NULL_DISCRIMINATOR)
    {
        getOffsets().emplace_back();
        return pos;
    }

    getOffsets().push_back(variants[local_discr]->size());
    return variants[local_discr]->deserializeAndInsertFromArena(pos);
}

const char * ColumnVariant::deserializeVariantAndInsertFromArena(DB::ColumnVariant::Discriminator global_discr, const char * pos)
{
    Discriminator local_discr = localDiscriminatorByGlobal(global_discr);
    getLocalDiscriminators().push_back(local_discr);
    getOffsets().push_back(variants[local_discr]->size());
    return variants[local_discr]->deserializeAndInsertFromArena(pos);
}

const char * ColumnVariant::skipSerializedInArena(const char * pos) const
{
    Discriminator global_discr = unalignedLoad<Discriminator>(pos);
    pos += sizeof(global_discr);
    if (global_discr == NULL_DISCRIMINATOR)
        return pos;

    return variants[localDiscriminatorByGlobal(global_discr)]->skipSerializedInArena(pos);
}

void ColumnVariant::updateHashWithValue(size_t n, SipHash & hash) const
{
    Discriminator global_discr = globalDiscriminatorAt(n);
    hash.update(global_discr);
    if (global_discr != NULL_DISCRIMINATOR)
        variants[localDiscriminatorByGlobal(global_discr)]->updateHashWithValue(offsetAt(n), hash);
}

WeakHash32 ColumnVariant::getWeakHash32() const
{
    auto s = size();

    /// If we have only NULLs, keep hash unchanged.
    if (hasOnlyNulls())
        return WeakHash32(s);

    /// Optimization for case when there is only 1 non-empty variant and no NULLs.
    /// In this case we can just calculate weak hash for this variant.
    if (auto non_empty_local_discr = getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls())
        return variants[*non_empty_local_discr]->getWeakHash32();

    /// Calculate weak hash for all variants.
    std::vector<WeakHash32> nested_hashes;
    for (const auto & variant : variants)
        nested_hashes.emplace_back(variant->getWeakHash32());

    /// For each row hash is a hash of corresponding row from corresponding variant.
    WeakHash32 hash(s);
    auto & hash_data = hash.getData();
    const auto & local_discriminators_data = getLocalDiscriminators();
    const auto & offsets_data = getOffsets();
    for (size_t i = 0; i != local_discriminators_data.size(); ++i)
    {
        Discriminator discr = local_discriminators_data[i];
        /// Update hash only for non-NULL values
        if (discr != NULL_DISCRIMINATOR)
            hash_data[i] = nested_hashes[discr].getData()[offsets_data[i]];
    }

    return hash;
}

void ColumnVariant::updateHashFast(SipHash & hash) const
{
    local_discriminators->updateHashFast(hash);
    for (const auto & variant : variants)
        variant->updateHashFast(hash);
}

ColumnPtr ColumnVariant::filter(const Filter & filt, ssize_t result_size_hint) const
{
    if (size() != filt.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of filter ({}) doesn't match size of column ({})", filt.size(), size());

    /// If we have only NULLs, just filter local_discriminators column.
    if (hasOnlyNulls())
    {
        Columns new_variants(variants.begin(), variants.end());
        auto new_discriminators = local_discriminators->filter(filt, result_size_hint);
        /// In case of all NULL values offsets doesn't contain any useful values, just resize it.
        ColumnPtr new_offsets = offsets->cloneResized(new_discriminators->size());
        return ColumnVariant::create(new_discriminators, new_offsets, new_variants, local_to_global_discriminators);
    }

    /// Optimization for case when there is only 1 non-empty variant and no NULLs.
    /// In this case we can just filter this variant and resize discriminators/offsets.
    if (auto non_empty_discr = getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls())
    {
        Columns new_variants(variants.begin(), variants.end());
        new_variants[*non_empty_discr] = variants[*non_empty_discr]->filter(filt, result_size_hint);
        size_t new_size = new_variants[*non_empty_discr]->size();
        ColumnPtr new_discriminators = local_discriminators->cloneResized(new_size);
        ColumnPtr new_offsets = offsets->cloneResized(new_size);
        return ColumnVariant::create(new_discriminators, new_offsets, new_variants, local_to_global_discriminators);
    }

    /// We should create filter for each variant
    /// according to local_discriminators and given filter.
    const size_t num_variants = variants.size();
    std::vector<Filter> nested_filters(num_variants);
    for (size_t i = 0; i != num_variants; ++i)
        nested_filters[i].reserve_exact(variants[i]->size());

    /// As we will iterate through local_discriminators anyway, we can count
    /// result size for each variant.
    std::vector<ssize_t> variant_result_size_hints(num_variants);

    const auto & local_discriminators_data = getLocalDiscriminators();
    for (size_t i = 0; i != local_discriminators_data.size(); ++i)
    {
        Discriminator discr = local_discriminators_data[i];
        if (discr != NULL_DISCRIMINATOR)
        {
            nested_filters[local_discriminators_data[i]].push_back(filt[i]);
            variant_result_size_hints[local_discriminators_data[i]] += !!(filt[i]);
        }
    }

    Columns new_variants;
    new_variants.reserve(num_variants);
    for (size_t i = 0; i != num_variants; ++i)
    {
        /// It make sense to call filter() on variant only if the result size is not 0.
        if (variant_result_size_hints[i])
            new_variants.emplace_back(variants[i]->filter(nested_filters[i], variant_result_size_hints[i]));
        else
            new_variants.emplace_back(variants[i]->cloneEmpty());
    }

    /// We cannot use filtered offsets column, as it will be incorrect.
    /// It will be reconstructed on ColumnVariant creation according to new local_discriminators.
    return ColumnVariant::create(local_discriminators->filter(filt, result_size_hint), new_variants, local_to_global_discriminators);
}

void ColumnVariant::expand(const Filter & mask, bool inverted)
{
    /// Expand local_discriminators using NULL_DISCRIMINATOR for 0-rows.
    expandDataByMask(getLocalDiscriminators(), mask, inverted, NULL_DISCRIMINATOR);
    expandDataByMask(getOffsets(), mask, inverted);
}

ColumnPtr ColumnVariant::permute(const Permutation & perm, size_t limit) const
{
    /// If we have only NULLs, permutation will take no effect, just return resized column.
    if (hasOnlyNulls())
    {
        if (limit)
            return cloneResized(limit);

        /// If no limit, we can just return current immutable column.
        return this->getPtr();
    }

    /// Optimization when we have only one non empty variant and no NULLs.
    /// In this case local_discriminators column is filled with identical values and offsets column
    /// filled with sequential numbers. In this case we can just apply permutation to this
    /// single non-empty variant and cut local_discriminators and offsets columns to the result size.
    if (auto non_empty_local_discr = getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls())
    {
        MutableColumns new_variants;
        const size_t num_variants = variants.size();
        new_variants.reserve(num_variants);
        for (size_t i = 0; i != num_variants; ++i)
        {
            if (i == *non_empty_local_discr)
                new_variants.emplace_back(variants[*non_empty_local_discr]->permute(perm, limit)->assumeMutable());
            else
                new_variants.emplace_back(variants[i]->assumeMutable());
        }

        size_t new_size = new_variants[*non_empty_local_discr]->size();
        return ColumnVariant::create(local_discriminators->cloneResized(new_size), offsets->cloneResized(new_size), std::move(new_variants), local_to_global_discriminators);
    }

    return permuteImpl(*this, perm, limit);
}

ColumnPtr ColumnVariant::index(const IColumn & indexes, size_t limit) const
{
    /// If we have only NULLs, index will take no effect, just return resized column.
    if (hasOnlyNulls())
        return cloneResized(limit == 0 ? indexes.size(): limit);

    /// Optimization when we have only one non empty variant and no NULLs.
    /// In this case local_discriminators column is filled with identical values and offsets column
    /// filled with sequential numbers. So we can just apply indexes to this
    /// single non-empty variant and cut local_discriminators and offsets columns to the result size.
    if (auto non_empty_local_discr = getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls())
    {
        MutableColumns new_variants;
        const size_t num_variants = variants.size();
        new_variants.reserve(num_variants);
        for (size_t i = 0; i != num_variants; ++i)
        {
            if (i == *non_empty_local_discr)
                new_variants.emplace_back(variants[*non_empty_local_discr]->index(indexes, limit)->assumeMutable());
            else
                new_variants.emplace_back(variants[i]->assumeMutable());
        }

        size_t new_size = new_variants[*non_empty_local_discr]->size();
        return ColumnVariant::create(local_discriminators->cloneResized(new_size), offsets->cloneResized(new_size), std::move(new_variants), local_to_global_discriminators);
    }

    return selectIndexImpl(*this, indexes, limit);
}

template <typename Type>
ColumnPtr ColumnVariant::indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const
{
    /// First, apply indexes for local_discriminators and offsets.
    ColumnPtr new_local_discriminators = assert_cast<const ColumnDiscriminators &>(*local_discriminators).indexImpl(indexes, limit);
    ColumnPtr new_offsets = assert_cast<const ColumnOffsets &>(*offsets).indexImpl(indexes, limit);
    const auto & new_local_discriminators_data =  assert_cast<const ColumnDiscriminators &>(*new_local_discriminators).getData();
    const auto & new_offsets_data = assert_cast<const ColumnOffsets &>(*new_offsets).getData();
    /// Then, create permutation for each variant.
    const size_t num_variants = variants.size();
    std::vector<Permutation> nested_perms(num_variants);
    /// If there is no limit, we know the size of each permutation
    /// in advance and can use reserve.
    if (limit == 0)
    {
        for (size_t i = 0; i != num_variants; ++i)
            nested_perms[i].reserve_exact(variants[i]->size());
    }

    for (size_t i = 0; i != new_local_discriminators_data.size(); ++i)
    {
        Discriminator discr = new_local_discriminators_data[i];
        if (discr != NULL_DISCRIMINATOR)
            nested_perms[discr].push_back(new_offsets_data[i]);
    }

    Columns new_variants;
    new_variants.reserve(num_variants);
    for (size_t i = 0; i != num_variants; ++i)
    {
        /// Check if no values from this variant were selected.
        if (nested_perms[i].empty())
        {
            new_variants.emplace_back(variants[i]->cloneEmpty());
        }
        else
        {
            size_t nested_limit = nested_perms[i].size() == variants[i]->size() ? 0 : nested_perms[i].size();
            new_variants.emplace_back(variants[i]->permute(nested_perms[i], nested_limit));
        }
    }

    /// We cannot use new_offsets column as an offset column, because it became invalid after variants permutation.
    /// New offsets column will be created in constructor.
    return ColumnVariant::create(new_local_discriminators, new_variants, local_to_global_discriminators);
}

ColumnPtr ColumnVariant::replicate(const Offsets & replicate_offsets) const
{
    if (size() != replicate_offsets.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of offsets {} doesn't match size of column {}", replicate_offsets.size(), size());

    if (empty())
        return cloneEmpty();

    /// If we have only NULLs, just resize column to the new size.
    if (hasOnlyNulls())
        return cloneResized(replicate_offsets.back());

    const size_t num_variants = variants.size();

    /// Optimization when we have only one non empty variant and no NULLs.
    /// In this case local_discriminators column is filled with identical values and offsets column
    /// filled with sequential numbers. So we can just replicate this one non empty variant,
    /// then resize local_discriminators to the result size and fill offsets column.
    if (auto non_empty_local_discr = getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls())
    {
        MutableColumns new_variants;
        new_variants.reserve(num_variants);
        for (size_t i = 0; i != num_variants; ++i)
        {
            if (i == *non_empty_local_discr)
                new_variants.emplace_back(variants[*non_empty_local_discr]->replicate(replicate_offsets)->assumeMutable());
            else
                new_variants.emplace_back(variants[i]->cloneEmpty());
        }

        size_t new_size = new_variants[*non_empty_local_discr]->size();
        /// Create and fill new local_discriminators column with non_empty_index discriminator.
        auto new_local_discriminators = IColumn::mutate(local_discriminators);
        assert_cast<ColumnDiscriminators &>(*new_local_discriminators).getData().resize_fill(new_size, *non_empty_local_discr);
        /// Create and fill new offsets column with sequential indexes.
        auto new_offsets = IColumn::mutate(offsets);
        auto & new_offsets_data = assert_cast<ColumnOffsets &>(*new_offsets).getData();
        size_t old_size = offsets->size();
        if (new_size > old_size)
        {
            new_offsets_data.reserve_exact(new_size);
            for (size_t i = old_size; i < new_size; ++i)
                new_offsets_data.push_back(i);
        }
        else
        {
            new_offsets_data.resize(new_size);
        }

        return ColumnVariant::create(std::move(new_local_discriminators), std::move(new_offsets), std::move(new_variants), local_to_global_discriminators);
    }

    /// Create replicate offsets for each variant according to
    /// local_discriminators column.
    std::vector<Offsets> nested_replicated_offsets(num_variants);
    for (size_t i = 0; i != num_variants; ++i)
        nested_replicated_offsets[i].reserve_exact(variants[i]->size());

    const auto & local_discriminators_data = getLocalDiscriminators();
    for (size_t i = 0; i != local_discriminators_data.size(); ++i)
    {
        Discriminator discr = local_discriminators_data[i];
        if (discr != NULL_DISCRIMINATOR)
        {
            size_t repeat_count = replicate_offsets[i] - replicate_offsets[i - 1];
            nested_replicated_offsets[discr].push_back(nested_replicated_offsets[discr].back() + repeat_count);
        }
    }

    auto new_local_discriminators = local_discriminators->replicate(replicate_offsets);
    Columns new_variants;
    new_variants.reserve(num_variants);
    for (size_t i = 0; i != num_variants; ++i)
        new_variants.emplace_back(variants[i]->replicate(nested_replicated_offsets[i]));

    /// New offsets column will be created in constructor.
    return ColumnVariant::create(new_local_discriminators, new_variants, local_to_global_discriminators);
}

MutableColumns ColumnVariant::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    const size_t num_variants = variants.size();

    /// If we have only NULLs, we need to scatter only local_discriminators.
    if (hasOnlyNulls())
    {
        auto scattered_local_discriminators = local_discriminators->scatter(num_columns, selector);
        MutableColumns result;
        result.reserve(num_columns);
        for (size_t i = 0; i != num_columns; ++i)
        {
            MutableColumns new_variants;
            new_variants.reserve(num_variants);
            for (const auto & variant : variants)
                new_variants.emplace_back(IColumn::mutate(variant));

            result.emplace_back(ColumnVariant::create(std::move(scattered_local_discriminators[i]), std::move(new_variants), local_to_global_discriminators));
        }

        return result;
    }

    /// Optimization when we have only one non empty variant and no NULLs.
    /// In this case we can just scatter local_discriminators and this non empty variant.
    if (auto non_empty_local_discr = getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls())
    {
        auto scattered_local_discriminators = local_discriminators->scatter(num_columns, selector);
        auto scattered_non_empty_variant = variants[*non_empty_local_discr]->scatter(num_columns, selector);
        MutableColumns result;
        result.reserve(num_columns);
        for (size_t i = 0; i != num_columns; ++i)
        {
            MutableColumns scattered_nested_variants(num_variants);
            for (size_t j = 0; j != num_variants; ++j)
            {
                if (j == *non_empty_local_discr)
                    scattered_nested_variants[j] = std::move(scattered_non_empty_variant[i]);
                else
                    scattered_nested_variants[j] = IColumn::mutate(variants[j]);
            }

            result.emplace_back(ColumnVariant::create(std::move(scattered_local_discriminators[i]), std::move(scattered_nested_variants), local_to_global_discriminators));
        }

        return result;
    }

    /// Create selector for each variant according to local_discriminators.
    std::vector<Selector> nested_selectors(num_variants);
    for (size_t i = 0; i != num_variants; ++i)
        nested_selectors[i].reserve_exact(variants[i]->size());

    const auto & local_discriminators_data = getLocalDiscriminators();
    for (size_t i = 0; i != local_discriminators_data.size(); ++i)
    {
        Discriminator discr = local_discriminators_data[i];
        if (discr != NULL_DISCRIMINATOR)
            nested_selectors[discr].push_back(selector[i]);
    }

    auto scattered_local_discriminators = local_discriminators->scatter(num_columns, selector);
    std::vector<MutableColumns> nested_scattered_variants;
    nested_scattered_variants.reserve(num_variants);
    for (size_t i = 0; i != num_variants; ++i)
        nested_scattered_variants.emplace_back(variants[i]->scatter(num_columns, nested_selectors[i]));

    MutableColumns result;
    result.reserve(num_columns);
    for (size_t i = 0; i != num_columns; ++i)
    {
        MutableColumns new_variants;
        new_variants.reserve(num_variants);
        for (size_t j = 0; j != num_variants; ++j)
            new_variants.emplace_back(std::move(nested_scattered_variants[j][i]));
        result.emplace_back(ColumnVariant::create(std::move(scattered_local_discriminators[i]), std::move(new_variants), local_to_global_discriminators));
    }

    return result;
}

bool ColumnVariant::hasEqualValues() const
{
    if (local_discriminators->empty() || hasOnlyNulls())
        return true;

    return local_discriminators->hasEqualValues() && variants[localDiscriminatorAt(0)]->hasEqualValues();
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
int ColumnVariant::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#else
int ColumnVariant::doCompareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
#endif
{
    const auto & rhs_variant = assert_cast<const ColumnVariant &>(rhs);
    Discriminator left_discr = globalDiscriminatorAt(n);
    Discriminator right_discr = rhs_variant.globalDiscriminatorAt(m);

    /// Check if we have NULLs and return result based on nan_direction_hint.
    if (left_discr == NULL_DISCRIMINATOR && right_discr == NULL_DISCRIMINATOR)
        return 0;
    if (left_discr == NULL_DISCRIMINATOR)
        return nan_direction_hint;
    if (right_discr == NULL_DISCRIMINATOR)
        return -nan_direction_hint;

    /// If rows have different discriminators, row with least discriminator is considered the least.
    if (left_discr != right_discr)
        return left_discr < right_discr ? -1 : 1;

    /// If rows have the same discriminators, compare actual values from corresponding variants.
    return getVariantByGlobalDiscriminator(left_discr).compareAt(offsetAt(n), rhs_variant.offsetAt(m), rhs_variant.getVariantByGlobalDiscriminator(right_discr), nan_direction_hint);
}

struct ColumnVariant::ComparatorBase
{
    const ColumnVariant & parent;
    int nan_direction_hint;

    ComparatorBase(const ColumnVariant & parent_, int nan_direction_hint_)
        : parent(parent_), nan_direction_hint(nan_direction_hint_)
    {
    }

    ALWAYS_INLINE int compare(size_t lhs, size_t rhs) const
    {
        return parent.compareAt(lhs, rhs, parent, nan_direction_hint);
    }
};

void ColumnVariant::getPermutation(PermutationSortDirection direction, PermutationSortStability stability, size_t limit, int nan_direction_hint, Permutation & res) const
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

void ColumnVariant::updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability, size_t limit, int nan_direction_hint, IColumn::Permutation & res, DB::EqualRanges & equal_ranges) const
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

void ColumnVariant::reserve(size_t n)
{
    getLocalDiscriminators().reserve_exact(n);
    getOffsets().reserve_exact(n);
}

void ColumnVariant::prepareForSquashing(const Columns & source_columns)
{
    size_t new_size = size();
    for (const auto & source_column : source_columns)
        new_size += source_column->size();
    reserve(new_size);

    for (size_t i = 0; i != variants.size(); ++i)
    {
        Columns source_variant_columns;
        source_variant_columns.reserve(source_columns.size());
        for (const auto & source_column : source_columns)
            source_variant_columns.push_back(assert_cast<const ColumnVariant &>(*source_column).getVariantPtrByGlobalDiscriminator(i));
        getVariantByGlobalDiscriminator(i).prepareForSquashing(source_variant_columns);
    }
}

size_t ColumnVariant::capacity() const
{
    return local_discriminators->capacity();
}

void ColumnVariant::ensureOwnership()
{
    const size_t num_variants = variants.size();
    for (size_t i = 0; i < num_variants; ++i)
        getVariantByLocalDiscriminator(i).ensureOwnership();
}

size_t ColumnVariant::byteSize() const
{
    size_t res = local_discriminators->byteSize() + offsets->byteSize();
    for (const auto & variant : variants)
        res += variant->byteSize();
    return res;
}

size_t ColumnVariant::byteSizeAt(size_t n) const
{
    size_t res = sizeof(Offset) + sizeof(Discriminator);
    Discriminator discr = localDiscriminatorAt(n);
    if (discr == NULL_DISCRIMINATOR)
        return res;

    return res + variants[discr]->byteSizeAt(offsetAt(n));
}

size_t ColumnVariant::allocatedBytes() const
{
    size_t res = local_discriminators->allocatedBytes() + offsets->allocatedBytes();
    for (const auto & variant : variants)
        res += variant->allocatedBytes();
    return res;
}

void ColumnVariant::protect()
{
    local_discriminators->protect();
    offsets->protect();
    for (auto & variant : variants)
        variant->protect();
}

void ColumnVariant::getExtremes(Field & min, Field & max) const
{
    min = Null();
    max = Null();
}

void ColumnVariant::forEachSubcolumn(MutableColumnCallback callback)
{
    callback(local_discriminators);
    callback(offsets);
    for (auto & variant : variants)
        callback(variant);
}

void ColumnVariant::forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback)
{
    callback(*local_discriminators);
    local_discriminators->forEachSubcolumnRecursively(callback);
    callback(*offsets);
    offsets->forEachSubcolumnRecursively(callback);

    for (auto & variant : variants)
    {
        callback(*variant);
        variant->forEachSubcolumnRecursively(callback);
    }
}

bool ColumnVariant::structureEquals(const IColumn & rhs) const
{
    const auto * rhs_variant = typeid_cast<const ColumnVariant *>(&rhs);
    if (!rhs_variant)
        return false;

    const size_t num_variants = variants.size();
    if (num_variants != rhs_variant->variants.size())
        return false;

    for (size_t i = 0; i < num_variants; ++i)
        if (!variants[i]->structureEquals(rhs_variant->getVariantByGlobalDiscriminator(globalDiscriminatorByLocal(i))))
            return false;

    return true;
}

ColumnPtr ColumnVariant::compress() const
{
    ColumnPtr local_discriminators_compressed = local_discriminators->compress();
    ColumnPtr offsets_compressed = offsets->compress();
    size_t byte_size = local_discriminators_compressed->byteSize() + offsets_compressed->byteSize();
    Columns compressed;
    compressed.reserve(variants.size());
    for (const auto & variant : variants)
    {
        auto compressed_variant = variant->compress();
        byte_size += compressed_variant->byteSize();
        compressed.emplace_back(std::move(compressed_variant));
    }

    return ColumnCompressed::create(size(), byte_size,
        [my_local_discriminators_compressed = std::move(local_discriminators_compressed), my_offsets_compressed = std::move(offsets_compressed), my_compressed = std::move(compressed), my_local_to_global_discriminators = this->local_to_global_discriminators]() mutable
        {
            Columns decompressed;
            decompressed.reserve(my_compressed.size());
            for (const auto & variant : my_compressed)
                decompressed.push_back(variant->decompress());
            return ColumnVariant::create(my_local_discriminators_compressed->decompress(), my_offsets_compressed->decompress(), decompressed, my_local_to_global_discriminators);
        });
}

double ColumnVariant::getRatioOfDefaultRows(double) const
{
    UInt64 num_defaults = getNumberOfDefaultRows();
    return static_cast<double>(num_defaults) / local_discriminators->size();
}

UInt64 ColumnVariant::getNumberOfDefaultRows() const
{
    size_t total_variant_sizes = 0;
    for (const auto & variant : variants)
        total_variant_sizes += variant->size();
    return local_discriminators->size() - total_variant_sizes;
}

void ColumnVariant::getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const
{
    size_t to = limit && from + limit < size() ? from + limit : size();
    indices.reserve(indices.size() + to - from);

    for (size_t i = from; i < to; ++i)
    {
        if (!isDefaultAt(i))
            indices.push_back(i);
    }
}

void ColumnVariant::finalize()
{
    for (auto & variant : variants)
        variant->finalize();
}

bool ColumnVariant::isFinalized() const
{
    return std::all_of(variants.begin(), variants.end(), [](const auto & variant) { return variant->isFinalized(); });
}

std::optional<ColumnVariant::Discriminator> ColumnVariant::getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls() const
{
    for (size_t i = 0; i != variants.size(); ++i)
    {
        if (variants[i]->size() == local_discriminators->size())
            return i;
        if (!variants[i]->empty())
            return std::nullopt;
    }

    return std::nullopt;
}

std::optional<ColumnVariant::Discriminator> ColumnVariant::getGlobalDiscriminatorOfOneNoneEmptyVariantNoNulls() const
{
    if (auto local_discr = getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls())
        return globalDiscriminatorByLocal(*local_discr);

    return std::nullopt;
}

std::optional<ColumnVariant::Discriminator> ColumnVariant::getGlobalDiscriminatorOfOneNoneEmptyVariant() const
{
    std::optional<ColumnVariant::Discriminator> discr;
    for (size_t i = 0; i != variants.size(); ++i)
    {
        if (!variants[i]->empty())
        {
            /// Check if we already had non-empty variant.
            if (discr)
                return std::nullopt;
            discr = globalDiscriminatorByLocal(i);
        }
    }

    return discr;
}

void ColumnVariant::applyNullMap(const ColumnVector<UInt8>::Container & null_map)
{
    applyNullMapImpl<false>(null_map);
}

void ColumnVariant::applyNegatedNullMap(const ColumnVector<UInt8>::Container & null_map)
{
    applyNullMapImpl<true>(null_map);
}

template <bool inverted>
void ColumnVariant::applyNullMapImpl(const ColumnVector<UInt8>::Container & null_map)
{
    if (null_map.size() != local_discriminators->size())
        throw Exception(ErrorCodes::SIZES_OF_NESTED_COLUMNS_ARE_INCONSISTENT,
                        "Logical error: Sizes of discriminators column and null map data are not equal");

    /// If we have only NULLs, nothing to do.
    if (hasOnlyNulls())
    {
        return;
    }

    /// If we have only 1 non empty column and no NULLs, we can just filter that
    /// variant according to the null_map.
    if (auto non_empty_local_discr = getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls())
    {
        auto & local_discriminators_data = getLocalDiscriminators();
        auto & offsets_data = getOffsets();
        size_t size_hint = 0;

        if constexpr (inverted)
        {
            for (size_t i = 0; i != local_discriminators_data.size(); ++i)
            {
                if (null_map[i])
                    offsets_data[i] = size_hint++;
                else
                    local_discriminators_data[i] = NULL_DISCRIMINATOR;
            }
            variants[*non_empty_local_discr] = variants[*non_empty_local_discr]->filter(null_map, size_hint);
        }
        else
        {
            ColumnVector<UInt8>::Container filter;
            filter.reserve_exact(null_map.size());
            for (size_t i = 0; i != local_discriminators_data.size(); ++i)
            {
               if (null_map[i])
               {
                    filter.push_back(0);
                    local_discriminators_data[i] = NULL_DISCRIMINATOR;
               }
               else
               {
                   filter.push_back(1);
                   offsets_data[i] = size_hint++;
               }
            }
            variants[*non_empty_local_discr] = variants[*non_empty_local_discr]->filter(filter, size_hint);
        }

        return;
    }

    /// In general case we should iterate through null_map + discriminators,
    /// create filter for each variant and update offsets column.
    std::vector<Filter> variant_filters;
    variant_filters.resize(variants.size());
    std::vector<size_t> variant_new_sizes;
    variant_new_sizes.resize(variants.size(), 0);

    auto & local_discriminators_data = getLocalDiscriminators();
    auto & offsets_data = getOffsets();
    for (size_t i = 0; i != local_discriminators_data.size(); ++i)
    {
        auto & discr = local_discriminators_data[i];
        if (discr != NULL_DISCRIMINATOR)
        {
            if (null_map[i] ^ inverted)
            {
                auto & variant_filter = variant_filters[discr];
                /// We create filters lazily.
                if (variant_filter.empty())
                   variant_filter.resize_fill(variants[discr]->size(), 1);
                variant_filter[offsets_data[i]] = 0;
                discr = NULL_DISCRIMINATOR;
            }
            else
            {
                offsets_data[i] = variant_new_sizes[discr]++;
            }
        }
    }

    for (size_t i = 0; i != variants.size(); ++i)
    {
        if (!variant_filters[i].empty())
            variants[i] = variants[i]->filter(variant_filters[i], variant_new_sizes[i]);
    }
}

void ColumnVariant::extend(const std::vector<Discriminator> & old_to_new_global_discriminators, std::vector<std::pair<MutableColumnPtr, Discriminator>> && new_variants_and_discriminators)
{
    /// Update global discriminators for current variants.
    for (Discriminator & global_discr : local_to_global_discriminators)
        global_discr = old_to_new_global_discriminators[global_discr];

    /// Add new variants.
    variants.reserve(variants.size() + new_variants_and_discriminators.size());
    local_to_global_discriminators.reserve(local_to_global_discriminators.size() + new_variants_and_discriminators.size());
    for (auto & new_variant_and_discriminator : new_variants_and_discriminators)
    {
        variants.emplace_back(std::move(new_variant_and_discriminator.first));
        local_to_global_discriminators.push_back(new_variant_and_discriminator.second);
    }

    /// Update global -> local discriminators matching.
    global_to_local_discriminators.resize(local_to_global_discriminators.size());
    for (Discriminator local_discr = 0; local_discr != local_to_global_discriminators.size(); ++local_discr)
        global_to_local_discriminators[local_to_global_discriminators[local_discr]] = local_discr;
}

bool ColumnVariant::hasDynamicStructure() const
{
    for (const auto & variant : variants)
    {
        if (variant->hasDynamicStructure())
            return true;
    }

    return false;
}

void ColumnVariant::takeDynamicStructureFromSourceColumns(const Columns & source_columns)
{
    std::vector<Columns> variants_source_columns;
    variants_source_columns.resize(variants.size());
    for (size_t i = 0; i != variants.size(); ++i)
        variants_source_columns[i].reserve(source_columns.size());

    for (const auto & source_column : source_columns)
    {
        const auto & source_variants = assert_cast<const ColumnVariant &>(*source_column).variants;
        for (size_t i = 0; i != source_variants.size(); ++i)
            variants_source_columns[i].push_back(source_variants[i]);
    }

    for (size_t i = 0; i != variants.size(); ++i)
        variants[i]->takeDynamicStructureFromSourceColumns(variants_source_columns[i]);
}

}
