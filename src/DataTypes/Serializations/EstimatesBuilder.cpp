#include <DataTypes/Serializations/EstimatesBuilder.h>

#include <Columns/ColumnSparse.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <DataTypes/Serializations/SerializationInfoTuple.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <algorithm>

namespace DB
{

namespace
{

/// Increment the (optional) default count of an estimate, treating an absent count as 0.
void addDefaultCount(Estimate & estimate, UInt64 delta)
{
    estimate.num_defaults = estimate.num_defaults.value_or(0) + delta;
}

/// End of the contiguous key range that holds the estimate of the column with key `key` and the
/// estimates of all its subcolumns: the paths are joined with '\0' (see `subcolumnEstimateKey`),
/// so in an ordered map the range is [key, key + '\x01').
String subtreeEndKey(const String & key)
{
    return key + '\x01';
}

}

EstimatesBuilder::EstimatesBuilder(const NamesAndTypesList & columns, const SerializationInfoSettings & settings_, const Estimates & external_estimates)
    : settings(settings_)
{
    if (settings.isAlwaysDefault())
        return;

    for (const auto & column : columns)
    {
        if (!settings.canUseSparseSerialization(*column.type))
            continue;

        /// A column whose default count is provided by the explicit statistics does not need to be
        /// sampled: store the exact counts and exclude the column from accumulation. Tuples are always
        /// sampled — their elements are counted independently, and only top-level columns have
        /// statistics (`basic` statistics cannot be created for tuples anyway).
        auto it = external_estimates.find(column.name);

        if (it != external_estimates.end() && it->second.num_defaults.has_value() && !typeid_cast<const DataTypeTuple *>(column.type.get()))
        {
            Estimate estimate;
            estimate.rows_count = it->second.rows_count;
            estimate.num_defaults = it->second.num_defaults;
            estimates.emplace(column.name, std::move(estimate));
            columns_with_exact_counts.insert(column.name);
        }
        else
        {
            addKeys(column.name, *column.type);
        }
    }
}

void EstimatesBuilder::addKeys(const String & key, const IDataType & type)
{
    estimates.emplace(key, Estimate{});

    /// Only `Tuple` has per-element serialization infos (mirrors `DataTypeTuple::createSerializationInfo`);
    /// all other types are leaves whose elements (if any) are serialized as a whole. Each element becomes
    /// a separate entry, keyed by its subcolumn path.
    if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(&type))
    {
        const auto & elements = type_tuple->getElements();
        const auto & names = type_tuple->getElementNames();
        for (size_t i = 0; i < elements.size(); ++i)
            addKeys(subcolumnEstimateKey(key, names[i]), *elements[i]);
    }
}

void EstimatesBuilder::sampleColumn(const String & key, const IColumn & column, const IDataType & type)
{
    auto it = estimates.find(key);
    if (it == estimates.end())
        return;

    size_t rows = column.size();
    double ratio = column.getRatioOfDefaultRows(ColumnSparse::DEFAULT_ROWS_SEARCH_SAMPLE_RATIO);
    it->second.rows_count += rows;
    addDefaultCount(it->second, static_cast<UInt64>(ratio * static_cast<double>(rows)));

    /// The tuple structure and the element names are taken from the block's own type, so the element
    /// columns always match them; an element whose key is not tracked is skipped by the lookup above.
    if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(&type))
    {
        const auto & elem_columns = assert_cast<const ColumnTuple &>(column).getColumns();
        const auto & elem_types = type_tuple->getElements();
        const auto & names = type_tuple->getElementNames();

        for (size_t i = 0; i < names.size(); ++i)
            sampleColumn(subcolumnEstimateKey(key, names[i]), *elem_columns[i], *elem_types[i]);
    }
}

void EstimatesBuilder::add(const Block & block)
{
    for (const auto & column : block)
        if (!columns_with_exact_counts.contains(column.name))
            sampleColumn(column.name, *column.column, *column.type);
}

void EstimatesBuilder::addDefaults(const String & name, size_t length)
{
    if (columns_with_exact_counts.contains(name))
        return;

    auto end = estimates.lower_bound(subtreeEndKey(name));
    for (auto it = estimates.lower_bound(name); it != end; ++it)
    {
        it->second.rows_count += length;
        addDefaultCount(it->second, length);
    }
}

void EstimatesBuilder::add(const Estimates & part_estimates)
{
    /// The entries of one column are contiguous, so the loop visits each top-level column and then
    /// consumes its whole key range.
    auto it = estimates.begin();
    while (it != estimates.end())
    {
        const auto & name = it->first;
        auto subtree_end = estimates.lower_bound(subtreeEndKey(name));

        auto part_it = part_estimates.find(name);
        if (part_it == part_estimates.end() || columns_with_exact_counts.contains(name))
        {
            it = subtree_end;
            continue;
        }

        /// Every entry of one column in a part has the part's row count for that column, so a tracked
        /// (sub)column missing from the source part contributes that many all-default rows.
        UInt64 part_rows_count = part_it->second.rows_count;

        for (; it != subtree_end; ++it)
        {
            if (auto entry_it = part_estimates.find(it->first); entry_it != part_estimates.end())
                addCounts(it->second, entry_it->second);
            else
            {
                it->second.rows_count += part_rows_count;
                addDefaultCount(it->second, part_rows_count);
            }
        }
    }
}

void EstimatesBuilder::mergeEstimates(const Estimates & external_estimates)
{
    /// Explicit statistics exist only for top-level columns; override the sampled default count with the
    /// exact one from the statistics where it is available. The sampled row count is kept (it is exact).
    for (const auto & [name, external] : external_estimates)
    {
        if (!external.num_defaults.has_value())
            continue;
        if (auto it = estimates.find(name); it != estimates.end())
            it->second.num_defaults = external.num_defaults;
    }
}

void EstimatesBuilder::mergeEstimates(Estimates & estimates, const Estimates & external_estimates)
{
    for (const auto & [name, external] : external_estimates)
    {
        if (!external.num_defaults.has_value())
            continue;
        if (auto it = estimates.find(name); it != estimates.end())
            it->second.num_defaults = external.num_defaults;
    }
}

Estimates EstimatesBuilder::getEstimates() const
{
    return {estimates.begin(), estimates.end()};
}

ISerialization::KindStack EstimatesBuilder::chooseKindStack(
    const Estimate & estimate, const SerializationInfoSettings & settings)
{
    ISerialization::KindStack kind_stack = {ISerialization::Kind::DEFAULT};
    double ratio = estimate.rows_count
        ? std::min(static_cast<double>(estimate.num_defaults.value_or(0)) / static_cast<double>(estimate.rows_count), 1.0)
        : 0.0;
    if (ratio > settings.ratio_of_defaults_for_sparse)
        kind_stack.push_back(ISerialization::Kind::SPARSE);
    return kind_stack;
}

void EstimatesBuilder::chooseKindsImpl(const String & key, SerializationInfo & info, const Estimates & estimates)
{
    if (info.getSettings().choose_kind)
        if (auto it = estimates.find(key); it != estimates.end())
            info.setKindStack(chooseKindStack(it->second, info.getSettings()));

    /// A `Tuple` chooses the kind of each element independently of its own: recurse even when the
    /// tuple itself had an estimate (on write paths it always does).
    if (auto * info_tuple = typeid_cast<SerializationInfoTuple *>(&info))
    {
        const auto & names = info_tuple->getElementNames();
        for (size_t i = 0; i < names.size(); ++i)
            chooseKindsImpl(subcolumnEstimateKey(key, names[i]), *info_tuple->getElementInfo(i), estimates);
    }
}

void EstimatesBuilder::chooseKinds(SerializationInfoByName & infos, const Estimates & estimates)
{
    for (const auto & [name, info] : infos)
        chooseKindsImpl(name, *info, estimates);
}

void EstimatesBuilder::chooseKinds(SerializationInfoByName & infos) const
{
    chooseKinds(infos, getEstimates());
}

void EstimatesBuilder::addCounts(Estimate & dst, const Estimate & src)
{
    dst.rows_count += src.rows_count;
    addDefaultCount(dst, src.num_defaults.value_or(0));
}

void EstimatesBuilder::subtractCounts(Estimate & dst, const Estimate & src)
{
    /// Saturating: a contribution may be subtracted without having been added first, e.g. when the
    /// per-table serialization hints were rebuilt from scratch between adding and removing a part.
    dst.rows_count -= std::min(dst.rows_count, src.rows_count);
    dst.num_defaults = dst.num_defaults.value_or(0) - std::min(dst.num_defaults.value_or(0), src.num_defaults.value_or(0));
}

void EstimatesBuilder::addEstimates(Estimates & dst, const Estimates & src)
{
    for (const auto & [key, src_estimate] : src)
        addCounts(dst[key], src_estimate);
}

namespace
{

void collectInfoKeys(const String & key, const SerializationInfo & info, NameSet & keys)
{
    keys.insert(key);

    if (const auto * info_tuple = typeid_cast<const SerializationInfoTuple *>(&info))
    {
        const auto & names = info_tuple->getElementNames();
        for (size_t i = 0; i < names.size(); ++i)
            collectInfoKeys(subcolumnEstimateKey(key, names[i]), *info_tuple->getElementInfo(i), keys);
    }
}

}

void EstimatesBuilder::filterEstimates(Estimates & estimates, const SerializationInfoByName & infos)
{
    NameSet keys;
    for (const auto & [name, info] : infos)
        collectInfoKeys(name, *info, keys);

    std::erase_if(estimates, [&](const auto & entry) { return !keys.contains(entry.first); });
}

}
