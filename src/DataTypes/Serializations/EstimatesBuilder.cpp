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
#include <Common/Exception.h>

#include <algorithm>

namespace DB
{

using SubcolumnCallback = std::function<void(const String &, const ColumnPtr &)>;

static void forEachSubcolumnWithEstimates(const String & column_name, const SubcolumnCallback & callback, const IDataType & type, const ColumnPtr & column)
{
    callback(column_name, column);

    if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(&type))
    {
        const auto & elem_types = type_tuple->getElements();
        const auto & names = type_tuple->getElementNames();

        for (size_t i = 0; i < names.size(); ++i)
        {
            ColumnPtr elem_column = column ? assert_cast<const ColumnTuple &>(*column).getColumnPtr(i) : nullptr;
            forEachSubcolumnWithEstimates(subcolumnEstimateKey(column_name, names[i]), callback, *elem_types[i], elem_column);
        }
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
            auto callback = [&](const auto & full_name, const auto &)
            {
                estimates.emplace(full_name, Estimate{});
            };

            forEachSubcolumnWithEstimates(column.name, callback, *column.type, nullptr);
        }
    }
}

void EstimatesBuilder::add(const Block & block)
{
    for (const auto & column : block)
    {
        auto callback = [&](const auto & full_name, const ColumnPtr & subcolumn)
        {
            if (columns_with_exact_counts.contains(full_name))
                return;

            auto it = estimates.find(full_name);
            if (it == estimates.end())
                return;

            size_t rows = subcolumn->size();
            double ratio = subcolumn->getRatioOfDefaultRows(ColumnSparse::DEFAULT_ROWS_SEARCH_SAMPLE_RATIO);

            it->second.rows_count += rows;
            it->second.num_defaults = it->second.num_defaults.value_or(0) + static_cast<UInt64>(ratio * static_cast<double>(rows));
        };

        forEachSubcolumnWithEstimates(column.name, callback, *column.type, column.column);
    }
}

void EstimatesBuilder::addNumRows(const String & column_name, const DataTypePtr & type, size_t num_rows)
{
    auto callback = [&](const auto & full_name, const auto &)
    {
        if (auto it = estimates.find(full_name); it != estimates.end())
            it->second.rows_count += num_rows;
    };

    forEachSubcolumnWithEstimates(column_name, callback, *type, nullptr);
}

void EstimatesBuilder::addNumDefaults(const String & column_name, const DataTypePtr & type, size_t num_defaults)
{
    auto callback = [&](const auto & full_name, const auto &)
    {
        if (auto it = estimates.find(full_name); it != estimates.end())
            it->second.num_defaults = it->second.num_defaults.value_or(0) + num_defaults;
    };

    forEachSubcolumnWithEstimates(column_name, callback, *type, nullptr);
}

void EstimatesBuilder::addNumDefaults(const String & column_name, const DataTypePtr & type, const Estimates & source_estimates)
{
    auto callback = [&](const auto & full_name, const auto &)
    {
        auto it_dst = estimates.find(full_name);
        auto it_src = source_estimates.find(full_name);

        if (it_dst != estimates.end() && it_src != source_estimates.end())
            it_dst->second.num_defaults = it_dst->second.num_defaults.value_or(0) + it_src->second.num_defaults.value_or(0);
    };

    forEachSubcolumnWithEstimates(column_name, callback, *type, nullptr);
}

void EstimatesBuilder::addEstimates(const Estimates & external_counts)
{
    for (const auto & [key, external] : external_counts)
        addCounts(estimates[key], external);
}

Estimates EstimatesBuilder::getEstimates() const
{
    return {estimates.begin(), estimates.end()};
}

Estimates EstimatesBuilder::getEstimates(const Estimates & external_estimates) const
{
    auto result = getEstimates();

    /// Explicit statistics exist only for top-level columns; override the sampled default count with the
    /// exact one from the statistics where it is available. The sampled row count is kept (it is exact).
    for (const auto & [name, external] : external_estimates)
    {
        if (!external.num_defaults.has_value())
            continue;
        if (auto it = result.find(name); it != result.end())
            it->second.num_defaults = external.num_defaults;
    }

    return result;
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
    dst.num_defaults = dst.num_defaults.value_or(0) + src.num_defaults.value_or(0);
}

void EstimatesBuilder::subtractCounts(Estimate & dst, const Estimate & src)
{
    /// Saturating: a contribution may be subtracted without having been added first, e.g. when the
    /// per-table serialization hints were rebuilt from scratch between adding and removing a part.
    dst.rows_count -= std::min(dst.rows_count, src.rows_count);
    dst.num_defaults = dst.num_defaults.value_or(0) - std::min(dst.num_defaults.value_or(0), src.num_defaults.value_or(0));
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
