#include <DataTypes/Serializations/EstimatesBuilder.h>

#include <Columns/ColumnSparse.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/NestedUtils.h>
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

}

EstimatesBuilder::EstimatesBuilder(const NamesAndTypesList & columns, const SerializationInfoSettings & settings_)
    : settings(settings_)
{
    if (settings.isAlwaysDefault())
        return;

    for (const auto & column : columns)
    {
        if (settings.canUseSparseSerialization(*column.type))
        {
            roots.push_back(column.name);
            addNodes(column.name, column.type);
        }
    }
}

void EstimatesBuilder::addNodes(const String & key, const DataTypePtr & type)
{
    auto node = std::make_shared<Node>();
    node->type = type;
    nodes.emplace(key, std::move(node));

    /// Only `Tuple` has per-element serialization infos (mirrors `DataTypeTuple::createSerializationInfo`);
    /// all other types are leaves whose elements (if any) are serialized as a whole. Each element becomes
    /// a separate entry in `nodes`, keyed by its subcolumn path.
    if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto & elements = type_tuple->getElements();
        const auto & names = type_tuple->getElementNames();
        for (size_t i = 0; i < elements.size(); ++i)
            addNodes(Nested::concatenateName(key, names[i]), elements[i]);
    }
}

void EstimatesBuilder::sampleColumn(const String & key, const IColumn & column)
{
    auto & node = getNode(key);
    size_t rows = column.size();
    double ratio = column.getRatioOfDefaultRows(ColumnSparse::DEFAULT_ROWS_SEARCH_SAMPLE_RATIO);
    node.estimate.rows_count += rows;
    addDefaultCount(node.estimate, static_cast<UInt64>(ratio * static_cast<double>(rows)));

    const auto * type_tuple = typeid_cast<const DataTypeTuple *>(node.type.get());
    if (!type_tuple)
        return;

    const auto & column_tuple = assert_cast<const ColumnTuple &>(column);
    const auto & elem_columns = column_tuple.getColumns();
    const auto & names = type_tuple->getElementNames();
    if (names.size() != elem_columns.size())
        return;

    for (size_t i = 0; i < names.size(); ++i)
        sampleColumn(Nested::concatenateName(key, names[i]), *elem_columns[i]);
}

void EstimatesBuilder::addDefaultsToNode(const String & key, size_t length)
{
    auto & node = getNode(key);
    node.estimate.rows_count += length;
    addDefaultCount(node.estimate, length);

    if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(node.type.get()))
        for (const auto & name : type_tuple->getElementNames())
            addDefaultsToNode(Nested::concatenateName(key, name), length);
}

void EstimatesBuilder::addPartEstimate(const String & key, const Estimates & part_estimates)
{
    auto & node = getNode(key);
    const auto & part_estimate = part_estimates.at(key);
    node.estimate.rows_count += part_estimate.rows_count;
    addDefaultCount(node.estimate, part_estimate.num_defaults.value_or(0));

    const auto * type_tuple = typeid_cast<const DataTypeTuple *>(node.type.get());
    if (!type_tuple)
        return;

    for (const auto & name : type_tuple->getElementNames())
    {
        auto child_key = Nested::concatenateName(key, name);
        /// Match elements by path; a tracked element missing from the source contributes all-default rows.
        if (part_estimates.contains(child_key))
            addPartEstimate(child_key, part_estimates);
        else
            addDefaultsToNode(child_key, part_estimate.rows_count);
    }
}

void EstimatesBuilder::add(const Block & block)
{
    for (const auto & name : roots)
        if (const auto * column = block.findByName(name))
            sampleColumn(name, *column->column);
}

void EstimatesBuilder::addDefaults(const String & name, size_t length)
{
    if (nodes.contains(name))
        addDefaultsToNode(name, length);
}

void EstimatesBuilder::add(const Estimates & part_estimates)
{
    for (const auto & name : roots)
        if (part_estimates.contains(name))
            addPartEstimate(name, part_estimates);
}

void EstimatesBuilder::mergeEstimates(const Estimates & external_estimates)
{
    /// Explicit statistics exist only for top-level columns; override the sampled default count with the
    /// exact one where it is available. The sampled row count is kept (it equals the exact one).
    for (const auto & [name, external] : external_estimates)
    {
        if (!external.num_defaults.has_value())
            continue;
        if (auto it = nodes.find(name); it != nodes.end())
            it->second->estimate.num_defaults = external.num_defaults;
    }
}

Estimates EstimatesBuilder::getEstimates() const
{
    Estimates estimates;
    for (const auto & [key, node] : nodes)
        estimates.emplace(key, node->estimate);
    return estimates;
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

    if (auto * info_tuple = typeid_cast<SerializationInfoTuple *>(&info))
    {
        const auto & names = info_tuple->getElementNames();
        for (size_t i = 0; i < names.size(); ++i)
            chooseKindsImpl(Nested::concatenateName(key, names[i]), *info_tuple->getElementInfo(i), estimates);
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

void EstimatesBuilder::addEstimates(Estimates & dst, const Estimates & src)
{
    for (const auto & [key, src_estimate] : src)
    {
        auto & dst_estimate = dst[key];
        dst_estimate.rows_count += src_estimate.rows_count;
        addDefaultCount(dst_estimate, src_estimate.num_defaults.value_or(0));
    }
}

}
