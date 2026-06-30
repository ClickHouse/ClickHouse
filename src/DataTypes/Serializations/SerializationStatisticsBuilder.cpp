#include <DataTypes/Serializations/SerializationStatisticsBuilder.h>

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

namespace DB
{

SerializationStatisticsBuilder::SerializationStatisticsBuilder(
    const NamesAndTypesList & columns, const SerializationInfoSettings & settings_)
    : settings(settings_)
{
    if (settings.isAlwaysDefault())
        return;

    for (const auto & column : columns)
        if (settings.canUseSparseSerialization(*column.type))
            nodes.emplace(column.name, createNode(*column.type));
}

SerializationStatisticsBuilder::NodePtr SerializationStatisticsBuilder::createNode(const IDataType & type)
{
    auto node = std::make_shared<Node>();

    /// Only `Tuple` has per-element serialization infos (mirrors `DataTypeTuple::createSerializationInfo`);
    /// all other types are leaves whose elements (if any) are serialized as a whole.
    if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(&type))
    {
        const auto & elements = type_tuple->getElements();
        node->element_names = type_tuple->getElementNames();
        node->elements.reserve(elements.size());
        for (const auto & element : elements)
            node->elements.push_back(createNode(*element));
    }

    return node;
}

void SerializationStatisticsBuilder::addColumn(Node & node, const IColumn & column)
{
    node.statistics.add(column);

    if (node.elements.empty())
        return;

    const auto & column_tuple = assert_cast<const ColumnTuple &>(column);
    const auto & elem_columns = column_tuple.getColumns();
    if (node.elements.size() != elem_columns.size())
        return;

    for (size_t i = 0; i < node.elements.size(); ++i)
        addColumn(*node.elements[i], *elem_columns[i]);
}

void SerializationStatisticsBuilder::addDefaultsToNode(Node & node, size_t length)
{
    node.statistics.addDefaults(length);
    for (const auto & element : node.elements)
        addDefaultsToNode(*element, length);
}

void SerializationStatisticsBuilder::addInfo(Node & node, const SerializationInfo & info)
{
    node.statistics.add(info.getStatistics());

    if (node.elements.empty())
        return;

    /// Source structure may differ (e.g. after a type change); only recurse when it is also a tuple.
    const auto * info_tuple = typeid_cast<const SerializationInfoTuple *>(&info);
    if (!info_tuple)
        return;

    for (size_t i = 0; i < node.elements.size(); ++i)
    {
        /// Match elements by name (as the former per-element building did); missing elements
        /// contribute all-default rows.
        if (const auto * elem_info = info_tuple->tryGetElementInfo(node.element_names[i]))
            addInfo(*node.elements[i], **elem_info);
        else
            addDefaultsToNode(*node.elements[i], info.getStatistics().num_rows);
    }
}

void SerializationStatisticsBuilder::add(const Block & block)
{
    for (const auto & [name, node] : nodes)
        if (const auto * column = block.findByName(name))
            addColumn(*node, *column->column);
}

void SerializationStatisticsBuilder::addDefaults(const String & name, size_t length)
{
    if (auto it = nodes.find(name); it != nodes.end())
        addDefaultsToNode(*it->second, length);
}

void SerializationStatisticsBuilder::add(const SerializationInfoByName & infos)
{
    for (const auto & [name, node] : nodes)
        if (auto info = infos.tryGet(name))
            addInfo(*node, *info);
}

void SerializationStatisticsBuilder::chooseKindsImpl(const Node & node, SerializationInfo & info) const
{
    if (info.getSettings().choose_kind)
        info.setKindStack(chooseKindStack(node.statistics, info.getSettings()));

    if (auto * info_tuple = typeid_cast<SerializationInfoTuple *>(&info))
    {
        chassert(node.elements.size() == info_tuple->getNumElements());
        for (size_t i = 0; i < node.elements.size(); ++i)
            chooseKindsImpl(*node.elements[i], *info_tuple->getElementInfo(i));
    }
}

void SerializationStatisticsBuilder::applyStatisticsImpl(const Node & node, SerializationInfo & info) const
{
    info.setStatistics(node.statistics);

    if (auto * info_tuple = typeid_cast<SerializationInfoTuple *>(&info))
    {
        chassert(node.elements.size() == info_tuple->getNumElements());
        for (size_t i = 0; i < node.elements.size(); ++i)
            applyStatisticsImpl(*node.elements[i], *info_tuple->getElementInfo(i));
    }
}

void SerializationStatisticsBuilder::chooseKinds(SerializationInfoByName & infos) const
{
    for (const auto & [name, node] : nodes)
        if (auto info = infos.tryGet(name))
            chooseKindsImpl(*node, *info);
}

void SerializationStatisticsBuilder::applyStatistics(SerializationInfoByName & infos) const
{
    for (const auto & [name, node] : nodes)
        if (auto info = infos.tryGet(name))
            applyStatisticsImpl(*node, *info);
}

ISerialization::KindStack SerializationStatisticsBuilder::chooseKindStack(
    const SerializationStatistics & statistics, const SerializationInfoSettings & settings)
{
    ISerialization::KindStack kind_stack = {ISerialization::Kind::DEFAULT};
    double ratio = statistics.num_rows
        ? std::min(static_cast<double>(statistics.num_defaults) / static_cast<double>(statistics.num_rows), 1.0)
        : 0.0;
    if (ratio > settings.ratio_of_defaults_for_sparse)
        kind_stack.push_back(ISerialization::Kind::SPARSE);
    return kind_stack;
}

void SerializationStatisticsBuilder::addStatistics(SerializationInfo & dst, const SerializationInfo & src)
{
    SerializationStatistics statistics = dst.getStatistics();
    statistics.add(src.getStatistics());
    dst.setStatistics(statistics);

    auto * dst_tuple = typeid_cast<SerializationInfoTuple *>(&dst);
    const auto * src_tuple = typeid_cast<const SerializationInfoTuple *>(&src);
    if (dst_tuple && src_tuple && dst_tuple->getNumElements() == src_tuple->getNumElements())
        for (size_t i = 0; i < dst_tuple->getNumElements(); ++i)
            addStatistics(*dst_tuple->getElementInfo(i), *src_tuple->getElementInfo(i));
}

void SerializationStatisticsBuilder::chooseKindsFromStatistics(SerializationInfo & info)
{
    if (info.getSettings().choose_kind)
        info.setKindStack(chooseKindStack(info.getStatistics(), info.getSettings()));

    if (auto * info_tuple = typeid_cast<SerializationInfoTuple *>(&info))
        for (size_t i = 0; i < info_tuple->getNumElements(); ++i)
            chooseKindsFromStatistics(*info_tuple->getElementInfo(i));
}

}
