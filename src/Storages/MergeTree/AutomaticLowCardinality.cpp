#include <Storages/MergeTree/AutomaticLowCardinality.h>

#include <DataTypes/IDataType.h>
#include <Storages/Statistics/Statistics.h>

namespace DB
{

NameSet chooseColumnsForAutomaticLowCardinality(
    const NamesAndTypesList & columns,
    const ColumnsStatistics & statistics,
    UInt64 max_uniq_number_for_low_cardinality)
{
    NameSet result;
    if (max_uniq_number_for_low_cardinality == 0)
        return result;

    for (const auto & [column_name, column_type] : columns)
    {
        if (!isStringOrFixedString(column_type))
            continue;

        auto stats_it = statistics.find(column_name);
        if (stats_it == statistics.end() || !stats_it->second->hasCardinality())
            continue;

        if (stats_it->second->estimateCardinality() > max_uniq_number_for_low_cardinality)
            continue;

        result.insert(column_name);
    }

    return result;
}

void appendAutomaticLowCardinalityKind(
    SerializationInfoByName & infos,
    const NamesAndTypesList & columns,
    const NameSet & column_names,
    const SerializationInfo::Settings & settings)
{
    for (const auto & [column_name, column_type] : columns)
    {
        if (!column_names.contains(column_name))
            continue;

        if (!infos.contains(column_name))
            infos.emplace(column_name, column_type->createSerializationInfo(settings));

        auto & info = infos.at(column_name);
        const auto & kind_stack = info->getKindStack();

        if (!ISerialization::hasKind(kind_stack, ISerialization::Kind::SPARSE)
            && !ISerialization::hasKind(kind_stack, ISerialization::Kind::LOW_CARDINALITY))
            info->appendToKindStack(ISerialization::Kind::LOW_CARDINALITY);
    }
}


void removeAutomaticLowCardinalityKind(
    SerializationInfoByName & infos,
    const NamesAndTypesList & columns)
{
    for (const auto & column : columns)
    {
        auto it = infos.find(column.name);
        if (it == infos.end())
            continue;

        const auto & kind_stack = it->second->getKindStack();
        if (!ISerialization::hasKind(kind_stack, ISerialization::Kind::LOW_CARDINALITY))
            continue;

        ISerialization::KindStack new_kind_stack;
        for (auto kind : kind_stack)
        {
            if (kind != ISerialization::Kind::LOW_CARDINALITY)
                new_kind_stack.push_back(kind);
        }

        it->second->setKindStack(std::move(new_kind_stack));
    }
}

}
