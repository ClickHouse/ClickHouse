#pragma once

#include <Core/Names.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <Storages/Statistics/Estimate.h>

#include <map>

namespace DB
{

class IColumn;
class IDataType;
class Block;
class NamesAndTypesList;
class SerializationInfo;
class SerializationInfoByName;

class EstimatesBuilder
{
public:
    EstimatesBuilder(const NamesAndTypesList & columns, const SerializationInfoSettings & settings, const Estimates & external_estimates);

    void add(const Block & block);
    void addNumRows(const String & column_name, const DataTypePtr & type, size_t num_rows);
    void addNumDefaults(const String & column_name, const DataTypePtr & type, size_t num_defaults);
    void addNumDefaults(const String & column_name, const DataTypePtr & type, const Estimates & source_estimates);
    bool hasColumn(const String & column_name) const { return estimates.contains(column_name); }

    void mergeEstimates(const Estimates & external_estimates);
    static void mergeEstimates(Estimates & estimates, const Estimates & external_estimates);
    Estimates getEstimates() const;
    void chooseKinds(SerializationInfoByName & infos) const;

    static void chooseKinds(SerializationInfoByName & infos, const Estimates & estimates);

    static ISerialization::KindStack chooseKindStack(const Estimate & estimate, const SerializationInfoSettings & settings);
    static void addCounts(Estimate & dst, const Estimate & src);
    static void subtractCounts(Estimate & dst, const Estimate & src);
    static void addEstimates(Estimates & dst, const Estimates & src);
    static void filterEstimates(Estimates & estimates, const SerializationInfoByName & infos);

private:
    static void chooseKindsImpl(const String & key, SerializationInfo & info, const Estimates & estimates);

    std::map<String, Estimate> estimates;
    NameSet columns_with_exact_counts;
    SerializationInfoSettings settings;
};

}
