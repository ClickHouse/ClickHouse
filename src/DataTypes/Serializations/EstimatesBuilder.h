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
    /// An empty builder: tracks no columns, all accumulation methods are no-ops.
    EstimatesBuilder() = default;
    EstimatesBuilder(const NamesAndTypesList & columns, const SerializationInfoSettings & settings, const Estimates & external_estimates);

    void add(const Block & block);
    void addNumRows(const String & column_name, const DataTypePtr & type, size_t num_rows);
    void addNumDefaults(const String & column_name, const DataTypePtr & type, size_t num_defaults);
    void addNumDefaults(const String & column_name, const DataTypePtr & type, const Estimates & source_estimates);

    /// Add externally accumulated counts, e.g. the counts carried over from the source part for the
    /// columns whose data files a mutation hardlinks instead of rewriting. Keys the builder does not
    /// track are inserted.
    void addEstimates(const Estimates & external_counts);
    bool hasColumn(const String & column_name) const { return estimates.contains(column_name); }

    Estimates getEstimates() const;

    /// The accumulated estimates with the exact default counts from the explicit statistics
    /// (`external_estimates`) taking precedence over the sampled ones.
    Estimates getEstimates(const Estimates & external_estimates) const;
    void chooseKinds(SerializationInfoByName & infos) const;

    static void chooseKinds(SerializationInfoByName & infos, const Estimates & estimates);

    static ISerialization::KindStack chooseKindStack(const Estimate & estimate, const SerializationInfoSettings & settings);
    static void addCounts(Estimate & dst, const Estimate & src);
    static void subtractCounts(Estimate & dst, const Estimate & src);
    static void filterEstimates(Estimates & estimates, const SerializationInfoByName & infos);

private:
    static void chooseKindsImpl(const String & key, SerializationInfo & info, const Estimates & estimates);

    std::map<String, Estimate> estimates;
    NameSet columns_with_exact_counts;
    SerializationInfoSettings settings;
};

}
