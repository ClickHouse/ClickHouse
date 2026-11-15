#pragma once

#include <Interpreters/StorageID.h>
#include <Parsers/ASTViewTargets.h>


namespace DB
{
class ASTColumnDeclaration;
class ASTCreateQuery;
struct ColumnDescription;
struct TimeSeriesSettings;

/// Normalizes a TimeSeries table definition.
class TimeSeriesDefinitionNormalizer
{
public:
    /// Constructor stores a reference to argument `time_series_settings_` (it's unnecessary to copy it).
    TimeSeriesDefinitionNormalizer(StorageID time_series_storage_id_,
                                   std::reference_wrapper<const TimeSeriesSettings> time_series_settings_,
                                   const ASTCreateQuery * as_create_query_);

    /// Adds missing columns to the definition and reorders all the columns in the canonical way.
    /// Also adds engines of inner tables to the definition if they aren't specified yet.
    /// The `as_table_create_query` parameter must be nullptr if it isn't a "CREATE AS <table> query".
    void normalize(ASTCreateQuery & create_query) const;

private:
    /// Reorders existing columns in the canonical way.
    void reorderColumns(ASTCreateQuery & create) const;

    /// Adds missing columns with data types set by default..
    void addMissingColumns(ASTCreateQuery & create) const;

    /// Adds the DEFAULT expression for the 'id' column if it isn't specified yet.
    void addMissingDefaultForIDColumn(ASTCreateQuery & create) const;

    /// Generates a formulae for calculating the identifier of a time series from the metric name and all the tags.
    ASTPtr chooseIDAlgorithm(const ASTColumnDeclaration & id_column) const;

    /// Copies the definitions of inner engines from "CREATE AS <table>" if this is that kind of query.
    void addMissingInnerEnginesFromAsTable(ASTCreateQuery & create) const;

    /// Adds engines of inner tables to the definition if they aren't specified yet.
    void addMissingInnerEngines(ASTCreateQuery & create) const;

    /// Sets the engine of an inner table by default.
    void setInnerEngineByDefault(ViewTarget::Kind inner_table_kind, ASTStorage & inner_storage_def) const;

    const StorageID time_series_storage_id;
    const TimeSeriesSettings & time_series_settings;
    const ASTCreateQuery * as_create_query = nullptr;
};

}
