#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/SerializationInfo.h>

namespace DB
{

class ColumnsStatistics;

/// Automatic `LowCardinality` serialization: a `String`/`FixedString` column that has a cardinality
/// statistic (`uniq` or `uniq_v2`) with an estimate not exceeding @max_uniq_number_for_low_cardinality
/// is stored in dictionary-encoded form, while keeping its declared data type.
/// Returns the subset of @columns that qualifies. The feature is disabled when the threshold is zero.
NameSet chooseColumnsForAutomaticLowCardinality(
    const NamesAndTypesList & columns,
    const ColumnsStatistics & statistics,
    UInt64 max_uniq_number_for_low_cardinality);

/// Appends the `LowCardinality` kind to the serialization infos of @column_names, creating the missing
/// entries. `SerializationInfoByName` creates entries only for columns eligible for sparse serialization,
/// and none at all when sparse serialization is disabled, while automatic `LowCardinality` does not depend
/// on sparse serialization, so the two settings are kept independent.
/// A column already chosen for sparse serialization is left alone: sparse is more efficient for it.
void appendAutomaticLowCardinalityKind(
    SerializationInfoByName & infos,
    const NamesAndTypesList & columns,
    const NameSet & column_names,
    const SerializationInfo::Settings & settings);

/// Removes the automatic `LowCardinality` kind from the serialization infos of @columns.
/// A merge or a rewrite calls it when `max_uniq_number_for_low_cardinality` is zero, so that setting the
/// threshold back to zero and running `OPTIMIZE FINAL` or `ALTER TABLE ... REWRITE PARTS` rolls the
/// encoding back to the plain representation instead of carrying it forward from the source parts.
/// Only columns whose data is rewritten may be passed: a hardlinked column keeps its data files, hence
/// also its serialization.
void removeAutomaticLowCardinalityKind(
    SerializationInfoByName & infos,
    const NamesAndTypesList & columns);

}
