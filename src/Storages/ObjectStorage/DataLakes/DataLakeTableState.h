#pragma once

#include "config.h"

#include <memory>

namespace DB
{

/// Opaque, forward-declarable holder for a `DataLakeTableStateSnapshot` (which is itself
/// a `std::variant<...>` over Iceberg / DeltaLake / Paimon concrete state structs).
///
/// The point of this wrapper is to keep `StorageInMemoryMetadata` and other core types
/// free of any include from `DataLakes/`. Core code stores a
/// `std::unique_ptr<DataLakeTableState>` whose concrete type lives only in
/// `DataLakeTableState.cpp`, where the variant alternatives are complete.
///
/// Inside the `DataLakes/` directory you can keep using the raw
/// `DataLakeTableStateSnapshot` variant alias \u2014 this wrapper does not replace it, it
/// only hides it from out-of-module headers. Use `DataLakeTableState::fromSnapshot` to
/// wrap a variant for storage in core types, and `DataLakeTableState::snapshot` to read
/// the variant back when you are inside DataLakes/.
class DataLakeTableState final
{
public:
    struct Impl;

    /// Construct from a fully-typed `DataLakeTableStateSnapshot` (the variant). Defined
    /// in the `.cpp` so callers do not need the variant alternatives to be complete.
    /// Use this overload from inside `DataLakes/`. From core code, prefer
    /// `clone` of an existing `DataLakeTableState`.
    template <typename Snapshot>
    static std::unique_ptr<DataLakeTableState> fromSnapshot(Snapshot && snapshot);

    DataLakeTableState(const DataLakeTableState & other);
    DataLakeTableState(DataLakeTableState && other) noexcept;
    DataLakeTableState & operator=(const DataLakeTableState & other);
    DataLakeTableState & operator=(DataLakeTableState && other) noexcept;
    ~DataLakeTableState();

    bool operator==(const DataLakeTableState & other) const;

    /// Access the underlying variant. Only safe to call from a TU that has the variant
    /// alternatives visible (i.e. anything that includes
    /// `DataLakes/DataLakeTableStateSnapshot.h`).
    template <typename Snapshot>
    const Snapshot * tryGet() const;

    template <typename Snapshot>
    bool holds() const;

    std::unique_ptr<DataLakeTableState> clone() const;

private:
    DataLakeTableState();

    std::unique_ptr<Impl> impl;
};

}
