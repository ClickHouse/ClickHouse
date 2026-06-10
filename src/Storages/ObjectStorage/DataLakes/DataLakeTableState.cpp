#include <Storages/ObjectStorage/DataLakes/DataLakeTableState.h>

#include <Storages/ObjectStorage/DataLakes/DataLakeTableStateSnapshot.h>

namespace DB
{

struct DataLakeTableState::Impl
{
    DataLakeTableStateSnapshot snapshot;
};

DataLakeTableState::DataLakeTableState() = default;

DataLakeTableState::DataLakeTableState(const DataLakeTableState & other)
    : impl(other.impl ? std::make_unique<Impl>(*other.impl) : nullptr)
{
}

DataLakeTableState::DataLakeTableState(DataLakeTableState && other) noexcept = default;

DataLakeTableState & DataLakeTableState::operator=(const DataLakeTableState & other)
{
    if (this == &other)
        return *this;
    impl = other.impl ? std::make_unique<Impl>(*other.impl) : nullptr;
    return *this;
}

DataLakeTableState & DataLakeTableState::operator=(DataLakeTableState && other) noexcept = default;

DataLakeTableState::~DataLakeTableState() = default;

bool DataLakeTableState::operator==(const DataLakeTableState & other) const
{
    if (!impl && !other.impl)
        return true;
    if (!impl || !other.impl)
        return false;
    return impl->snapshot == other.impl->snapshot;
}

std::unique_ptr<DataLakeTableState> DataLakeTableState::clone() const
{
    auto result = std::unique_ptr<DataLakeTableState>(new DataLakeTableState());
    if (impl)
        result->impl = std::make_unique<Impl>(*impl);
    return result;
}

template <typename Snapshot>
std::unique_ptr<DataLakeTableState> DataLakeTableState::fromSnapshot(Snapshot && snapshot)
{
    auto result = std::unique_ptr<DataLakeTableState>(new DataLakeTableState());
    result->impl = std::make_unique<Impl>(Impl{DataLakeTableStateSnapshot{std::forward<Snapshot>(snapshot)}});
    return result;
}

template <typename Snapshot>
const Snapshot * DataLakeTableState::tryGet() const
{
    if (!impl)
        return nullptr;
    return std::get_if<Snapshot>(&impl->snapshot);
}

template <typename Snapshot>
bool DataLakeTableState::holds() const
{
    if (!impl)
        return false;
    return std::holds_alternative<Snapshot>(impl->snapshot);
}

/// Explicit instantiations for every alternative the variant carries and for the
/// variant alias itself (most callers in `DataLakes/` pass the variant directly).
#if USE_AVRO
template std::unique_ptr<DataLakeTableState> DataLakeTableState::fromSnapshot<Iceberg::TableStateSnapshot>(Iceberg::TableStateSnapshot &&);
template std::unique_ptr<DataLakeTableState> DataLakeTableState::fromSnapshot<const Iceberg::TableStateSnapshot &>(const Iceberg::TableStateSnapshot &);
template std::unique_ptr<DataLakeTableState> DataLakeTableState::fromSnapshot<Iceberg::TableStateSnapshot &>(Iceberg::TableStateSnapshot &);
template const Iceberg::TableStateSnapshot * DataLakeTableState::tryGet<Iceberg::TableStateSnapshot>() const;
template bool DataLakeTableState::holds<Iceberg::TableStateSnapshot>() const;
#endif

#if USE_PARQUET && USE_DELTA_KERNEL_RS
template std::unique_ptr<DataLakeTableState> DataLakeTableState::fromSnapshot<DeltaLake::TableStateSnapshot>(DeltaLake::TableStateSnapshot &&);
template std::unique_ptr<DataLakeTableState> DataLakeTableState::fromSnapshot<const DeltaLake::TableStateSnapshot &>(const DeltaLake::TableStateSnapshot &);
template std::unique_ptr<DataLakeTableState> DataLakeTableState::fromSnapshot<DeltaLake::TableStateSnapshot &>(DeltaLake::TableStateSnapshot &);
template const DeltaLake::TableStateSnapshot * DataLakeTableState::tryGet<DeltaLake::TableStateSnapshot>() const;
template bool DataLakeTableState::holds<DeltaLake::TableStateSnapshot>() const;
#endif

#if USE_AVRO
template std::unique_ptr<DataLakeTableState> DataLakeTableState::fromSnapshot<Paimon::TableStateSnapshot>(Paimon::TableStateSnapshot &&);
template std::unique_ptr<DataLakeTableState> DataLakeTableState::fromSnapshot<const Paimon::TableStateSnapshot &>(const Paimon::TableStateSnapshot &);
template std::unique_ptr<DataLakeTableState> DataLakeTableState::fromSnapshot<Paimon::TableStateSnapshot &>(Paimon::TableStateSnapshot &);
template const Paimon::TableStateSnapshot * DataLakeTableState::tryGet<Paimon::TableStateSnapshot>() const;
template bool DataLakeTableState::holds<Paimon::TableStateSnapshot>() const;
#endif

/// The variant alias itself \u2014 most existing callers (`StorageObjectStorage`,
/// `StorageObjectStorageCluster`, the per-format metadata implementations) pass it.
template std::unique_ptr<DataLakeTableState> DataLakeTableState::fromSnapshot<DataLakeTableStateSnapshot>(DataLakeTableStateSnapshot &&);
template std::unique_ptr<DataLakeTableState> DataLakeTableState::fromSnapshot<const DataLakeTableStateSnapshot &>(const DataLakeTableStateSnapshot &);
template std::unique_ptr<DataLakeTableState> DataLakeTableState::fromSnapshot<DataLakeTableStateSnapshot &>(DataLakeTableStateSnapshot &);

}
