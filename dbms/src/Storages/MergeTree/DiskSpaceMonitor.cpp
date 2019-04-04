#include <Storages/MergeTree/DiskSpaceMonitor.h>

namespace DB
{

std::map<String, DiskSpaceMonitor::DiskReserve> DiskSpaceMonitor::reserved;
std::mutex DiskSpaceMonitor::mutex;

Disk::Disk(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
    : path(config.getString(config_prefix + ".path")),
      keep_free_space_bytes(config.getUInt64(config_prefix + ".keep_free_space_bytes", 0))
{
}

DisksSelector::DisksSelector(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix) {
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    for (const auto & name : keys)
    {
        disks.emplace(name, Disk{config, config_prefix + "." + name});
    }
}

const Disk & DisksSelector::operator[](const String & name) const {
    auto it = disks.find(name);
    if (it == disks.end()) {
        throw Exception("Unknown disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
    }
    return it->second;
}

Schema::Volume::Volume(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const DisksSelector & disk_selector) {
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    /// Disk's names
    Strings disks_names;

    for (const auto & name : keys)
    {
        if (startsWith(name.data(), "disk")) {
            disks_names.push_back(config.getString(config_prefix + "." + name));
        } else if (name == "part_size_threshold_bytes") {
            max_data_part_size = config.getUInt64(config_prefix + "." + name, 0);
        }
        ///@TODO_IGR part_size_threshold_ratio which set max_data_part_size by total disk size
    }

    if (max_data_part_size == 0) {
        --max_data_part_size;
    }

    /// Get paths from disk's names
    for (const auto & disk_name : disks_names) {
        /// Disks operator [] may throw exception
        disks.push_back(disk_selector[disk_name]);
    }
}

bool Schema::Volume::setDefaultPath(const String & path) {
    bool set = false;
    for (auto & disk : disks) {
        if (disk.path == "default") {
            if (set) {
                throw Exception("It is not possible to have two default disks", ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG); ///@TODO_IGR ASK ErrorCode
            }
            set = true;
            disk.path = path;

        }
    }
    return set;
}

DiskSpaceMonitor::ReservationPtr Schema::Volume::reserve(UInt64 expected_size) const {
    /// This volume can not store files which size greater than max_data_part_size
    if (expected_size > max_data_part_size) {
        return {};
    }
    /// Real order is not necessary
    size_t start_from = last_used.fetch_add(1u, std::memory_order_relaxed);
    for (size_t i = 0; i != disks.size(); ++i) {
        size_t index = (start_from + i) % disks.size();
        auto reservation = DiskSpaceMonitor::tryToReserve(disks[index], expected_size);
        if (reservation) {
            return reservation;
        }
    }
    return {};
}

UInt64 Schema::Volume::getMaxUnreservedFreeSpace() const {
    UInt64 res = 0;
    for (const auto & disk : disks) {
        ///@TODO_IGR ASK There is cycle with mutex locking inside(((
        res = std::max(res, DiskSpaceMonitor::getUnreservedFreeSpace(disk));
    }
    return res;
}

Schema::Schema(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const DisksSelector & disks) {
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    for (const auto & name : keys)
    {
        if (!startsWith(name.data(), "volume"))
            throw Exception("Unknown element in config: " + config_prefix + "." + name + ", must be 'volume'",\
                            ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
        volumes.emplace_back(config, config_prefix + "." + name, disks);
    }
}

///@TODO_IRG ASK Single use in MergeTreeData constuctor
void Schema::setDefaultPath(const String & path) {
    bool set = false;
    for (auto & volume : volumes) {
        if (volume.setDefaultPath(path)) {
            if (set) {
                throw Exception("It is not possible to have two default disks",
                                ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG); ///@TODO_IGR ASK ErrorCode
            }
            set = true;
        }
    }
}

///@TODO_IGR ASK maybe iterator without copy?
Strings Schema::getFullPaths() const {
    Strings res;
    for (const auto & volume : volumes) {
        for (const auto & disk : volume.disks) {
            res.push_back(disk.path);
        }
    }
    return res;
}

UInt64 Schema::getMaxUnreservedFreeSpace() const {
    UInt64 res = 0;
    for (const auto & volume : volumes) {
        res = std::max(res, volume.getMaxUnreservedFreeSpace());
    }
    return res;
}

DiskSpaceMonitor::ReservationPtr Schema::reserve(UInt64 expected_size) const {
    for (auto & volume : volumes) {
        auto reservation = volume.reserve(expected_size);
        if (reservation) {
            return reservation;
        }
    }
    return {};
}

SchemaSelector::SchemaSelector(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const DisksSelector & disks) {
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    for (const auto & name : keys)
    {
        ///@TODO_IGR ASK What if same names?
        std::cerr << "Schema " + name << std::endl;
        schemes.emplace(name, Schema{config, config_prefix + "." + name, disks});
    }

    std::cerr << config_prefix << " " << schemes.size() << std::endl;
}

const Schema & SchemaSelector::operator[](const String & name) const {
    auto it = schemes.find(name);
    if (it == schemes.end()) {
        throw Exception("Unknown schema " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG); ///@TODO_IGR Choose error code
    }
    return it->second;
}

MergeTreeStorageConfiguration::MergeTreeStorageConfiguration(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
        : disks(config, config_prefix + ".disks"), schema_selector(config, config_prefix + ".schemes", disks)
{
    std::cerr << config_prefix << " " << disks.size() << std::endl;
}

}
